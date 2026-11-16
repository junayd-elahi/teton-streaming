#!/usr/bin/env python3
"""External event-truth oracle. Requires an isolated PostgreSQL admin URL.

Run against a local PostgreSQL instance with database-creation permission.
Creates/drops only oracle_<uuid>; never changes the supplied database's tables.
No production clock injection. Every time-sensitive read retains its wall-clock
request interval and computes the complete admissible oracle range for that interval.
"""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import datetime as dt
import hashlib
import json
import math
import os
from pathlib import Path
import random
import signal
import socket
import subprocess
import sys
import time
import tempfile
import urllib.error
import urllib.parse
import urllib.request
import uuid

import psycopg2
from psycopg2 import sql

ROOT = Path(__file__).resolve().parent
DEFAULT_REPO = next((p for p in ROOT.parents if (p / 'go.mod').is_file()), ROOT.parent.parent / 'repo')
UTC = dt.timezone.utc


def timestamp(t):
    return dt.datetime.fromtimestamp(t, UTC).isoformat(timespec='milliseconds').replace('+00:00', 'Z')


def epoch(t):
    return dt.datetime.fromisoformat(t.replace('Z', '+00:00')).timestamp()


def occupancy_truth(events, room, width, now):
    """Integrate a step function by intersecting each state interval with the window.

    Equal-time policy assumption: highest (device_id, seq) wins. This is tested
    separately because the official contract does not resolve equal-time ties.
    Future events determine latest state but have zero retrospective duration.
    Before the first event is unknown and contributes zero occupied duration.
    """
    changes = sorted((epoch(e['ts']), e['device_id'], e['seq'], e['in_room'])
                     for e in events if e['type'] == 'presence' and e['room_id'] == room)
    known = occupied = 0.0
    for i, (at, _, _, state) in enumerate(changes):
        until = changes[i + 1][0] if i + 1 < len(changes) else now
        span = max(0.0, min(until, now) - max(at, now - width))
        known += span
        occupied += span if state else 0.0
    return {'in_room': changes[-1][3] if changes else False,
            'occupied_pct': occupied / width, 'window_seconds': width,
            'known_seconds': known, 'unknown_seconds': width - known}


def occupancy_range(events, room, width, lo, hi):
    # Window integrals are piecewise linear with knots at event times and at
    # event+window. Evaluate all knots inside the request interval, not just ends.
    points = [lo, hi]
    for e in events:
        if e['type'] == 'presence' and e['room_id'] == room:
            at = epoch(e['ts'])
            points.extend(x for x in (at, at + width) if lo <= x <= hi)
    vals = [occupancy_truth(events, room, width, t) for t in points]
    return {k: [min(v[k] for v in vals), max(v[k] for v in vals)]
            for k in vals[0] if k != 'in_room'}, vals[0]['in_room']


def health_slot_range(timestamps, lo, hi):
    # Count independent occupied 1s lag bins. Sweep every possible bin/window
    # boundary within the interval so uncertainty never hides an incorrect count.
    points = [lo, hi]
    for at in timestamps:
        first = math.ceil(lo - at)
        last = math.floor(hi - at)
        for k in range(first, last + 1):
            boundary = at + k
            points.extend(t for t in (boundary - 1e-6, boundary, boundary + 1e-6) if lo <= t <= hi)
    counts = [len({math.floor(t - at) for at in timestamps if 0 <= t - at < 300}) for t in points]
    return [min(counts), max(counts)]


class Probe:
    def __init__(self, out, repo):
        self.out = out
        self.repo = repo
        self.out.mkdir(parents=True, exist_ok=True)
        self.events = []
        self.requests = []
        self.checks = []
        self.phase = 'setup'
        self.server = None
        self.admin = None
        self.database = 'oracle_' + uuid.uuid4().hex
        self.health_policy = {}

    def record(self, name, ok, **details):
        check = {'phase': self.phase, 'name': name, 'pass': bool(ok), **details}
        self.checks.append(check)
        if not ok:
            print('FAIL ' + json.dumps(check), flush=True)

    def call(self, path, event=None):
        data = json.dumps(event).encode() if event is not None else None
        req = urllib.request.Request(self.base + path, data=data,
                                     headers={'Content-Type': 'application/json'} if data else {})
        before = time.time()
        try:
            with urllib.request.urlopen(req, timeout=15) as response:
                status, body = response.status, response.read()
        except urllib.error.HTTPError as exc:
            status, body = exc.code, exc.read()
        after = time.time()
        result = json.loads(body)
        self.requests.append({'phase': self.phase, 'path': path, 'event': event,
                              'before': before, 'after': after, 'status': status, 'body': result})
        return status, result, before, after

    def post(self, event, exact_retry=False):
        status, body, before, after = self.call('/events', event)
        self.record('durable_ingest', status == 200 and body.get('ok') is True,
                    event=event, status=status, response=body)
        if status != 200:
            raise RuntimeError('Cannot continue a truth oracle with rejected valid input')
        if exact_retry:
            self.record('exact_retry_ack', body.get('duplicate') is True, event=event, response=body)
        else:
            self.events.append(event)
        return before, after

    def event(self, device, room, kind, at, seq, **payload):
        return dict(device_id=device, room_id=room, type=kind, ts=timestamp(at), seq=seq, **payload)

    def start(self):
        self.server_log = open(self.out / 'server.log', 'ab')
        self.server = subprocess.Popen([str(self.binary)], cwd=self.repo, env=self.env,
                                       stdout=self.server_log, stderr=subprocess.STDOUT)
        for _ in range(200):
            if self.server.poll() is not None:
                raise RuntimeError('API exited; see server.log')
            try:
                with urllib.request.urlopen(self.base + '/readyz', timeout=.2) as response:
                    if response.status == 200:
                        return
            except (OSError, urllib.error.URLError):
                pass
            time.sleep(.025)
        raise RuntimeError('API readiness timed out')

    def setup(self):
        self.provenance = {
            'git_head': subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=self.repo, text=True).strip(),
            'git_status': subprocess.check_output(['git', 'status', '--short'], cwd=self.repo, text=True),
            'source_sha256': {str(p.relative_to(self.repo)): hashlib.sha256(p.read_bytes()).hexdigest()
                              for p in sorted(list(self.repo.rglob('*.go')) + list(self.repo.rglob('*.sql')))},
        }
        supplied = urllib.parse.urlsplit(os.environ['DATABASE_URL'])
        admin_url = urllib.parse.urlunsplit(supplied._replace(path='/postgres', query=''))
        self.admin = psycopg2.connect(admin_url)
        self.admin.autocommit = True
        with self.admin.cursor() as cursor:
            cursor.execute(sql.SQL('CREATE DATABASE {}').format(sql.Identifier(self.database)))
        db_url = urllib.parse.urlunsplit(supplied._replace(path='/' + self.database, query='pool_max_conns=12'))
        self.db_url = urllib.parse.urlunsplit(supplied._replace(path='/' + self.database, query=''))
        with socket.socket() as sock:
            sock.bind(('127.0.0.1', 0))
            port = sock.getsockname()[1]
        self.base = f'http://127.0.0.1:{port}'
        self.env = dict(os.environ, DATABASE_URL=db_url, LISTEN_ADDR=f'127.0.0.1:{port}',
                        INGEST_WRITERS='4', INGEST_QUEUE_SIZE='5000', INGEST_PRIORITY_QUEUE_SIZE='1000',
                        INGEST_BATCH_SIZE='500', INGEST_LINGER_MS='1', INGEST_ENQUEUE_TIMEOUT_MS='2000',
                        INGEST_COMMIT_TIMEOUT_MS='5000')
        self.binary = self.out / 'server'
        subprocess.run(['go', 'build', '-buildvcs=false', '-o', str(self.binary), './cmd/server'],
                       cwd=self.repo, env=self.env, check=True)
        self.provenance['binary_sha256'] = hashlib.sha256(self.binary.read_bytes()).hexdigest()
        self.start()

    def check_occupancy(self, rooms):
        for room in rooms:
            for window, width in [('1m', 60), ('5m', 300), ('1h', 3600)]:
                status, body, before, after = self.call(f'/rooms/{room}/occupancy?window={window}')
                ranges, state = occupancy_range(self.events, room, width, before, after)
                # 2 microseconds accommodates timestamp float representation only;
                # request latency uncertainty is represented by oracle intervals.
                valid = status == 200 and body.get('in_room') is state
                for key, (lo, hi) in ranges.items():
                    tolerance = 2e-6 / width if key == 'occupied_pct' else 2e-6
                    value = body.get(key)
                    valid = valid and isinstance(value, (int, float)) and lo - tolerance <= value <= hi + tolerance
                self.record('occupancy_step_function', valid, room=room, window=window,
                            query_interval=[before, after], expected_ranges=ranges,
                            expected_in_room=state, actual=body)

    def check_health(self, devices):
        for device in devices:
            status, body, before, after = self.call(f'/devices/{device}/health')
            times = [epoch(e['ts']) for e in self.events if e['type'] == 'heartbeat' and e['device_id'] == device]
            last = timestamp(max(times)) if times else None
            policy = self.health_policy.get(device, {'offset_range': [0, 0]})
            offset_lo, offset_hi = policy['offset_range']
            slots_lo, slots_hi = health_slot_range(times, before + offset_lo, after + offset_hi)
            actual_slots = body.get('availability_5m', float('nan')) * 300
            valid = (status == 200 and body.get('last_heartbeat_ts') == last
                     and slots_lo - 1e-7 <= actual_slots <= slots_hi + 1e-7
                     and abs(actual_slots - round(actual_slots)) < 1e-7
                     and offset_lo - 2e-6 <= body.get('clock_offset_seconds', float('inf')) <= offset_hi + 2e-6)
            self.record('health_distinct_second_slots', valid, device=device,
                        query_interval=[before, after], expected_last=last,
                        expected_slot_range=[slots_lo, slots_hi], expected_offset_range=[offset_lo, offset_hi], actual=body)

    def expected_falls(self):
        # Official generator jitter changes sequence only, retaining physical
        # fall timestamp/confidence. No speculative temporal dedup is assumed.
        distinct = {}
        for e in self.events:
            if e['type'] == 'fall_warn':
                key = (e['device_id'], e['room_id'], e['ts'], e['confidence'])
                distinct.setdefault(key, e)
        return distinct

    def check_alarms(self):
        status, body, _, _ = self.call('/alarms?since=0')
        alarms = body.get('alarms', [])
        keys = [(a['device_id'], a['room_id'], a['ts'], a['confidence']) for a in alarms]
        expected = self.expected_falls()
        ids = [a.get('event_id') for a in alarms]
        valid = (status == 200 and len(keys) == len(expected) and set(keys) == set(expected)
                 and len(set(ids)) == len(ids) and all(ids)
                 and [a['ts'] for a in alarms] == sorted(a['ts'] for a in alarms))
        self.record('alarm_truth_and_jitter_dedup', valid, expected_count=len(expected), actual=body)
        return body

    def scenarios(self):
        base = math.floor(time.time())
        self.phase = 'initial_out_of_order'
        rooms = ['room_full', 'room_replay', 'room_unknown', 'room_none', 'room_tie', 'room_future', 'room_5001']
        # Source truth uses nonuniform transitions, predecessor state before the
        # 1h window, repeated states, multiple devices, and no room initialization.
        full = [(-3700, True), (-3520, False), (-3180, True), (-2100, False),
                (-1400, True), (-620, False), (-295, True), (-212, False),
                (-153, True), (-58, False), (-44, True), (-31, True), (-13, False), (-5, True)]
        batch = [self.event('dev_full', 'room_full', 'presence', base + lag, i + 1, in_room=state)
                 for i, (lag, state) in enumerate(full)]
        batch += [self.event('dev_replay', 'room_replay', 'presence', base - 3500, 1, in_room=False),
                  self.event('dev_replay', 'room_replay', 'presence', base - 5, 6, in_room=False),
                  self.event('dev_unknown', 'room_unknown', 'presence', base - 125, 1, in_room=True),
                  self.event('dev_unknown', 'room_unknown', 'presence', base - 22, 2, in_room=False),
                  self.event('dev_tie', 'room_tie', 'presence', base - 100, 10, in_room=True),
                  self.event('dev_tie', 'room_tie', 'presence', base - 100, 11, in_room=False),
                  self.event('dev_future', 'room_future', 'presence', base - 100, 1, in_room=False),
                  self.event('dev_future', 'room_future', 'presence', base + 30, 2, in_room=True)]
        random.Random(971).shuffle(batch)
        for e in batch:
            self.post(e)
        self.post(batch[0], exact_retry=True)

        # All replay-only samples have >60s transport lag, so a zero clock offset
        # is independently justified and no calibration policy is involved.
        heartbeat_batch = []
        for i, lag in enumerate(range(80, 260)):
            if lag % 7:
                heartbeat_batch.append(self.event('dev_slots', 'room_slots', 'heartbeat', base - lag + .2, i + 1))
        heartbeat_batch += [self.event('dev_slots', 'room_slots', 'heartbeat', base - 180 + .25, 1001),
                            self.event('dev_slots', 'room_slots', 'heartbeat', base - 180 + .3, 1002)]
        # old buffers must not revive availability or displace max heartbeat.
        heartbeat_batch += [self.event('dev_old', 'room_old', 'heartbeat', base - 1200 - i, i + 1) for i in range(20)]
        random.Random(442).shuffle(heartbeat_batch)
        for e in heartbeat_batch:
            self.post(e)
        self.post(heartbeat_batch[0], exact_retry=True)

        # Establish a real 5,000-device population before introducing device
        # 5,001. Unique IDs, no registry/configuration update or redeployment.
        self.phase = 'dynamic_5001st_device'
        population = [self.event(f'population_{i:04d}', f'population_room_{i:04d}', 'heartbeat', base - 1000, 1)
                      for i in range(5000)]
        with ThreadPoolExecutor(max_workers=16) as pool:
            list(pool.map(self.post, population))
        self.post(self.event('dev_5001', 'room_5001', 'presence', base - 50, 1, in_room=True))
        self.post(self.event('dev_5001', 'room_5001', 'heartbeat', base - 80, 2))
        self.check_health(['dev_5001'])
        self.check_occupancy(['room_5001'])
        self.phase = 'initial_out_of_order'

        # Known synthetic ±30s clocks, with actual HTTP receipt interval retained.
        # Historical replay is delivered first; one fresh anchor follows last.
        for skew in [-30, 30]:
            device = 'dev_skew_' + str(skew)
            for i, lag in enumerate(range(75, 250)):
                self.post(self.event(device, 'room_skew', 'heartbeat', base - lag + skew, i + 1))
            at = math.floor(time.time() * 1000) / 1000 + skew
            anchor = self.event(device, 'room_skew', 'heartbeat', at, 1000)
            before, after = self.post(anchor)
            self.health_policy[device] = {'offset_range': [epoch(anchor['ts']) - after, epoch(anchor['ts']) - before]}

        # Accurate-clock late replay must preserve its prior live calibration.
        # The 59s-old packet is a delayed heartbeat, not a physical clock change.
        accurate = 'dev_accurate_replay'
        for i, lag in enumerate([80, 120, 180, 240]):
            self.post(self.event(accurate, 'room_accurate', 'heartbeat', base - lag, i + 1))
        live_at = math.floor(time.time() * 1000) / 1000
        live = self.event(accurate, 'room_accurate', 'heartbeat', live_at, 100)
        before, after = self.post(live)
        self.health_policy[accurate] = {'offset_range': [epoch(live['ts']) - after, epoch(live['ts']) - before]}
        self.check_health([accurate])

        # Probe exact rolling cutoff and duplicate occupancy of a slot. All
        # historical packets are outside the calibration range; the last live
        # packet provides a measured, bounded clock anchor.
        boundary_device = 'dev_health_boundary'
        anchor_at = math.floor(time.time() * 1000) / 1000
        for i, lag in enumerate([300.2, 300, 299.8, 299.2, 180.8, 180.7, 100.2]):
            self.post(self.event(boundary_device, 'room_boundary', 'heartbeat', anchor_at - lag, i + 1))
        boundary_anchor = self.event(boundary_device, 'room_boundary', 'heartbeat', anchor_at, 100)
        before, after = self.post(boundary_anchor)
        self.health_policy[boundary_device] = {'offset_range': [epoch(boundary_anchor['ts']) - after,
                                                              epoch(boundary_anchor['ts']) - before]}
        self.check_health([boundary_device])
        self.post(self.event(accurate, 'room_accurate', 'heartbeat', live_at - 59, 99))
        self.check_health([accurate])

        falls = [self.event('dev_fall_a', 'room_fall_a', 'fall_warn', base - 12, 1, confidence=.91),
                 self.event('dev_fall_a', 'room_fall_a', 'fall_warn', base - 12, 2, confidence=.91),
                 self.event('dev_fall_a', 'room_fall_a', 'fall_warn', base - 12, 3, confidence=.91),
                 self.event('dev_fall_a', 'room_fall_a', 'fall_warn', base - 9, 4, confidence=.91),
                 self.event('dev_fall_b', 'room_fall_b', 'fall_warn', base - 100, 1, confidence=.91)]
        for e in falls:
            self.post(e)
        self.post(falls[0], exact_retry=True)
        devices = ['dev_slots', 'dev_old', 'dev_none', 'dev_skew_-30', 'dev_skew_30', 'dev_5001', accurate, boundary_device]
        self.check_occupancy(rooms)
        self.check_health(devices)
        self.check_alarms()

        self.phase = 'offline_replay_correction'
        for lag, seq, state in [(-1200, 4, True), (-2400, 2, True), (-1800, 3, False), (-300, 5, False)]:
            self.post(self.event('dev_replay', 'room_replay', 'presence', base + lag, seq, in_room=state))
        self.check_occupancy(rooms)
        self.check_health(devices)
        _, feed, _, _ = self.call('/alarms?after_id=0&wait=0')
        cursor = feed['next_cursor']
        before_ids = [a['event_id'] for a in feed['alarms']]
        self.record('feed_initial_complete', len(before_ids) == len(self.expected_falls()), actual=feed)
        late = self.event('dev_gap', 'room_gap', 'fall_warn', base - 1500, 1, confidence=.88)
        self.post(late)
        before_snapshot = self.check_alarms()

        self.phase = 'sigkill_restart'
        pid = self.server.pid
        self.server.send_signal(signal.SIGKILL)
        killed_code = self.server.wait(timeout=10)
        self.server_log.close()
        self.record('hard_kill_observed', killed_code == -signal.SIGKILL, pid=pid, exit_code=killed_code)
        self.start()
        self.check_occupancy(rooms)
        self.check_health(devices)
        after_snapshot = self.check_alarms()
        self.record('alarm_identities_survive_restart', before_snapshot == after_snapshot,
                    before=before_snapshot, after=after_snapshot)
        _, resumed, _, _ = self.call(f'/alarms?after_id={cursor}&wait=0')
        resumed_alarms = resumed.get('alarms', [])
        self.record('cursor_resume_gap_exactly_once', len(resumed_alarms) == 1
                    and resumed_alarms[0]['device_id'] == 'dev_gap'
                    and resumed_alarms[0]['ts'] == late['ts']
                    and resumed['next_cursor'] > cursor, cursor=cursor, actual=resumed)
        _, empty, _, _ = self.call(f"/alarms?after_id={resumed['next_cursor']}&wait=0")
        self.record('cursor_resume_no_repetition', empty.get('alarms') == []
                    and empty.get('next_cursor') == resumed['next_cursor'], actual=empty)
        self.post(late, exact_retry=True)
        self.check_alarms()
        with psycopg2.connect(self.db_url) as conn:
            with conn.cursor() as c:
                c.execute('SELECT count(*) FROM event_log')
                count = c.fetchone()[0]
        expected = len({(e['device_id'], e['seq'], e['ts']) for e in self.events})
        self.record('journal_acknowledged_truth_after_restart', count == expected,
                    expected_unique_event_count=expected, actual_count=count)

    def finish(self, error=None):
        if self.server is not None and self.server.poll() is None:
            self.server.terminate()
            self.server.wait(timeout=10)
        if getattr(self, 'server_log', None) and not self.server_log.closed:
            self.server_log.close()
        if self.admin is not None:
            with self.admin.cursor() as c:
                c.execute('SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=%s', (self.database,))
                c.execute(sql.SQL('DROP DATABASE IF EXISTS {}').format(sql.Identifier(self.database)))
            self.admin.close()
        summary = Counter(c['name'] for c in self.checks)
        report = {'database': self.database, 'database_dropped': True, 'error': error,
                  'provenance': getattr(self, 'provenance', {}), 'check_counts_by_name': dict(summary),
                  'checks': self.checks, 'passed': sum(c['pass'] for c in self.checks),
                  'failed': sum(not c['pass'] for c in self.checks), 'event_count': len(self.events),
                  'policy_notes': [
                      'Occupancy truth integrates original event timestamps; pre-first-event time is unknown.',
                      'Equal-time presence assumes ascending device_id then seq; contract leaves ties unspecified.',
                      'Latest future presence determines current boolean; future durations do not count.',
                      'Health uses 300 distinct one-second lag bins on calibrated device time; exact implementation bin convention is a policy assumption.',
                      'Replay-only health has no credible ±60s calibration sample, expected offset exactly zero.',
                      'Skew anchors expected offset is computed only from independently observed HTTP receipt interval.',
                      'Falls follow official generator identity: same device/room/ts/confidence with changed seq is one fall.',
                      'No synthetic clock injection; expectations cover every admissible request time.',
                      'This is correctness coverage at low event rate, not evidence of correctness during 50k/s bursts or a private score.'
                  ]}
        (self.out / 'report.json').write_text(json.dumps(report, indent=2) + '\n')
        (self.out / 'http.jsonl').write_text(''.join(json.dumps(r) + '\n' for r in self.requests))
        (self.out / 'event_truth.json').write_text(json.dumps(self.events, indent=2) + '\n')
        print(json.dumps({k: report[k] for k in ['passed', 'failed', 'event_count', 'error']}), flush=True)
        return 1 if report['failed'] or error else 0


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--out', type=Path)
    parser.add_argument('--repo', type=Path, default=DEFAULT_REPO)
    args = parser.parse_args()
    out = args.out or Path(tempfile.mkdtemp(prefix='teton-verification-'))
    probe = Probe(out.resolve(), args.repo.resolve())
    error = None
    try:
        probe.setup()
        probe.scenarios()
    except Exception as exc:
        import traceback
        error = traceback.format_exc()
        print(error, file=sys.stderr)
    return probe.finish(error)


if __name__ == '__main__':
    sys.exit(main())

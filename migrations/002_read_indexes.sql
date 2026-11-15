-- Device health reads heartbeats per device by event time.
CREATE INDEX event_log_heartbeat_idx ON event_log (device_id, ts)
    WHERE type = 'heartbeat';

-- Room occupancy reads presence transitions per room in deterministic
-- event-time order: ts, then device_id, then seq.
CREATE INDEX event_log_presence_idx ON event_log (room_id, ts, device_id, seq)
    WHERE type = 'presence';

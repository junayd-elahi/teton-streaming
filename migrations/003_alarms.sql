-- One row per logical fall. A sensor can report the same fall several times
-- with new sequence numbers but the same device, room, timestamp and
-- confidence; the unique key collapses those into one alarm.
--
-- id is the feed cursor. Alarms are inserted only by the single journal
-- writer, inside the same transaction as their event, so ids become visible
-- in increasing order and a reader holding cursor N never misses a later row.
CREATE TABLE alarms (
    id          bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    device_id   text        NOT NULL,
    room_id     text        NOT NULL,
    ts          timestamptz NOT NULL,
    confidence  numeric     NOT NULL,
    seq         bigint      NOT NULL,
    received_at timestamptz NOT NULL,
    UNIQUE (device_id, room_id, ts, confidence)
);

CREATE INDEX alarms_ts_idx ON alarms (ts, id);

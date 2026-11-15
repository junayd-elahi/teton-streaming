CREATE TABLE event_log (
    id           bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    device_id    text        NOT NULL,
    seq          bigint      NOT NULL,
    room_id      text        NOT NULL,
    type         text        NOT NULL,
    ts           timestamptz NOT NULL,
    payload      jsonb       NOT NULL,
    payload_hash bytea       NOT NULL,
    received_at  timestamptz NOT NULL,
    UNIQUE (device_id, seq)
);

-- A device's sequence numbers restart when the device (or a test generator)
-- restarts, so (device_id, seq) alone can repeat with a different event. The
-- event time is added to the identity: a true retry carries the same
-- timestamp, a new event after a restart does not.
ALTER TABLE event_log DROP CONSTRAINT event_log_device_id_seq_key;
ALTER TABLE event_log ADD CONSTRAINT event_log_identity_key UNIQUE (device_id, seq, ts);

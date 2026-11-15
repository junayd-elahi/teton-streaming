-- Existing consumers may already have recorded the old public identifiers.
ALTER TABLE alarms ADD COLUMN event_id text;
UPDATE alarms SET event_id = device_id || ':' || seq::text;
ALTER TABLE alarms ALTER COLUMN event_id SET NOT NULL;
-- New identifiers remain unique when a device's sequence restarts.
ALTER TABLE alarms ALTER COLUMN event_id
    SET DEFAULT ('alarm:' || gen_random_uuid()::text);

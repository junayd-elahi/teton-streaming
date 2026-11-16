-- Select the newest credible device-time sample without scanning its history.
CREATE INDEX event_log_heartbeat_event_time_calibration_idx
    ON event_log (device_id, ts DESC, received_at DESC, id DESC)
    WHERE type = 'heartbeat'
      AND (ts - received_at) BETWEEN INTERVAL '-60 seconds' AND INTERVAL '60 seconds';

DROP INDEX event_log_heartbeat_calibration_idx;

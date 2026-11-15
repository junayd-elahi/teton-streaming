-- Find recent credible calibration without scanning a device's lifetime history.
CREATE INDEX event_log_heartbeat_calibration_idx
    ON event_log (device_id, received_at DESC, ts DESC, id DESC)
    WHERE type = 'heartbeat'
      AND (ts - received_at) BETWEEN INTERVAL '-60 seconds' AND INTERVAL '60 seconds';

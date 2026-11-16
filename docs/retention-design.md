# Retention proposal

Design only. No journal or alarm deletion is implemented.

1. **Define the horizons.** A conservative raw-history cutoff would retain the largest read window (one hour), the supported late-replay interval (one hour), and a clock/processing margin. Current ingestion also accepts older timestamps, and retained identities are recognized indefinitely. Those existing behaviours need an explicit expiry contract before a finite cutoff is safe.
2. **Checkpoint room history.** Keep the last presence state before the cutoff, its original ordering key `(ts, device_id, seq)`, and whether a state is known. Otherwise a quiet, occupied room becomes unknown after cleanup.
3. **Checkpoint device history.** Keep the maximum original heartbeat timestamp and the latest credible calibration sample in receipt order. Raw rows alone cannot preserve these for a long-offline device after cleanup.
4. **Retain retry evidence.** Store each retained event identity and its canonical input hash for an agreed retry horizon. The hash must precede stored-payload normalization. Expired submissions need an explicit outcome; they must not silently become new events.
5. **Keep alarm contracts separate.** Preserve logical-fall deduplication keys, stable public identifiers and monotonically resumable cursors. Define a consumer retention horizon and an explicit expired-cursor response before deleting alarm history.
6. **Compact atomically and verify equivalence.** Commit checkpoints and cleanup together in bounded work. Test concurrent late inserts, interrupted compaction and restart. Before/after results must match for health, all occupancy windows, retries/conflicts, late replay and feed resume.

Choose partitioning or bounded deletion after measuring its migration, index and WAL costs. A simple timestamp-based purge is unsafe: an independent probe already demonstrated loss of occupied-room state and the last heartbeat.

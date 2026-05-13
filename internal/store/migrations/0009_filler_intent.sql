-- 0009: per-bot intent-classified filler. `filler_mode`
--   - 'short'  (default): play a random file from the existing short
--                          pool (back-compat with current behaviour).
--   - 'hybrid': call `filler_intent_url` with the user transcript; if
--               it returns BUSINESS → play a `long/` filler, CHITCHAT
--               → short. Empty url / timeout / unknown response falls
--               back to short so the bot never goes silent.
--
-- filler_intent_timeout_ms is the hard ceiling on the classify call.
-- The filler decision races this against the bot's first-sentence
-- signal so a fast bot skips filler entirely.

ALTER TABLE bots
    ADD COLUMN IF NOT EXISTS filler_mode TEXT NOT NULL DEFAULT 'short'
        CHECK (filler_mode IN ('short','hybrid'));

ALTER TABLE bots
    ADD COLUMN IF NOT EXISTS filler_intent_url TEXT;

ALTER TABLE bots
    ADD COLUMN IF NOT EXISTS filler_intent_timeout_ms INT NOT NULL DEFAULT 1500
        CHECK (filler_intent_timeout_ms BETWEEN 50 AND 5000);

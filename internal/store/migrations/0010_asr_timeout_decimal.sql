-- 0010: allow sub-second ASR timeouts.
--
-- The original schema stored asr_*_sec as INT — fine for the Viettel
-- minimum of 1 s, but operators wanted to tune below a second (e.g.
-- 0.5 s silence timeout to catch quick interjections). Convert to
-- NUMERIC(5,2) so values like 0.5, 1.2, 12.75 store cleanly. INT
-- defaults round-trip without surprise (PG widens 5 → 5.00).
--
-- Old INT-only CHECK constraints (BETWEEN 1 AND 60 etc.) reject 0.5
-- so we drop and re-add them with the wider lower bound. PG named
-- the originals automatically — discover them via pg_constraint and
-- drop dynamically so the migration doesn't break if the catalog
-- assigned a different suffix.

ALTER TABLE bots ALTER COLUMN asr_silence_timeout_sec TYPE NUMERIC(5,2)
    USING asr_silence_timeout_sec::NUMERIC(5,2);
ALTER TABLE bots ALTER COLUMN asr_speech_timeout_sec  TYPE NUMERIC(5,2)
    USING asr_speech_timeout_sec::NUMERIC(5,2);
ALTER TABLE bots ALTER COLUMN asr_speech_max_sec      TYPE NUMERIC(5,2)
    USING asr_speech_max_sec::NUMERIC(5,2);

DO $$
DECLARE r record;
BEGIN
    FOR r IN
        SELECT conname FROM pg_constraint
         WHERE conrelid = 'bots'::regclass
           AND contype  = 'c'
           AND (pg_get_constraintdef(oid) ILIKE '%asr_silence_timeout_sec%'
                OR pg_get_constraintdef(oid) ILIKE '%asr_speech_timeout_sec%'
                OR pg_get_constraintdef(oid) ILIKE '%asr_speech_max_sec%')
    LOOP
        EXECUTE format('ALTER TABLE bots DROP CONSTRAINT %I', r.conname);
    END LOOP;
END $$;

ALTER TABLE bots
    ADD CONSTRAINT bots_asr_silence_timeout_sec_range
        CHECK (asr_silence_timeout_sec BETWEEN 0.1 AND 60);
ALTER TABLE bots
    ADD CONSTRAINT bots_asr_speech_timeout_sec_range
        CHECK (asr_speech_timeout_sec BETWEEN 0.1 AND 60);
ALTER TABLE bots
    ADD CONSTRAINT bots_asr_speech_max_sec_range
        CHECK (asr_speech_max_sec BETWEEN 1 AND 600);

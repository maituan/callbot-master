-- 0003: bot configuration + inbound DID routing.

CREATE TABLE IF NOT EXISTS bots (
    id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    tenant_id   UUID NOT NULL REFERENCES tenants(id) ON DELETE RESTRICT,
    slug        TEXT NOT NULL
                  CHECK (slug ~ '^[a-z0-9][a-z0-9-]{1,40}[a-z0-9]$'),
    name        TEXT NOT NULL,
    enabled     BOOLEAN NOT NULL DEFAULT true,

    -- Connection
    bot_url                   TEXT NOT NULL,
    bot_first_byte_timeout_ms INT  NOT NULL DEFAULT 5000
                                CHECK (bot_first_byte_timeout_ms BETWEEN 100 AND 60000),
    bot_total_timeout_ms      INT  NOT NULL DEFAULT 25000
                                CHECK (bot_total_timeout_ms BETWEEN 1000 AND 300000),

    asr_provider              TEXT NOT NULL DEFAULT 'viettel'
                                CHECK (asr_provider IN ('viettel','google','azure')),
    asr_endpoint              TEXT NOT NULL,
    asr_token                 TEXT,

    tts_provider              TEXT NOT NULL DEFAULT 'viettel'
                                CHECK (tts_provider IN ('viettel','google','azure')),
    tts_endpoint              TEXT NOT NULL,
    tts_token                 TEXT,

    -- Provider params
    tts_voice_id              TEXT,
    tts_tempo                 NUMERIC(3,2) NOT NULL DEFAULT 1.00
                                CHECK (tts_tempo BETWEEN 0.50 AND 2.00),
    asr_silence_timeout_sec   INT NOT NULL DEFAULT 5
                                CHECK (asr_silence_timeout_sec BETWEEN 1 AND 60),
    asr_speech_timeout_sec    INT NOT NULL DEFAULT 1
                                CHECK (asr_speech_timeout_sec BETWEEN 1 AND 60),
    asr_speech_max_sec        INT NOT NULL DEFAULT 30
                                CHECK (asr_speech_max_sec BETWEEN 1 AND 600),
    asr_single_sentence       BOOLEAN NOT NULL DEFAULT true,

    -- Behavior
    bargein_enabled           BOOLEAN NOT NULL DEFAULT true,
    bargein_min_words         INT NOT NULL DEFAULT 3
                                CHECK (bargein_min_words BETWEEN 1 AND 20),
    filler_enabled            BOOLEAN NOT NULL DEFAULT false,

    version     INT NOT NULL DEFAULT 1,
    created_by  UUID REFERENCES users(id) ON DELETE SET NULL,
    updated_by  UUID REFERENCES users(id) ON DELETE SET NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    deleted_at  TIMESTAMPTZ,

    UNIQUE (tenant_id, slug)
);

CREATE INDEX IF NOT EXISTS idx_bots_tenant_active
    ON bots (tenant_id) WHERE deleted_at IS NULL;

CREATE TABLE IF NOT EXISTS bot_inbound_dids (
    did         TEXT PRIMARY KEY
                  CHECK (did ~ '^[0-9+*#]+$' AND length(did) BETWEEN 3 AND 32),
    bot_id      UUID NOT NULL REFERENCES bots(id) ON DELETE RESTRICT,
    note        TEXT,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_bot_inbound_dids_bot ON bot_inbound_dids (bot_id);

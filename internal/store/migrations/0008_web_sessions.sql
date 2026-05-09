-- 0008: web playground (chat + voice) sessions for public bot share links.
-- Independent from call_history because lifecycle differs: no FreeSWITCH
-- UUID, no recording archive, visitor-driven via JWT share token.

CREATE TABLE IF NOT EXISTS web_session (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    bot_id          UUID NOT NULL REFERENCES bots(id) ON DELETE CASCADE,
    tenant_id       UUID NOT NULL REFERENCES tenants(id) ON DELETE CASCADE,
    channel         TEXT NOT NULL CHECK (channel IN ('chat','voice')),
    -- JWT iat (issued-at) of the share token used to start the session;
    -- not a FK because share tokens are stateless (no jti registry yet).
    -- Useful for grouping sessions by share-link campaign.
    token_iat       TIMESTAMPTZ,
    ip              INET,
    user_agent      TEXT,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT now(),
    ended_at        TIMESTAMPTZ,
    status          TEXT NOT NULL DEFAULT 'active'
                      CHECK (status IN ('active','ended','aborted','error')),
    total_turns     INT NOT NULL DEFAULT 0,
    -- Voice-only: archive dir for per-turn TTS WAV dumps (one folder per
    -- session). Null for chat or when MASTER_WEB_RECORDING is off.
    recording_dir   TEXT,
    error_message   TEXT
);

CREATE INDEX IF NOT EXISTS idx_web_session_bot_time
    ON web_session (bot_id, started_at DESC);
CREATE INDEX IF NOT EXISTS idx_web_session_tenant_time
    ON web_session (tenant_id, started_at DESC);

CREATE TABLE IF NOT EXISTS web_turn (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    session_id          UUID NOT NULL REFERENCES web_session(id) ON DELETE CASCADE,
    idx                 INT  NOT NULL,
    role                TEXT NOT NULL CHECK (role IN ('user','bot')),
    text                TEXT NOT NULL DEFAULT '',
    -- Voice TTS WAV path under recording_dir (relative). Null for chat
    -- or for user turns.
    audio_path          TEXT,
    asr_partial_at      TIMESTAMPTZ,
    asr_final_at        TIMESTAMPTZ,
    bot_first_byte_at   TIMESTAMPTZ,
    bot_done_at         TIMESTAMPTZ,
    tts_first_audio_at  TIMESTAMPTZ,
    tts_done_at         TIMESTAMPTZ,
    action              TEXT,    -- CHAT | ENDCALL (bot-emitted)
    created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (session_id, idx)
);

CREATE INDEX IF NOT EXISTS idx_web_turn_session ON web_turn (session_id, idx);

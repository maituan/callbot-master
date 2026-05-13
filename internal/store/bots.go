package store

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
)

// BotConfig is the snapshot of a bot's settings used to construct providers
// for one call. Mirrors the bots table 1:1 — pass by value, copy is cheap.
type BotConfig struct {
	ID         uuid.UUID
	TenantID   uuid.UUID
	TenantSlug string
	Slug       string
	Name       string
	Enabled    bool

	// Connection
	BotURL                string
	BotFirstByteTimeoutMs int
	BotTotalTimeoutMs     int

	ASRProvider string
	ASREndpoint string
	ASRToken    string

	TTSProvider string
	TTSEndpoint string
	TTSToken    string

	// Provider params
	TTSVoiceID            string
	TTSTempo              float64
	ASRSilenceTimeoutSec  int
	ASRSpeechTimeoutSec   int
	ASRSpeechMaxSec       int
	ASRSingleSentence     bool

	// Behavior
	BargeInEnabled  bool
	BargeInMinWords int
	FillerEnabled   bool

	// Filler intent classification — when FillerMode="hybrid" and
	// FillerIntentURL is set, master POSTs the user transcript to
	// FillerIntentURL and expects a plain-text BUSINESS/CHITCHAT reply
	// within FillerIntentTimeoutMs. BUSINESS → long filler, CHITCHAT or
	// any failure → short. Mode "short" (default) bypasses the API.
	FillerMode            string // "short" | "hybrid"
	FillerIntentURL       string
	FillerIntentTimeoutMs int

	// OutboundPrefix is prepended to the dialed phone before originate
	// when the phone doesn't already start with it. Routes the call to
	// the right carrier gateway via the FS dialplan (e.g. "3323" →
	// minhphuc-vina, "3317" → leeon-viettel).
	OutboundPrefix string

	Version   int
	CreatedAt time.Time
	UpdatedAt time.Time
}

// BotFirstByteTimeout / BotTotalTimeout convert the stored milliseconds to
// time.Duration for caller convenience.
func (b *BotConfig) BotFirstByteTimeout() time.Duration {
	return time.Duration(b.BotFirstByteTimeoutMs) * time.Millisecond
}
func (b *BotConfig) BotTotalTimeout() time.Duration {
	return time.Duration(b.BotTotalTimeoutMs) * time.Millisecond
}

const botSelectCols = `
b.id, b.tenant_id, t.slug, b.slug, b.name, b.enabled,
b.bot_url, b.bot_first_byte_timeout_ms, b.bot_total_timeout_ms,
b.asr_provider, b.asr_endpoint, COALESCE(b.asr_token, ''),
b.tts_provider, b.tts_endpoint, COALESCE(b.tts_token, ''),
COALESCE(b.tts_voice_id, ''), b.tts_tempo,
b.asr_silence_timeout_sec, b.asr_speech_timeout_sec, b.asr_speech_max_sec,
b.asr_single_sentence,
b.bargein_enabled, b.bargein_min_words, b.filler_enabled,
COALESCE(b.filler_mode, 'short'),
COALESCE(b.filler_intent_url, ''),
COALESCE(b.filler_intent_timeout_ms, 1500),
COALESCE(b.outbound_prefix, ''),
b.version, b.created_at, b.updated_at`

// GetBotByDID resolves the inbound DID → bot. Returns nil,nil when no
// route exists (typical: dialplan sent a DID we don't own). Filters out
// disabled / soft-deleted bots so a flipped flag actually breaks routing.
func (p *Postgres) GetBotByDID(ctx context.Context, did string) (*BotConfig, error) {
	const q = `
SELECT ` + botSelectCols + `
FROM bot_inbound_dids d
JOIN bots b    ON b.id = d.bot_id    AND b.enabled AND b.deleted_at IS NULL
JOIN tenants t ON t.id = b.tenant_id AND t.enabled
WHERE d.did = $1`
	row := p.pool.QueryRow(ctx, q, did)
	bot, err := scanBot(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return bot, err
}

func (p *Postgres) GetBotByID(ctx context.Context, id uuid.UUID) (*BotConfig, error) {
	const q = `
SELECT ` + botSelectCols + `
FROM bots b
JOIN tenants t ON t.id = b.tenant_id
WHERE b.id = $1 AND b.deleted_at IS NULL`
	row := p.pool.QueryRow(ctx, q, id)
	bot, err := scanBot(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return bot, err
}

// GetBotByTenantAndSlug is the lookup outbound campaigns use when they
// only know the tenant + bot slug (e.g. "hcc"/"hcc-laichau").
func (p *Postgres) GetBotByTenantAndSlug(ctx context.Context, tenantID uuid.UUID, slug string) (*BotConfig, error) {
	const q = `
SELECT ` + botSelectCols + `
FROM bots b
JOIN tenants t ON t.id = b.tenant_id
WHERE b.tenant_id = $1 AND b.slug = $2 AND b.deleted_at IS NULL`
	row := p.pool.QueryRow(ctx, q, tenantID, slug)
	bot, err := scanBot(row)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, nil
	}
	return bot, err
}

// ListBots returns every bot a caller is allowed to see. Pass nil for
// tenantID when the caller is platform_admin (sees every tenant).
func (p *Postgres) ListBots(ctx context.Context, tenantID *uuid.UUID) ([]*BotConfig, error) {
	q := `
SELECT ` + botSelectCols + `
FROM bots b
JOIN tenants t ON t.id = b.tenant_id
WHERE b.deleted_at IS NULL`
	args := []any{}
	if tenantID != nil {
		q += " AND b.tenant_id = $1"
		args = append(args, *tenantID)
	}
	q += " ORDER BY t.slug, b.slug"
	rows, err := p.pool.Query(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []*BotConfig
	for rows.Next() {
		b, err := scanBot(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// SeedDefaultBot is called on startup when the DB has no bots yet. It
// materialises a tenant + bot + DID from the legacy env config so
// existing deployments keep working without UI configuration. Returns
// the bot id; idempotent thanks to ON CONFLICT clauses.
func (p *Postgres) SeedDefaultBot(ctx context.Context, in SeedBotInput) (uuid.UUID, error) {
	tx, err := p.pool.Begin(ctx)
	if err != nil {
		return uuid.Nil, fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback(ctx) }()

	var tenantID uuid.UUID
	if err := tx.QueryRow(ctx,
		`INSERT INTO tenants (slug, name) VALUES ($1, $2)
		 ON CONFLICT (slug) DO UPDATE SET name = EXCLUDED.name
		 RETURNING id`, in.TenantSlug, in.TenantName).Scan(&tenantID); err != nil {
		return uuid.Nil, fmt.Errorf("upsert tenant: %w", err)
	}

	var botID uuid.UUID
	if err := tx.QueryRow(ctx, `
INSERT INTO bots (
    tenant_id, slug, name,
    bot_url, bot_first_byte_timeout_ms, bot_total_timeout_ms,
    asr_endpoint, asr_token,
    tts_endpoint, tts_token, tts_voice_id, tts_tempo,
    asr_silence_timeout_sec, asr_speech_timeout_sec, asr_speech_max_sec,
    bargein_enabled, bargein_min_words, filler_enabled,
    outbound_prefix
)
VALUES ($1,$2,$3, $4,$5,$6, $7,$8, $9,$10,$11,$12,
        $13,$14,$15, $16,$17,$18, $19)
ON CONFLICT (tenant_id, slug) DO UPDATE SET name = EXCLUDED.name
RETURNING id`,
		tenantID, in.BotSlug, in.BotName,
		in.BotURL, in.BotFirstByteTimeoutMs, in.BotTotalTimeoutMs,
		in.ASREndpoint, nullStr(in.ASRToken),
		in.TTSEndpoint, nullStr(in.TTSToken), nullStr(in.TTSVoiceID), in.TTSTempo,
		in.ASRSilenceTimeoutSec, in.ASRSpeechTimeoutSec, in.ASRSpeechMaxSec,
		in.BargeInEnabled, in.BargeInMinWords, in.FillerEnabled,
		in.OutboundPrefix,
	).Scan(&botID); err != nil {
		return uuid.Nil, fmt.Errorf("upsert bot: %w", err)
	}

	if in.DID != "" {
		if _, err := tx.Exec(ctx,
			`INSERT INTO bot_inbound_dids (did, bot_id) VALUES ($1, $2)
			 ON CONFLICT (did) DO NOTHING`, in.DID, botID); err != nil {
			return uuid.Nil, fmt.Errorf("seed did: %w", err)
		}
	}

	if err := tx.Commit(ctx); err != nil {
		return uuid.Nil, fmt.Errorf("commit: %w", err)
	}
	return botID, nil
}

// CountBots is used by main.go to decide whether to seed a default.
func (p *Postgres) CountBots(ctx context.Context) (int, error) {
	var n int
	err := p.pool.QueryRow(ctx, `SELECT count(*) FROM bots WHERE deleted_at IS NULL`).Scan(&n)
	return n, err
}

// CreateBot inserts a new bot row. Caller is responsible for tenant scope
// (passing the right tenant_id from the authenticated identity). Returns
// the inserted row id.
func (p *Postgres) CreateBot(ctx context.Context, in BotWriteInput) (uuid.UUID, error) {
	const q = `
INSERT INTO bots (
    tenant_id, slug, name, enabled,
    bot_url, bot_first_byte_timeout_ms, bot_total_timeout_ms,
    asr_provider, asr_endpoint, asr_token,
    tts_provider, tts_endpoint, tts_token,
    tts_voice_id, tts_tempo,
    asr_silence_timeout_sec, asr_speech_timeout_sec, asr_speech_max_sec, asr_single_sentence,
    bargein_enabled, bargein_min_words, filler_enabled,
    filler_mode, filler_intent_url, filler_intent_timeout_ms,
    outbound_prefix,
    created_by, updated_by
)
VALUES ($1,$2,$3,$4, $5,$6,$7, $8,$9,$10, $11,$12,$13,
        $14,$15, $16,$17,$18,$19, $20,$21,$22,
        $23,$24,$25,
        $26, $27,$27)
RETURNING id`
	mode := in.FillerMode
	if mode == "" {
		mode = "short"
	}
	timeout := in.FillerIntentTimeoutMs
	if timeout == 0 {
		timeout = 1500
	}
	var id uuid.UUID
	if err := p.pool.QueryRow(ctx, q,
		in.TenantID, in.Slug, in.Name, in.Enabled,
		in.BotURL, in.BotFirstByteTimeoutMs, in.BotTotalTimeoutMs,
		in.ASRProvider, in.ASREndpoint, nullStr(in.ASRToken),
		in.TTSProvider, in.TTSEndpoint, nullStr(in.TTSToken),
		nullStr(in.TTSVoiceID), in.TTSTempo,
		in.ASRSilenceTimeoutSec, in.ASRSpeechTimeoutSec, in.ASRSpeechMaxSec, in.ASRSingleSentence,
		in.BargeInEnabled, in.BargeInMinWords, in.FillerEnabled,
		mode, nullStr(in.FillerIntentURL), timeout,
		in.OutboundPrefix,
		in.ActorUserID,
	).Scan(&id); err != nil {
		return uuid.Nil, fmt.Errorf("create bot: %w", err)
	}
	return id, nil
}

// UpdateBot applies a full-record update with optimistic locking on
// version. tokenPolicy controls whether the new asr/tts tokens replace
// or preserve the existing ones (since the API masks tokens, the UI
// only sends them when the user explicitly rotates).
//
// Returns ErrVersionMismatch if version on disk doesn't match expected.
func (p *Postgres) UpdateBot(ctx context.Context, id uuid.UUID, in BotWriteInput, expectedVersion int) error {
	const q = `
UPDATE bots SET
    slug                       = $1,
    name                       = $2,
    enabled                    = $3,
    bot_url                    = $4,
    bot_first_byte_timeout_ms  = $5,
    bot_total_timeout_ms       = $6,
    asr_provider               = $7,
    asr_endpoint               = $8,
    asr_token                  = CASE WHEN $9::int = 1 THEN $10 ELSE asr_token END,
    tts_provider               = $11,
    tts_endpoint               = $12,
    tts_token                  = CASE WHEN $13::int = 1 THEN $14 ELSE tts_token END,
    tts_voice_id               = $15,
    tts_tempo                  = $16,
    asr_silence_timeout_sec    = $17,
    asr_speech_timeout_sec     = $18,
    asr_speech_max_sec         = $19,
    asr_single_sentence        = $20,
    bargein_enabled            = $21,
    bargein_min_words          = $22,
    filler_enabled             = $23,
    filler_mode                = $24,
    filler_intent_url          = $25,
    filler_intent_timeout_ms   = $26,
    outbound_prefix            = $27,
    version                    = version + 1,
    updated_by                 = $28
WHERE id = $29 AND version = $30 AND deleted_at IS NULL`
	mode := in.FillerMode
	if mode == "" {
		mode = "short"
	}
	timeout := in.FillerIntentTimeoutMs
	if timeout == 0 {
		timeout = 1500
	}
	tag, err := p.pool.Exec(ctx, q,
		in.Slug, in.Name, in.Enabled,
		in.BotURL, in.BotFirstByteTimeoutMs, in.BotTotalTimeoutMs,
		in.ASRProvider, in.ASREndpoint,
		boolToInt(in.ReplaceASRToken), nullStr(in.ASRToken),
		in.TTSProvider, in.TTSEndpoint,
		boolToInt(in.ReplaceTTSToken), nullStr(in.TTSToken),
		nullStr(in.TTSVoiceID), in.TTSTempo,
		in.ASRSilenceTimeoutSec, in.ASRSpeechTimeoutSec, in.ASRSpeechMaxSec, in.ASRSingleSentence,
		in.BargeInEnabled, in.BargeInMinWords, in.FillerEnabled,
		mode, nullStr(in.FillerIntentURL), timeout,
		in.OutboundPrefix,
		in.ActorUserID,
		id, expectedVersion,
	)
	if err != nil {
		return fmt.Errorf("update bot: %w", err)
	}
	if tag.RowsAffected() == 0 {
		return ErrVersionMismatch
	}
	return nil
}

// SoftDeleteBot marks the bot as deleted (deleted_at = now()). Bot row
// stays so call_history.bot_id keeps its FK target.
func (p *Postgres) SoftDeleteBot(ctx context.Context, id uuid.UUID) error {
	_, err := p.pool.Exec(ctx,
		`UPDATE bots SET deleted_at = now() WHERE id = $1 AND deleted_at IS NULL`, id)
	return err
}

// AddDID maps a DID → bot. Returns ErrDIDTaken if the DID is already
// owned by some other bot (DID is unique global per the schema).
func (p *Postgres) AddDID(ctx context.Context, did string, botID uuid.UUID, note string) error {
	_, err := p.pool.Exec(ctx,
		`INSERT INTO bot_inbound_dids (did, bot_id, note) VALUES ($1, $2, $3)`,
		did, botID, nullStr(note))
	if err != nil && isUniqueViolation(err) {
		return ErrDIDTaken
	}
	return err
}

// RemoveDID deletes the mapping. Idempotent — no error if the DID
// wasn't there to begin with.
func (p *Postgres) RemoveDID(ctx context.Context, did string) error {
	_, err := p.pool.Exec(ctx,
		`DELETE FROM bot_inbound_dids WHERE did = $1`, did)
	return err
}

// ListDIDs returns the DIDs assigned to a given bot, oldest first.
func (p *Postgres) ListDIDs(ctx context.Context, botID uuid.UUID) ([]DIDRecord, error) {
	rows, err := p.pool.Query(ctx,
		`SELECT did, bot_id, COALESCE(note,''), created_at
		   FROM bot_inbound_dids WHERE bot_id = $1 ORDER BY created_at`,
		botID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []DIDRecord
	for rows.Next() {
		var r DIDRecord
		if err := rows.Scan(&r.DID, &r.BotID, &r.Note, &r.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

type DIDRecord struct {
	DID       string
	BotID     uuid.UUID
	Note      string
	CreatedAt time.Time
}

// BotWriteInput is the create/update payload the API constructs from
// incoming JSON. ReplaceASRToken/ReplaceTTSToken are sentinels: when
// false, the existing token in DB is preserved (so masked tokens can
// safely round-trip through the form without leaking plaintext).
type BotWriteInput struct {
	TenantID uuid.UUID
	Slug     string
	Name     string
	Enabled  bool

	BotURL                string
	BotFirstByteTimeoutMs int
	BotTotalTimeoutMs     int

	ASRProvider     string
	ASREndpoint     string
	ASRToken        string
	ReplaceASRToken bool

	TTSProvider     string
	TTSEndpoint     string
	TTSToken        string
	ReplaceTTSToken bool

	TTSVoiceID            string
	TTSTempo              float64
	ASRSilenceTimeoutSec  int
	ASRSpeechTimeoutSec   int
	ASRSpeechMaxSec       int
	ASRSingleSentence     bool

	BargeInEnabled  bool
	BargeInMinWords int
	FillerEnabled   bool

	FillerMode            string
	FillerIntentURL       string
	FillerIntentTimeoutMs int

	OutboundPrefix string

	ActorUserID *uuid.UUID
}

// ErrVersionMismatch is returned by UpdateBot when the caller's expected
// version doesn't match the row's current version — UI should refetch
// and show a "stale form" notice.
var ErrVersionMismatch = errors.New("bot version mismatch (someone else updated)")

// ErrDIDTaken is returned by AddDID when the DID is already mapped.
var ErrDIDTaken = errors.New("DID already assigned to another bot")

func boolToInt(b bool) int {
	if b {
		return 1
	}
	return 0
}

// isUniqueViolation matches Postgres SQLSTATE 23505 by string-matching
// on the formatted error rather than importing pgx/pgconn types.
func isUniqueViolation(err error) bool {
	return err != nil && strings.Contains(err.Error(), "23505")
}

type SeedBotInput struct {
	TenantSlug string
	TenantName string
	BotSlug    string
	BotName    string
	DID        string

	BotURL                string
	BotFirstByteTimeoutMs int
	BotTotalTimeoutMs     int

	ASREndpoint string
	ASRToken    string

	TTSEndpoint string
	TTSToken    string
	TTSVoiceID  string
	TTSTempo    float64

	ASRSilenceTimeoutSec int
	ASRSpeechTimeoutSec  int
	ASRSpeechMaxSec      int

	BargeInEnabled  bool
	BargeInMinWords int
	FillerEnabled   bool

	OutboundPrefix string
}

func scanBot(row scannable) (*BotConfig, error) {
	var b BotConfig
	if err := row.Scan(
		&b.ID, &b.TenantID, &b.TenantSlug, &b.Slug, &b.Name, &b.Enabled,
		&b.BotURL, &b.BotFirstByteTimeoutMs, &b.BotTotalTimeoutMs,
		&b.ASRProvider, &b.ASREndpoint, &b.ASRToken,
		&b.TTSProvider, &b.TTSEndpoint, &b.TTSToken,
		&b.TTSVoiceID, &b.TTSTempo,
		&b.ASRSilenceTimeoutSec, &b.ASRSpeechTimeoutSec, &b.ASRSpeechMaxSec,
		&b.ASRSingleSentence,
		&b.BargeInEnabled, &b.BargeInMinWords, &b.FillerEnabled,
		&b.FillerMode, &b.FillerIntentURL, &b.FillerIntentTimeoutMs,
		&b.OutboundPrefix,
		&b.Version, &b.CreatedAt, &b.UpdatedAt,
	); err != nil {
		return nil, err
	}
	return &b, nil
}

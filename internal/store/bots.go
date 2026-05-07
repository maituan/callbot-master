package store

import (
	"context"
	"errors"
	"fmt"
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
    bargein_enabled, bargein_min_words, filler_enabled
)
VALUES ($1,$2,$3, $4,$5,$6, $7,$8, $9,$10,$11,$12,
        $13,$14,$15, $16,$17,$18)
ON CONFLICT (tenant_id, slug) DO UPDATE SET name = EXCLUDED.name
RETURNING id`,
		tenantID, in.BotSlug, in.BotName,
		in.BotURL, in.BotFirstByteTimeoutMs, in.BotTotalTimeoutMs,
		in.ASREndpoint, nullStr(in.ASRToken),
		in.TTSEndpoint, nullStr(in.TTSToken), nullStr(in.TTSVoiceID), in.TTSTempo,
		in.ASRSilenceTimeoutSec, in.ASRSpeechTimeoutSec, in.ASRSpeechMaxSec,
		in.BargeInEnabled, in.BargeInMinWords, in.FillerEnabled,
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
		&b.Version, &b.CreatedAt, &b.UpdatedAt,
	); err != nil {
		return nil, err
	}
	return &b, nil
}

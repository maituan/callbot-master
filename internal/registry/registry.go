// Package registry materialises ASR/TTS/Bot clients for one call from a
// store.BotConfig. ASR clients hold an expensive gRPC dial, so they're
// pooled by (provider, addr, token); TTS and Bot clients are cheap
// per-call constructions.
package registry

import (
	"context"
	"fmt"
	"sync"
	"time"

	"callbot-master/internal/asr"
	"callbot-master/internal/bot"
	"callbot-master/internal/store"
	"callbot-master/internal/tts"
)

// Providers is the bundle SessionRunner needs to run one call.
type Providers struct {
	ASR asr.Client
	TTS tts.Client
	Bot bot.Client
}

// Registry holds the long-lived ASR pool. Safe for concurrent use.
type Registry struct {
	asrMu  sync.Mutex
	asrMap map[asrKey]*asr.ViettelClient
}

type asrKey struct {
	provider string
	addr     string
	token    string
}

func New() *Registry {
	return &Registry{asrMap: make(map[asrKey]*asr.ViettelClient)}
}

// Close releases every pooled ASR connection. Call once at shutdown.
func (r *Registry) Close() {
	r.asrMu.Lock()
	defer r.asrMu.Unlock()
	for k, c := range r.asrMap {
		_ = c.Close()
		delete(r.asrMap, k)
	}
}

// For builds a Providers bundle for the given bot. ASR is fetched from
// the pool (or dialed on first use); TTS and Bot are constructed fresh
// because their underlying transports are cheap and per-bot settings
// (voice, timeouts) need a per-instance struct anyway.
func (r *Registry) For(ctx context.Context, b *store.BotConfig) (*Providers, error) {
	if b == nil {
		return nil, fmt.Errorf("bot config is nil")
	}
	asrClient, err := r.asrFor(ctx, b)
	if err != nil {
		return nil, fmt.Errorf("asr: %w", err)
	}
	ttsClient, err := r.ttsFor(b)
	if err != nil {
		return nil, fmt.Errorf("tts: %w", err)
	}
	botClient, err := r.botFor(b)
	if err != nil {
		return nil, fmt.Errorf("bot: %w", err)
	}
	return &Providers{ASR: asrClient, TTS: ttsClient, Bot: botClient}, nil
}

func (r *Registry) asrFor(ctx context.Context, b *store.BotConfig) (asr.Client, error) {
	switch b.ASRProvider {
	case "viettel", "":
	default:
		return nil, fmt.Errorf("asr provider %q not supported yet", b.ASRProvider)
	}
	k := asrKey{provider: "viettel", addr: b.ASREndpoint, token: b.ASRToken}

	r.asrMu.Lock()
	defer r.asrMu.Unlock()
	if c, ok := r.asrMap[k]; ok {
		return c, nil
	}
	c, err := asr.NewViettelClient(ctx, b.ASREndpoint, b.ASRToken)
	if err != nil {
		return nil, err
	}
	r.asrMap[k] = c
	return c, nil
}

func (r *Registry) ttsFor(b *store.BotConfig) (tts.Client, error) {
	switch b.TTSProvider {
	case "viettel", "":
	default:
		return nil, fmt.Errorf("tts provider %q not supported yet", b.TTSProvider)
	}
	// Viettel TTS dials per StartStream — cheap to construct fresh per call.
	// resampleRate=0 means no resample (Viettel returns 8kHz already).
	return tts.NewViettelClient(b.TTSEndpoint, b.TTSToken, b.TTSVoiceID, 0, b.TTSTempo), nil
}

func (r *Registry) botFor(b *store.BotConfig) (bot.Client, error) {
	first := time.Duration(b.BotFirstByteTimeoutMs) * time.Millisecond
	total := time.Duration(b.BotTotalTimeoutMs) * time.Millisecond
	return bot.NewHTTPClient(b.BotURL, first, total), nil
}

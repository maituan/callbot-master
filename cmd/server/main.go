// Command server is the callbot-master entrypoint. It wires together:
//   - HTTP /health, /metrics
//   - FreeSWITCH ESL connection (event + API channels)
//   - Inbound park-and-pickup handler (CHANNEL_PARK on the inbound DID)
//   - Provider clients (ASR gRPC, TTS WS, bot HTTP)
//   - Session manager + graceful shutdown
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"callbot-master/config"
	"callbot-master/internal/api"
	"callbot-master/internal/asr"
	"callbot-master/internal/bot"
	"callbot-master/internal/campaign"
	"callbot-master/internal/freeswitch"
	"callbot-master/internal/metrics"
	"callbot-master/internal/pipeline"
	"callbot-master/internal/session"
	"callbot-master/internal/store"
	"callbot-master/internal/telemetry"
	"callbot-master/internal/tts"
	"callbot-master/internal/vad"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		slog.Error("config load failed", "err", err)
		os.Exit(1)
	}

	logger := buildLogger(cfg.Log)
	slog.SetDefault(logger)

	slog.Info("callbot-master starting",
		"http_addr", cfg.Server.HTTPAddr,
		"bot_url", cfg.Bot.URL,
		"asr_endpoint", cfg.ASR.Endpoint,
		"tts_endpoint", cfg.TTS.Endpoint,
		"fs_host", cfg.FreeSWITCH.Host,
		"inbound_did", cfg.Inbound.DID,
	)

	rootCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Tracing: noop when endpoint is empty.
	tracerShutdown, err := telemetry.Init(rootCtx, telemetry.Config{
		Endpoint:    cfg.Telemetry.Endpoint,
		ServiceName: cfg.Telemetry.ServiceName,
		Insecure:    cfg.Telemetry.Insecure,
	})
	if err != nil {
		slog.Error("telemetry init failed", "err", err)
		os.Exit(1)
	}
	if cfg.Telemetry.Endpoint != "" {
		slog.Info("telemetry enabled",
			"endpoint", cfg.Telemetry.Endpoint,
			"service_name", cfg.Telemetry.ServiceName,
			"insecure", cfg.Telemetry.Insecure)
	}

	manager := session.NewManager()
	mc := metrics.New()

	// Postgres store: optional. When DSN is empty, persistence is skipped
	// and /api/v1/calls endpoints return 503.
	var pgStore *store.Postgres
	if cfg.Postgres.DSN != "" {
		pgStore, err = store.New(rootCtx, cfg.Postgres.DSN)
		if err != nil {
			slog.Error("postgres init failed", "err", err)
			os.Exit(1)
		}
		defer pgStore.Close()
		slog.Info("postgres connected", "dsn_masked", maskDSN(cfg.Postgres.DSN))
	}

	// Provider clients. ASR dial happens here; TTS+bot are lazy per-call.
	asrClient, err := asr.NewViettelClient(rootCtx, cfg.ASR.Endpoint, cfg.ASR.Token)
	if err != nil {
		slog.Error("asr client init failed", "err", err)
		os.Exit(1)
	}
	defer asrClient.Close()

	ttsClient := tts.NewViettelClient(
		cfg.TTS.Endpoint, cfg.TTS.Token, cfg.TTS.VoiceID,
		cfg.TTS.ResampleRate, cfg.TTS.Tempo,
	)
	botClient := bot.NewHTTPClient(cfg.Bot.URL, cfg.Bot.FirstByteTimeout, cfg.Bot.TotalTimeout)

	// FreeSWITCH ESL — maintainConnection() reconnects in the background.
	esl, err := freeswitch.NewEventSocket(&cfg.FreeSWITCH)
	if err != nil {
		slog.Error("esl init failed", "err", err)
		os.Exit(1)
	}
	defer esl.Close()

	// Shared session runner for both inbound (PARK pickup) and outbound
	// (originated, ANSWER pickup) call flows.
	pCfg := pipeline.Defaults()
	pCfg.SampleRate = cfg.Audio.SampleRate
	pCfg.VoiceID = cfg.TTS.VoiceID
	pCfg.Tempo = cfg.TTS.Tempo
	pCfg.ResampleRate = cfg.TTS.ResampleRate
	pCfg.ASRSilenceTimeout = cfg.ASR.SilenceTimeout
	pCfg.ASRSpeechTimeout = cfg.ASR.SpeechTimeout
	pCfg.ASRSpeechMax = cfg.ASR.SpeechMax

	runner := &pipeline.SessionRunner{
		ESL:          esl,
		Sessions:     manager,
		ASR:          asrClient,
		TTS:          ttsClient,
		Bot:          botClient,
		NewVAD:       func() vad.Detector { return vad.NewEnergy(vadConfigFromCfg(cfg.VAD)) },
		RecordingDir: cfg.Audio.RecordingsDir,
		TTSDir:       cfg.Audio.TTSDir,
		FrameBytes:   cfg.Audio.FrameSamples * 2, // S16LE → 2 bytes per sample
		PipelineCfg:  pCfg,
		BargeIn:       cfg.BargeIn.Enabled,
		BargeMinWords: cfg.BargeIn.MinWords,
		Metrics:       mc,
	}
	if pgStore != nil {
		runner.Store = pgStore
	}

	inbound := pipeline.NewInboundHandler(rootCtx, pipeline.InboundDeps{
		Runner:    runner,
		DID:       cfg.Inbound.DID,
		PickupSLA: cfg.Inbound.PickupTimeout,
		Scenario:  cfg.Inbound.Scenario,
	})
	inbound.Register()

	outbound := pipeline.NewOutboundHandler(rootCtx, pipeline.OutboundDeps{
		Runner: runner,
	})
	outbound.Register()

	campaigns := campaign.NewManager()
	campaigns.Metrics = mc

	// HTTP server (health/metrics + campaigns API).
	router := api.New(manager, mc)
	api.RegisterCampaigns(router.Mux(), api.CampaignDeps{
		Manager:         campaigns,
		BindFunc:        outbound.MakeCampaignOriginateFuncWithManager,
		DefaultScenario: cfg.Inbound.Scenario,
		DefaultCallerID: "callbot",
	})
	var callReader api.CallReader
	if pgStore != nil {
		callReader = pgStore
	}
	api.RegisterCalls(router.Mux(), api.CallsDeps{Store: callReader})
	srv := &http.Server{
		Addr:              cfg.Server.HTTPAddr,
		Handler:           router.Handler(),
		ReadHeaderTimeout: 5 * time.Second,
	}
	serverErr := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			serverErr <- err
		}
		close(serverErr)
	}()
	slog.Info("http server listening", "addr", cfg.Server.HTTPAddr)

	select {
	case <-rootCtx.Done():
		slog.Info("shutdown signal received")
	case err := <-serverErr:
		if err != nil {
			slog.Error("http server failed", "err", err)
			os.Exit(1)
		}
	}

	// Graceful shutdown:
	//   1. Cancel rootCtx → in-flight session goroutines unwind.
	//   2. Sessions.DrainAll waits for them.
	//   3. inbound.Wait blocks until session goroutines exit.
	//   4. HTTP shutdown.
	stop() // ensure rootCtx is canceled even if we got here via serverErr
	if !manager.DrainAll(30 * time.Second) {
		slog.Warn("drain timed out — forcing shutdown")
	}
	inbound.Wait()
	outbound.Wait()

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		slog.Error("http shutdown failed", "err", err)
	}
	if err := tracerShutdown(shutdownCtx); err != nil {
		slog.Warn("tracer shutdown failed", "err", err)
	}
	slog.Info("callbot-master stopped")
}

// maskDSN returns the DSN with the password obscured so it's safe to log.
// Handles both URL-style ("postgres://user:pass@host") and key=value forms.
func maskDSN(dsn string) string {
	if i := strings.Index(dsn, "://"); i >= 0 {
		rest := dsn[i+3:]
		if at := strings.LastIndex(rest, "@"); at >= 0 {
			creds := rest[:at]
			if colon := strings.Index(creds, ":"); colon >= 0 {
				return dsn[:i+3] + creds[:colon+1] + "***" + rest[at:]
			}
		}
	}
	return dsn
}

func vadConfigFromCfg(c config.VADConfig) vad.Config {
	d := vad.Default()
	if c.EnergyThreshold > 0 {
		d.EnergyThreshold = c.EnergyThreshold
	}
	if c.MinSpeechDur > 0 {
		d.MinSpeechDur = c.MinSpeechDur
	}
	if c.MinSilenceDur > 0 {
		d.MinSilenceDur = c.MinSilenceDur
	}
	return d
}

func buildLogger(c config.LogConfig) *slog.Logger {
	level := slog.LevelInfo
	switch c.Level {
	case "debug":
		level = slog.LevelDebug
	case "warn":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	opts := &slog.HandlerOptions{Level: level}
	if c.Format == "text" {
		return slog.New(slog.NewTextHandler(os.Stderr, opts))
	}
	return slog.New(slog.NewJSONHandler(os.Stderr, opts))
}

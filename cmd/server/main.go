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
	"callbot-master/internal/auth"
	"callbot-master/internal/bot"
	"callbot-master/internal/campaign"
	"callbot-master/internal/freeswitch"
	"callbot-master/internal/metrics"
	"callbot-master/internal/pipeline"
	"callbot-master/internal/filler"
	"callbot-master/internal/recording"
	"callbot-master/internal/registry"
	"callbot-master/internal/session"
	"callbot-master/internal/store"
	"callbot-master/internal/telemetry"
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

		// Bootstrap the platform admin from env every startup. Cheap; lets
		// operators rotate the password by changing env + restarting.
		if cfg.Auth.PlatformAdminPassword != "" {
			hash, err := auth.HashPassword(cfg.Auth.PlatformAdminPassword)
			if err != nil {
				slog.Error("hash admin password", "err", err)
				os.Exit(1)
			}
			if err := pgStore.UpsertPlatformAdmin(rootCtx, cfg.Auth.PlatformAdminUser, hash); err != nil {
				slog.Error("upsert platform_admin", "err", err)
				os.Exit(1)
			}
			slog.Info("platform_admin bootstrapped", "user", cfg.Auth.PlatformAdminUser)
		} else {
			slog.Warn("MASTER_PLATFORM_ADMIN_PASSWORD not set — login will fail until you set it")
		}
	}

	// JWT issuer. nil-tolerant: if no secret is configured, auth-protected
	// routes will reject every request with 401 and the UI shows the
	// login page indefinitely.
	var issuer *auth.Issuer
	if cfg.Auth.JWTSecret != "" {
		issuer, err = auth.NewIssuer(cfg.Auth.JWTSecret, cfg.Auth.JWTTTL)
		if err != nil {
			slog.Error("jwt issuer init", "err", err)
			os.Exit(1)
		}
	} else {
		slog.Warn("MASTER_JWT_SECRET not set — auth disabled, /api/v1/auth/* will 503")
	}

	// Provider registry: pools ASR gRPC connections by (endpoint, token);
	// TTS + Bot clients are constructed fresh per call. The previous
	// architecture wired one ASR/TTS/Bot singleton from env; this is what
	// made true multi-bot impossible. Now registry.For(*BotConfig) is the
	// only place providers come from.
	provReg := registry.New()
	defer provReg.Close()

	// Bootstrap a default bot from env when the DB is empty. Lets a fresh
	// install boot with the same env it used pre-multi-tenant; once an
	// admin creates real bots in the UI, this branch is a no-op.
	if pgStore != nil {
		if n, err := pgStore.CountBots(rootCtx); err != nil {
			slog.Error("count bots", "err", err)
			os.Exit(1)
		} else if n == 0 {
			botID, err := pgStore.SeedDefaultBot(rootCtx, store.SeedBotInput{
				TenantSlug:            "hcc",
				TenantName:            "Hành chính công",
				BotSlug:               "hcc-default",
				BotName:               "HCC default bot",
				DID:                   cfg.Inbound.DID,
				BotURL:                cfg.Bot.URL,
				BotFirstByteTimeoutMs: int(cfg.Bot.FirstByteTimeout / time.Millisecond),
				BotTotalTimeoutMs:     int(cfg.Bot.TotalTimeout / time.Millisecond),
				ASREndpoint:           cfg.ASR.Endpoint,
				ASRToken:              cfg.ASR.Token,
				TTSEndpoint:           cfg.TTS.Endpoint,
				TTSToken:              cfg.TTS.Token,
				TTSVoiceID:            cfg.TTS.VoiceID,
				TTSTempo:              cfg.TTS.Tempo,
				ASRSilenceTimeoutSec:  int(cfg.ASR.SilenceTimeout / time.Second),
				ASRSpeechTimeoutSec:   int(cfg.ASR.SpeechTimeout / time.Second),
				ASRSpeechMaxSec:       int(cfg.ASR.SpeechMax / time.Second),
				BargeInEnabled:        cfg.BargeIn.Enabled,
				BargeInMinWords:       cfg.BargeIn.MinWords,
				FillerEnabled:         cfg.Filler.Enabled,
			})
			if err != nil {
				slog.Error("seed default bot", "err", err)
				os.Exit(1)
			}
			slog.Info("seeded default bot from env",
				"bot_id", botID, "tenant", "hcc", "did", cfg.Inbound.DID)
		}
	}

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

	// Filler pool: discover {Filler.Dir}/{voice_id}/*.wav lazily when each
	// call resolves a voice. Enabled per-bot via the FillerEnabled flag;
	// dir-empty/missing degrades to silent (no filler) without erroring.
	var fillerPool *filler.Pool
	if cfg.Filler.Dir != "" {
		fillerPool = filler.NewPool(cfg.Filler.Dir)
		slog.Info("filler pool ready", "base_dir", cfg.Filler.Dir)
	}

	runner := &pipeline.SessionRunner{
		ESL:          esl,
		Sessions:     manager,
		NewVAD:       func() vad.Detector { return vad.NewEnergy(vadConfigFromCfg(cfg.VAD)) },
		RecordingDir: cfg.Audio.RecordingsDir,
		TTSDir:       cfg.Audio.TTSDir,
		FrameBytes:   cfg.Audio.FrameSamples * 2, // S16LE → 2 bytes per sample
		PipelineCfg:  pCfg,
		Metrics:      mc,
		Filler:       fillerPool,
	}
	if pgStore != nil {
		runner.Store = pgStore
	}

	// Optional recording archiver — copies FS-side MP3 to a persistent
	// dir post-hangup and writes the resulting URL into call_history.
	// Disabled if SourceDir or ArchiveDir is empty.
	var archiver *recording.Archiver
	srcDirs := cfg.Recording.AllSourceDirs()
	if len(srcDirs) > 0 && cfg.Recording.ArchiveDir != "" {
		var persister recording.URLPersister
		if pgStore != nil {
			persister = pgStore
		}
		archiver = recording.NewMulti(
			srcDirs,
			cfg.Recording.ArchiveDir,
			cfg.Recording.URLPrefix,
			cfg.Recording.FileExt,
			persister,
		)
		runner.Archiver = archiver
		slog.Info("recording archiver wired",
			"sources", srcDirs,
			"archive", cfg.Recording.ArchiveDir,
			"url_prefix", cfg.Recording.URLPrefix)
	}

	// Wire PLAYBACK_STOP listener — Speak uses it to know when FS finished
	// playing each utterance.
	runner.RegisterESLHandlers()

	if pgStore == nil {
		slog.Error("pgStore is required for multi-tenant routing — set MASTER_PG_DSN")
		os.Exit(1)
	}
	inbound := pipeline.NewInboundHandler(rootCtx, pipeline.InboundDeps{
		Runner:    runner,
		Resolver:  pgStore,
		Registry:  provReg,
		PickupSLA: cfg.Inbound.PickupTimeout,
	})
	inbound.Register()

	outbound := pipeline.NewOutboundHandler(rootCtx, pipeline.OutboundDeps{
		Runner:   runner,
		Registry: provReg,
	})
	outbound.Register()

	campaigns := campaign.NewManager()
	campaigns.Metrics = mc

	// HTTP server (health/metrics + campaigns API).
	router := api.New(manager, mc)
	router.SetCORS(cfg.Server.CORSAllowOrigin)
	router.SetAuth(issuer)
	if issuer != nil && pgStore != nil {
		api.RegisterAuth(router.Mux(), api.AuthDeps{
			Issuer:       issuer,
			Store:        pgStore,
			CookieSecure: cfg.Auth.CookieSecure,
		})
	} else {
		api.RegisterAuth(router.Mux(), api.AuthDeps{}) // mounts 503 stubs
	}
	api.RegisterCampaigns(router.Mux(), api.CampaignDeps{
		Manager:           campaigns,
		BindFunc:          outbound.MakeCampaignOriginateFuncWithManager,
		BotLookup:         pgStore,
		DefaultTenantSlug: "hcc",
		DefaultBotSlug:    "hcc-default",
		DefaultCallerID:   "callbot",
	})
	var callReader api.CallReader
	if pgStore != nil {
		callReader = pgStore
	}
	api.RegisterCalls(router.Mux(), api.CallsDeps{Store: callReader})

	if pgStore != nil {
		api.RegisterBots(router.Mux(), api.BotsDeps{Store: pgStore, Auditor: pgStore})
		api.RegisterTenants(router.Mux(), api.TenantsDeps{Store: pgStore, Auditor: pgStore})
		api.RegisterUsers(router.Mux(), api.UsersDeps{Store: pgStore, Auditor: pgStore})
		api.RegisterAudit(router.Mux(), api.AuditDeps{Store: pgStore})
		api.RegisterOriginate(router.Mux(), api.OriginateDeps{
			Outbound:          outbound,
			BotLookup:         pgStore,
			DefaultCallerID:   "callbot",
			DefaultTenantSlug: "hcc",
		})
		if issuer != nil {
			api.RegisterShare(router.Mux(), api.ShareDeps{
				Issuer: issuer,
				Store:  pgStore,
				// TTL=0 → 7 days default. Override per-mint via
				// {"ttl_hours": N} body param (capped at 30 days).
			})
			api.RegisterWeb(router.Mux(), api.WebDeps{
				Issuer: issuer,
				Store:  pgStore,
				ChatTTL: cfg.Web.ShareTTL,
				BotFactory: func(b *store.BotConfig) (bot.Client, error) {
					first := b.BotFirstByteTimeout()
					total := b.BotTotalTimeout()
					return bot.NewHTTPClient(b.BotURL, first, total), nil
				},
				VoiceASREndpoint:     cfg.Web.VoiceASREndpoint,
				VoiceASRSampleRate:   cfg.Web.VoiceASRSampleRate,
				VoiceTTSResampleRate: cfg.Web.VoiceTTSResampleRate,
				VoiceRecordingDir:    cfg.Web.VoiceRecordingDir,
			})
		}
	} else {
		api.RegisterBots(router.Mux(), api.BotsDeps{}) // 503 stubs
	}

	// Serve archived recordings under URLPrefix from ArchiveDir.
	if archiver != nil && cfg.Recording.URLPrefix != "" {
		router.MountStaticDir(cfg.Recording.URLPrefix, cfg.Recording.ArchiveDir)
	}
	// Serve web playground recordings (TTS WAV per turn) so QC can
	// listen back. Auth-gated via the JWT middleware (auth-required;
	// not in the bypass list above).
	if cfg.Web.VoiceRecordingDir != "" {
		router.MountStaticDir("/web-recordings", cfg.Web.VoiceRecordingDir)
	}
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

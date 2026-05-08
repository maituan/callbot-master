package config

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"time"

	"gopkg.in/yaml.v3"
)

type Config struct {
	Server      ServerConfig      `yaml:"server"`
	FreeSWITCH  FreeSWITCHConfig  `yaml:"freeswitch"`
	ASR         ASRConfig         `yaml:"asr"`
	TTS         TTSConfig         `yaml:"tts"`
	Bot         BotConfig         `yaml:"bot"`
	VAD         VADConfig         `yaml:"vad"`
	Audio       AudioConfig       `yaml:"audio"`
	Inbound     InboundConfig     `yaml:"inbound"`
	BargeIn     BargeInConfig     `yaml:"barge_in"`
	Filler      FillerConfig      `yaml:"filler"`
	Postgres    PostgresConfig    `yaml:"postgres"`
	Recording   RecordingConfig   `yaml:"recording"`
	Telemetry   TelemetryConfig   `yaml:"telemetry"`
	Log         LogConfig         `yaml:"log"`
	Auth        AuthConfig        `yaml:"auth"`
}

// AuthConfig holds JWT + bootstrap admin settings. The platform admin
// is upserted on every startup from these values, so rotating the
// password is just an env var change + restart.
type AuthConfig struct {
	JWTSecret             string        `yaml:"jwt_secret"`              // ≥32 chars
	JWTTTL                time.Duration `yaml:"jwt_ttl"`                 // default 12h
	PlatformAdminUser     string        `yaml:"platform_admin_user"`     // bootstrap admin username
	PlatformAdminPassword string        `yaml:"platform_admin_password"` // plaintext; hashed before insert
	CookieSecure          bool          `yaml:"cookie_secure"`           // set true behind HTTPS
}

// RecordingConfig wires the FS-side stereo recording (MP3 written by the
// dialplan into SourceDir) to a persistent archive served over HTTP.
//
// Flow per call:
//
//   1. SIP team's dialplan writes <SourceDir>/<call_id>.mp3 (or .wav)
//   2. After hangup, master copies it to <ArchiveDir>/YYYY/MM/DD/<id>.mp3
//   3. call_history.recording_url is set to URLPrefix + relative path
//   4. The same archive path is mounted under URLPrefix by the HTTP server
//      so an operator UI can play the file directly.
//
// Disable archiving by leaving SourceDir or ArchiveDir empty.
type RecordingConfig struct {
	SourceDir   string `yaml:"source_dir"`   // FS recordings dir, e.g. /var/lib/freeswitch/recordings/voiceai-hotline
	ArchiveDir  string `yaml:"archive_dir"`  // Persistent archive, e.g. /var/lib/callbot/recordings
	URLPrefix   string `yaml:"url_prefix"`   // HTTP prefix served by master, e.g. /recordings
	FileExt     string `yaml:"file_ext"`     // ".mp3" (default) or ".wav"
}

type ServerConfig struct {
	HTTPAddr string `yaml:"http_addr"` // e.g. ":8083"
	// CORSAllowOrigin sets Access-Control-Allow-Origin on /api/* and
	// /recordings/* responses so the ops UI can call master from a different
	// origin (e.g. http://localhost:3001 in dev). Common values:
	//   ""  (default) — disabled, no header set.
	//   "*" — allow any origin (no credentials).
	//   "http://localhost:3001" — exact origin.
	// In prod, prefer same-origin via reverse proxy and leave this empty.
	CORSAllowOrigin string `yaml:"cors_allow_origin"`
}

type FreeSWITCHConfig struct {
	Host     string `yaml:"host"`     // "127.0.0.1:8021"
	Password string `yaml:"password"` // "ClueCon"
	Domain   string `yaml:"domain"`   // "voiceai.autotele.vn"
}

// ASRConfig knobs map to Viettel STT metadata fields. The two timeouts are
// independent and do *different* things:
//
//   - SpeechTimeout: while caller is mid-utterance, this is the trailing
//     silence that flushes a *non-empty* IsFinal transcript ("they
//     stopped talking, here's what they said").
//
//   - SilenceTimeout: caller hasn't said anything yet; after this duration
//     the server flushes an *empty* IsFinal ("nothing to transcribe,
//     move on"). Pipeline.RunTurn turns the empty result into a
//     "skip bot, listen again" branch.
//
//   - SpeechMax: hard cap on a single utterance even if the caller keeps
//     talking. Defensive against runaway streams.
type ASRConfig struct {
	Endpoint       string        `yaml:"endpoint"` // "103.253.20.28:9000"
	Token          string        `yaml:"token"`
	SilenceTimeout time.Duration `yaml:"silence_timeout"`
	SpeechTimeout  time.Duration `yaml:"speech_timeout"`
	SpeechMax      time.Duration `yaml:"speech_max"`
	SingleSentence bool          `yaml:"single_sentence"`
}

type TTSConfig struct {
	Endpoint     string  `yaml:"endpoint"` // "ws://103.253.20.27:8767"
	Token        string  `yaml:"token"`
	VoiceID      string  `yaml:"voice_id"`
	ResampleRate int     `yaml:"resample_rate"` // 8000
	Tempo        float64 `yaml:"tempo"`         // 1.0
}

type BotConfig struct {
	URL              string        `yaml:"url"` // "http://localhost:11006/api/v1/call/"
	FirstByteTimeout time.Duration `yaml:"first_byte_timeout"`
	TotalTimeout     time.Duration `yaml:"total_timeout"`
}

type VADConfig struct {
	EnergyThreshold float64       `yaml:"energy_threshold"`
	MinSpeechDur    time.Duration `yaml:"min_speech_duration"`
	MinSilenceDur   time.Duration `yaml:"min_silence_duration"`
}

type AudioConfig struct {
	SampleRate    int    `yaml:"sample_rate"`     // 8000
	FrameSamples  int    `yaml:"frame_samples"`   // 320 (20ms @ 8kHz)
	RecordingsDir string `yaml:"recordings_dir"`  // /dev/shm/bridge/recordings
	TTSDir        string `yaml:"tts_dir"`         // /dev/shm/bridge/tts
}

// InboundConfig governs the park-and-pickup handoff with the SIP team.
//
// External team's dialplan parks the channel on a virtual DID; we filter
// CHANNEL_PARK events by the destination number and claim only ours.
type InboundConfig struct {
	// DID = virtual number used by the SIP team to mark inbound calls
	// destined for the AI bot. Default 5000000024 (HCC scenario).
	DID string `yaml:"did"`
	// PickupTimeout is the SLA window between PARK and master starting the
	// pipeline. The SIP team releases the call past this. Default 30s.
	PickupTimeout time.Duration `yaml:"pickup_timeout"`
	// Scenario defaults for inbound calls (overridable per-call later).
	Scenario string `yaml:"scenario"`
}

type BargeInConfig struct {
	Enabled bool `yaml:"enabled"`
	// MinWords is the running-transcript word count that trips barge-in.
	// 3 is a good default — long enough to ignore "vâng"/"ờ" backchannel,
	// short enough to feel responsive.
	MinWords int `yaml:"min_words"`
}

type FillerConfig struct {
	Enabled bool   `yaml:"enabled"`
	// Dir is the base directory where filler audio files live, organised
	// per voice: `{Dir}/{voice_id}/*.wav` (or .mp3). Empty disables.
	Dir string `yaml:"dir"`
}

type PostgresConfig struct {
	DSN string `yaml:"dsn"` // e.g. postgres://user:pass@localhost:5432/callbot
}

type LogConfig struct {
	Level  string `yaml:"level"`  // debug|info|warn|error
	Format string `yaml:"format"` // json|text
}

// TelemetryConfig governs OpenTelemetry tracing. Disabled when Endpoint is empty.
//
// Tracing is per-call: one root span "call.<direction>" with child spans for
// each turn (greeting + N) and within each turn a span per provider call
// (asr.listen, bot.turn, tts.turn) so latency at every layer is visible.
//
// Standard OTEL env vars (OTEL_EXPORTER_OTLP_ENDPOINT, OTEL_SERVICE_NAME,
// OTEL_RESOURCE_ATTRIBUTES, OTEL_TRACES_SAMPLER) are honored too — set them
// to override yaml values.
type TelemetryConfig struct {
	Endpoint    string `yaml:"endpoint"`     // OTLP gRPC, e.g. "tempo:4317"
	ServiceName string `yaml:"service_name"` // resource attr; default "callbot-master"
	Insecure    bool   `yaml:"insecure"`     // skip TLS for local dev
}

// Load reads config.yaml from the standard location and applies env overrides.
// Search order:
//  1. $MASTER_CONFIG (if set)
//  2. /app/config/config.yaml (Docker)
//  3. <pkg-dir>/config.yaml (local dev, via runtime.Caller)
func Load() (*Config, error) {
	c := defaults()

	path := resolvePath()
	if path != "" {
		f, err := os.Open(path)
		if err == nil {
			defer f.Close()
			if err := yaml.NewDecoder(f).Decode(c); err != nil {
				return nil, fmt.Errorf("decode %s: %w", path, err)
			}
		} else if !os.IsNotExist(err) {
			return nil, fmt.Errorf("open %s: %w", path, err)
		}
	}

	applyEnvOverrides(c)
	return c, nil
}

func resolvePath() string {
	if p := os.Getenv("MASTER_CONFIG"); p != "" {
		return p
	}
	if _, err := os.Stat("/app/config/config.yaml"); err == nil {
		return "/app/config/config.yaml"
	}
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		return ""
	}
	return filepath.Join(filepath.Dir(file), "config.yaml")
}

func defaults() *Config {
	return &Config{
		Server: ServerConfig{HTTPAddr: ":8083"},
		FreeSWITCH: FreeSWITCHConfig{
			Host:     "127.0.0.1:8021",
			Password: "ClueCon",
			Domain:   "voiceai.autotele.vn",
		},
		ASR: ASRConfig{
			Endpoint: "103.253.20.28:9000",
			// Viettel STT expects integer-second values for these timeouts —
			// sub-second config gets rounded up at send time. Defaults tuned
			// for natural Vietnamese phone calls.
			SilenceTimeout: 5 * time.Second, // caller-never-spoke cap
			SpeechTimeout:  1 * time.Second, // mid-utterance trailing silence cut
			SpeechMax:      30 * time.Second,
			SingleSentence: true,
		},
		TTS: TTSConfig{
			Endpoint:     "ws://103.253.20.27:8767",
			ResampleRate: 8000,
			Tempo:        1.0,
		},
		Bot: BotConfig{
			URL:              "http://localhost:11006/api/v1/call/",
			FirstByteTimeout: 5 * time.Second,
			TotalTimeout:     25 * time.Second,
		},
		VAD: VADConfig{
			EnergyThreshold: 500.0,
			MinSpeechDur:    200 * time.Millisecond,
			MinSilenceDur:   600 * time.Millisecond,
		},
		Audio: AudioConfig{
			SampleRate:    8000,
			FrameSamples:  320,
			RecordingsDir: "/dev/shm/bridge/recordings",
			TTSDir:        "/dev/shm/bridge/tts",
		},
		Inbound: InboundConfig{
			DID:           "5000000024",
			PickupTimeout: 30 * time.Second,
			Scenario:      "hcc-inbound",
		},
		BargeIn:   BargeInConfig{Enabled: false, MinWords: 3},
		Filler:    FillerConfig{Enabled: false, Dir: "/var/lib/callbot/fillers"},
		Recording: RecordingConfig{
			SourceDir:  "/var/lib/freeswitch/recordings/voiceai-hotline",
			ArchiveDir: "/var/lib/callbot/recordings",
			URLPrefix:  "/recordings",
			FileExt:    ".mp3",
		},
		Telemetry: TelemetryConfig{ServiceName: "callbot-master", Insecure: true},
		Log:       LogConfig{Level: "info", Format: "json"},
		Auth: AuthConfig{
			JWTTTL:            12 * time.Hour,
			PlatformAdminUser: "admin",
			// JWTSecret + PlatformAdminPassword have NO default — operator must set them.
			// Master refuses to start with auth enabled if either is empty.
		},
	}
}

func applyEnvOverrides(c *Config) {
	envStr("MASTER_HTTP_ADDR", &c.Server.HTTPAddr)
	envStr("MASTER_CORS_ALLOW_ORIGIN", &c.Server.CORSAllowOrigin)
	envStr("MASTER_FS_HOST", &c.FreeSWITCH.Host)
	envStr("MASTER_FS_PASSWORD", &c.FreeSWITCH.Password)
	envStr("MASTER_FS_DOMAIN", &c.FreeSWITCH.Domain)
	envStr("MASTER_ASR_ENDPOINT", &c.ASR.Endpoint)
	envStr("MASTER_ASR_TOKEN", &c.ASR.Token)
	envDur("MASTER_ASR_SILENCE_TIMEOUT", &c.ASR.SilenceTimeout)
	envDur("MASTER_ASR_SPEECH_TIMEOUT", &c.ASR.SpeechTimeout)
	envDur("MASTER_ASR_SPEECH_MAX", &c.ASR.SpeechMax)
	envStr("MASTER_TTS_ENDPOINT", &c.TTS.Endpoint)
	envStr("MASTER_TTS_TOKEN", &c.TTS.Token)
	envStr("MASTER_TTS_VOICE_ID", &c.TTS.VoiceID)
	envStr("MASTER_BOT_URL", &c.Bot.URL)
	envDur("MASTER_BOT_FIRST_BYTE_TIMEOUT", &c.Bot.FirstByteTimeout)
	envDur("MASTER_BOT_TOTAL_TIMEOUT", &c.Bot.TotalTimeout)
	envStr("MASTER_PG_DSN", &c.Postgres.DSN)
	envStr("MASTER_LOG_LEVEL", &c.Log.Level)
	envStr("MASTER_LOG_FORMAT", &c.Log.Format)
	envStr("MASTER_INBOUND_DID", &c.Inbound.DID)
	envStr("MASTER_INBOUND_SCENARIO", &c.Inbound.Scenario)
	envBool("MASTER_BARGE_IN", &c.BargeIn.Enabled)
	envInt("MASTER_BARGE_MIN_WORDS", &c.BargeIn.MinWords)
	envBool("MASTER_FILLER", &c.Filler.Enabled)
	envStr("MASTER_FILLER_DIR", &c.Filler.Dir)
	// OTEL standard env vars take precedence; fallback to MASTER_OTEL_*.
	if v := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"); v != "" {
		c.Telemetry.Endpoint = v
	} else {
		envStr("MASTER_OTEL_ENDPOINT", &c.Telemetry.Endpoint)
	}
	if v := os.Getenv("OTEL_SERVICE_NAME"); v != "" {
		c.Telemetry.ServiceName = v
	} else {
		envStr("MASTER_OTEL_SERVICE_NAME", &c.Telemetry.ServiceName)
	}
	envBool("MASTER_OTEL_INSECURE", &c.Telemetry.Insecure)
	envStr("MASTER_RECORDING_SOURCE_DIR", &c.Recording.SourceDir)
	envStr("MASTER_RECORDING_ARCHIVE_DIR", &c.Recording.ArchiveDir)
	envStr("MASTER_RECORDING_URL_PREFIX", &c.Recording.URLPrefix)
	envStr("MASTER_RECORDING_FILE_EXT", &c.Recording.FileExt)
	envStr("MASTER_JWT_SECRET", &c.Auth.JWTSecret)
	envDur("MASTER_JWT_TTL", &c.Auth.JWTTTL)
	envStr("MASTER_PLATFORM_ADMIN_USER", &c.Auth.PlatformAdminUser)
	envStr("MASTER_PLATFORM_ADMIN_PASSWORD", &c.Auth.PlatformAdminPassword)
	envBool("MASTER_COOKIE_SECURE", &c.Auth.CookieSecure)
}

func envStr(key string, dst *string) {
	if v := os.Getenv(key); v != "" {
		*dst = v
	}
}

func envDur(key string, dst *time.Duration) {
	v := os.Getenv(key)
	if v == "" {
		return
	}
	if d, err := time.ParseDuration(v); err == nil {
		*dst = d
	}
}

func envInt(key string, dst *int) {
	v := os.Getenv(key)
	if v == "" {
		return
	}
	if n, err := strconv.Atoi(v); err == nil {
		*dst = n
	}
}

func envBool(key string, dst *bool) {
	v := os.Getenv(key)
	if v == "" {
		return
	}
	if b, err := strconv.ParseBool(v); err == nil {
		*dst = b
	}
}

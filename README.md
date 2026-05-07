# callbot-master

Vietnamese callbot orchestrator for FreeSWITCH-driven SIP traffic. Bridges
caller audio to ASR, streams an LLM-backed bot response, and synthesizes
voice via TTS — all wired through one Go binary.

```
Caller ⇄ FreeSWITCH ⇄ callbot-master (Go) ⇄ {ASR gRPC, TTS WS, Bot REST stream}
```

## Layout

```
callbot-master/
├── cmd/
│   ├── server/        # main daemon (HTTP /health /metrics, ESL, pipeline)
│   ├── bot-cli/       # smoke test for bot streaming endpoint
│   ├── tts-cli/       # smoke test for TTS WS → PCM file
│   ├── offline-cli/   # full pipeline against a WAV/PCM file (no FS)
│   └── loadtest/      # N-concurrent offline pipeline driver
├── config/            # default YAML + env overrides
├── deploy/            # Dockerfile, docker-compose.yml, prom + grafana
├── internal/
│   ├── api/           # HTTP routes (health, metrics, campaigns, calls)
│   ├── asr/           # streaming ASR contract + Viettel gRPC impl
│   ├── audio/         # FIFO source/sink + ring buffer
│   ├── bot/           # bot REST stream + sentence parser
│   ├── campaign/      # CSV → leads → outbound dial pool
│   ├── freeswitch/    # ESL event socket + commands
│   ├── metrics/       # Prometheus collectors
│   ├── pipeline/      # Listen + Speak + barge-in + per-call orchestration
│   ├── session/       # FSM + manager
│   ├── store/         # PostgreSQL call_history persistence
│   ├── telemetry/     # OpenTelemetry tracer init
│   ├── tts/           # streaming TTS contract + Viettel WS impl
│   └── vad/           # energy-based detector
└── proto/             # streaming_voice.proto + generated pb.go
```

## Quick start (dev)

```sh
# 1. start the bot brain (separate repo)
cd ../callbot-hcc-base-ts && bun run dev   # :11006

# 2. run master
cd callbot-master
go run ./cmd/server                         # :8083
curl http://localhost:8083/health
```

## Smoke tools

| Tool | What it tests |
|------|---------------|
| `go run ./cmd/bot-cli "câu hỏi của tôi"` | bot HTTP stream + sentence parser |
| `go run ./cmd/tts-cli -api-key … -voice thuyanh-north "Xin chào"` | Viettel TTS → PCM file |
| `go run ./cmd/offline-cli -in sample.wav -out resp.pcm …` | full ASR→bot→TTS without FS |
| `go run ./cmd/loadtest -ccu 50 -turns 2 -in sample.wav` | concurrent pipelines, p50/p95/p99 |

## Configuration

Defaults live in [`config/config.yaml`](config/config.yaml). Every field is
overridable via env vars prefixed `MASTER_*`; standard `OTEL_*` env vars
are honored too. See [`config/config.go`](config/config.go) for the full
list.

Critical env vars:

| Var | Purpose |
|-----|---------|
| `MASTER_FS_HOST` | FreeSWITCH ESL `host:port` |
| `MASTER_FS_PASSWORD` | ESL password |
| `MASTER_INBOUND_DID` | virtual number marking inbound calls (default `5000000024`) |
| `MASTER_BARGE_IN` | enable mid-bot caller interruption (default `false`) |
| `MASTER_PG_DSN` | Postgres DSN; empty disables persistence |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | enables OTLP gRPC tracing |

## Telemetry

- **Logs** — JSON via `log/slog`; every line carries `call_uuid`.
- **Metrics** — Prometheus on `/metrics` (`master_active_calls`,
  `master_turn_duration_seconds`, `master_stage_latency_seconds`,
  `master_barge_in_total`, `master_campaign_progress`, …).
- **Traces** — OpenTelemetry per call, span tree:
  `call.<dir> → pipeline.{greeting,turn} → {asr.listen, bot.turn, tts.turn}`
  with events for `first_partial`, `first_sentence`, `first_audio`,
  `barge_in`, `final_transcript`.

## Ops API

| Route | Method |
|-------|--------|
| `/health` | GET — liveness + active calls |
| `/metrics` | GET — Prometheus scrape |
| `/api/v1/campaigns` | POST (multipart CSV) / GET (list) |
| `/api/v1/campaigns/{id}` | GET |
| `/api/v1/campaigns/{id}/cancel` | POST |
| `/api/v1/calls` | GET — filters: phone, scenario, direction, since, until, limit, offset |
| `/api/v1/calls/{id}` | GET — full record + per-turn JSONB history |

## Build

```sh
go build ./...
go test ./...
go vet ./...
```

Container:

```sh
docker build -f deploy/Dockerfile -t callbot-master:dev .
docker compose -f deploy/docker-compose.yml up
```

## Architecture notes

See [`CLAUDE.md`](../CLAUDE.md) (parent dir) for the full design rationale
and roadmap, including the inbound park-and-pickup contract with the SIP
team and the bot streaming wire format.

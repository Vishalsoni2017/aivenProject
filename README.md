# Project Avien — Log Consumer & Incident Automation

Short: This project tails PM2 logs, publishes them to Kafka, consumes logs, detects incidents, optionally asks OpenAI for a concise suggestion, executes actions (pm2 restart) and stores incidents in PostgreSQL. Redis (Valkey) is used for short-term deduplication.

## Architecture (quick)
PM2 app logs (app2.js) -> Kafka `logs` topic -> Python consumer (consumercopy.py)
Consumer:
- detect_issue() → decide_action()
- is_duplicate() (Redis) → avoid repeats
- call_gemini_for_solution() (OpenAI) → short JSON suggestion
- execute_action() (pm2 restart)
- save_incident() → PostgreSQL (incidents, llm_responses, incident_events)

## Repo layout
- node-app/
  - app2.js — PM2 tailer + Kafka producer
  - package.json, Dockerfile
- consumercopy.py — Python Kafka consumer, detector, OpenAI integration, Postgres writer
- requirements.txt — Python dependencies
- Dockerfile — consumer image (python)
- docker-compose.yml — sample compose
- README.md — this file

## Quick prerequisites
- Node.js >= 18 (for producer)
- Python 3.11 (consumer)
- Kafka cluster (Aiven or self-hosted)
- PostgreSQL
- Redis (used for dedupe)
- OpenAI API key (optional but recommended)
- PM2 (on app hosts) for restarts
- CA/client certs for Kafka (ca.pem, service.cert, service.key)

## Environment variables (example)
Place in a `.env` or export in shell:

OPENAI_API_KEY=sk_...
OPENAI_MODEL=gpt-4o-mini
PGHOST=your_pg_host
PGPORT=5432
PGDATABASE=defaultdb
PGUSER=dbuser
PGPASSWORD=secret
REDIS_HOST=redis.example.com
REDIS_PORT=6379
KAFKA_BROKERS=kafka:22770
PM2_LOG_DIR=/root/.pm2/logs

Windows (PowerShell) example:
```powershell
setx OPENAI_API_KEY "sk_..."
setx OPENAI_MODEL "gpt-4o-mini"
setx PGHOST "your_pg_host"
setx PGUSER "dbuser"
setx PGPASSWORD "secret"

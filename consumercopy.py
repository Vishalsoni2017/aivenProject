from confluent_kafka import Consumer
import json
import subprocess
import psycopg2
import time
import redis
import signal
import sys
import traceback
import os

# Optional: requests is used to call the Gemini API
# Install with: pip install requests
try:
    import requests
except Exception:
    requests = None
    print("⚠️ requests not available. Install with: pip install requests to enable Gemini integration")

try:
    from openai import OpenAI
except Exception:
    OpenAI = None

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")
OPENAI_BASE_URL = os.getenv("OPENAI_BASE_URL")  # optional custom base URL
OPENAI_MODEL = os.getenv("OPENAI_MODEL", "gpt-oss-20b")  # override with env if needed

valkey_client = redis.Redis(
    host="HOST_GOES_HERE",
    port="PORT_GOES_HERE",
    username="default",
    password="PASSWORD_GOES_HERE",
    ssl=True
)

print("✅ Valkey connected")

def is_duplicate(issue, service="unknown", ttl=60):
    key = f"issue:{service}:{issue}"
    print(f"🔍 is_duplicate check for key={key}")
    try:
        if valkey_client.exists(key):
            print("⏱ duplicate detected")
            return True
        # store key with TTL
        valkey_client.set(key, "1", ex=ttl)
        print("✅ Stored dedupe key with TTL:", ttl)
        return False
    except Exception as e:
        print("⚠️ Redis check failed, allowing processing:", e)
        return False


# =========================
# 🔌 PostgreSQL Connection
# =========================
def connect_db():
    print("🔌 connect_db: connecting to Postgres...")
    try:
        conn = psycopg2.connect(
            host="HOST_GOES_HERE",
            port="PORT_GOES_HERE",
            database="defaultdb",
            user="USERNAME_GOES_HERE",
            password="PASSWORD_GOES_HERE",
            sslmode="require"
        )
        print("✅ PostgreSQL connected successfully")
        return conn
    except Exception as e:
        print("❌ PostgreSQL connection failed:", e)
        return None


# =========================
# 🧪 Test DB Connection
# =========================
def test_db(conn):
    print("🧪 test_db: running test query")
    try:
        cur = conn.cursor()
        cur.execute("SELECT 1;")
        cur.close()
        print("✅ PostgreSQL test query successful")
    except Exception as e:
        print("❌ PostgreSQL test failed:", e)


# =========================
# 💡 Gemini integration
# =========================
def call_gemini_for_solution(service, issue, details, raw_payload):
    """
    Try OpenAI SDK (if available). If SDK missing, API key missing, or call fails,
    return a concise local suggestion dict: {"reason", "suggested_solution"} so DB
    always receives a non-null solution.
    """
    def local_suggestion():
        reason = f"{issue} detected"
        if issue == "high_cpu":
            suggested = f"High CPU on {service}. Inspect with pm2 monit / pm2 list and restart: pm2 restart {service}."
            reason = f"High CPU usage ({details.get('reason','')})."
        elif issue == "app_crash":
            suggested = f"App crashed. Check logs: pm2 logs {service} --lines 200; restart: pm2 restart {service}."
            reason = "Application crash detected."
        elif issue == "db_connect_error":
            suggested = "DB connection issue. Verify host/port/credentials and network; restart app after DB recovery: pm2 restart {svc}."
            suggested = suggested.format(svc=service)
            reason = "Database connection failure."
        elif issue == "app_error":
            suggested = f"Inspect error stack in logs: pm2 logs {service} --lines 500; restart if needed: pm2 restart {service}."
            reason = "Application error/exception detected."
        else:
            suggested = f"Inspect logs for {service} and restart if needed: pm2 restart {service}."
        return {"reason": reason, "suggested_solution": suggested}

    # Quick checks / debug
    has_sdk = OpenAI is not None
    has_key = bool(OPENAI_API_KEY)
    print(f"🔐 OpenAI SDK present: {has_sdk}, OPENAI_API_KEY set: {has_key}")

    if not has_key or not has_sdk:
        if not has_key:
            print("⚠️ OPENAI_API_KEY missing — using local suggestion")
        else:
            print("⚠️ OpenAI SDK not installed/importable — using local suggestion")
        return local_suggestion()

    # Build prompt
    prompt_parts = [
        f"Service: {service}",
        f"Detected issue: {issue}",
        "Details (json):",
        json.dumps(details or {}, ensure_ascii=False),
        "Raw payload:",
        raw_payload or "",
        "",
        "Respond ONLY with a very short JSON object with exactly two keys: reason and suggested_solution.",
        "Make reason one short sentence and suggested_solution 1-2 concrete steps (include pm2 commands when relevant).",
        "Output valid JSON only."
    ]
    user_prompt = "\n".join(prompt_parts)

    try:
        client = OpenAI(api_key=OPENAI_API_KEY)
        resp = client.chat.completions.create(
            model=OPENAI_MODEL,
            messages=[
                {"role": "system", "content": "You are a concise actionable assistant. Output valid JSON only."},
                {"role": "user", "content": user_prompt}
            ],
            temperature=0.0,
            max_tokens=150
        )

        # extract text from common shapes
        text = None
        if getattr(resp, "choices", None):
            first = resp.choices[0]
            msg = getattr(first, "message", None)
            if isinstance(msg, dict):
                text = msg.get("content")
            else:
                text = getattr(first, "message", None) and getattr(first.message, "content", None)
            if not text:
                text = getattr(first, "text", None) or (getattr(first, "delta", None) and getattr(first.delta, "content", None))

        if not text:
            try:
                text = json.dumps(resp, default=lambda o: getattr(o, "__dict__", str(o)), ensure_ascii=False)
            except Exception:
                text = str(resp)

        print("🔎 OpenAI returned raw text:", text if isinstance(text, str) else str(type(text)))

        # Try parse strict JSON, then JSON substring, else use raw text as suggested_solution
        try:
            parsed = json.loads(text)
            if isinstance(parsed, dict):
                reason = parsed.get("reason")
                suggested = parsed.get("suggested_solution") or parsed.get("solution")
                if reason or suggested:
                    return {"reason": reason, "suggested_solution": suggested}
        except Exception:
            import re
            m = re.search(r'(\{[\s\S]*\})', text)
            if m:
                try:
                    parsed = json.loads(m.group(1))
                    if isinstance(parsed, dict):
                        reason = parsed.get("reason")
                        suggested = parsed.get("suggested_solution") or parsed.get("solution")
                        if reason or suggested:
                            return {"reason": reason, "suggested_solution": suggested}
                except Exception:
                    pass

        # Fallback: store raw LLM text (trim) so DB won't be NULL
        fallback_text = (text.strip() if isinstance(text, str) else str(text))[:1000]
        return {"reason": None, "suggested_solution": fallback_text or local_suggestion()["suggested_solution"]}

    except Exception as e:
        print("❌ OpenAI call failed:", e)
        return local_suggestion()


# =========================
# 💾 Save Incident (extended)
# =========================
def save_incident(conn, service, issue, action, status):
    """
    Insert incident into Postgres. Attempts extended insert (details/json, raw, solution),
    falls back to minimal insert if schema lacks columns.
    """
    print(f"💾 save_incident: service={service} issue={issue} action={action} status={status}")
    cur = None
    try:
        cur = conn.cursor()
        details_json = getattr(save_incident, "_last_details_json", None)
        raw_payload = getattr(save_incident, "_last_raw", None)
        solution_text = getattr(save_incident, "_last_solution", None)

        try:
            cur.execute(
                """
                INSERT INTO incidents (service_name, issue, action, status, details, raw_payload, solution)
                VALUES (%s, %s, %s, %s, %s::jsonb, %s, %s)
                """,
                (service, issue, action, status, details_json, raw_payload, solution_text)
            )
        except Exception:
            # fallback to minimal insert
            cur.execute(
                """
                INSERT INTO incidents (service_name, issue, action, status)
                VALUES (%s, %s, %s, %s)
                """,
                (service, issue, action, status)
            )
        conn.commit()
        print("📦 Incident saved to DB")
    except Exception as e:
        print("❌ Failed to save incident:", e)
        try:
            if conn:
                conn.rollback()
        except Exception:
            pass
    finally:
        try:
            if cur:
                cur.close()
        except Exception:
            pass


# =========================
# 🔍 Detection Logic
# =========================
def detect_issue(log):
    """
    Analyze log (dict or string). Return (issue_name or None, details dict).
    """
    try:
        details = {}
        raw_text = ""

        if isinstance(log, dict):
            details['payload'] = log
            raw_text = log.get('raw') or log.get('message') or json.dumps(log)
            cpu = log.get('cpu', 0)
            status = log.get('status')
            if isinstance(cpu, (int, float)) and cpu > 80:
                details['reason'] = f"cpu={cpu}"
                return "high_cpu", details
            if status == "crash":
                details['reason'] = "status=crash"
                return "app_crash", details
            if log.get('error_name') or log.get('error_message'):
                details['error_name'] = log.get('error_name')
                details['error_message'] = log.get('error_message') or raw_text
                return "app_error", details

        else:
            raw_text = str(log)
            # try parse JSON string
            try:
                parsed = json.loads(raw_text)
                return detect_issue(parsed)
            except Exception:
                details['raw'] = raw_text

        txt = (raw_text or "").lower()
        import re
        # DB/connectivity detection (3306/mysql or common patterns)
        mysql_port = re.search(r'\b3306\b', txt)
        pg_port = re.search(r'\b5432\b', txt)
        conn_refused = re.search(r'(connection refused|connect.*refused|timeout|could not connect|refused)', txt)
        db_word = re.search(r'\b(mysql|mariadb|postgres|postgresql|db)\b', txt)
        if conn_refused and (mysql_port or pg_port or db_word):
            details['matched'] = conn_refused.group(0)
            details['note'] = 'db_connection_issue'
            return "db_connect_error", details

        # generic error/exception detection
        if 'error' in txt or 'exception' in txt:
            m = re.search(r'([A-Za-z0-9_.$]+(?:Error|Exception))[:\s-]+(.+)', raw_text)
            if m:
                details['error_name'] = m.group(1)
                details['error_message'] = m.group(2).strip()
            else:
                details['error_message'] = raw_text
            return "app_error", details

    except Exception as e:
        print("⚠️ detect_issue failed:", e)

    return None, {}


# =========================
# ⚡ Decision Engine
# =========================
def decide_action(issue):
    print("🧠 decide_action:", issue)
    if issue in ["high_cpu", "app_crash", "app_error", "db_connect_error"]:
        return "pm2_restart"
    return None


# =========================
# 🔧 Execution Layer
# =========================
def execute_action(action, service):
    print(f"🔧 execute_action: action={action} service={service}")
    if action == "pm2_restart":
        # restart the service managed by pm2; service should be the pm2 name
        svc = service or "whatscrm"
        print(f"🔁 Restarting PM2 process: {svc}")
        try:
            subprocess.run(["pm2", "restart", svc], check=True)
            print(f"✅ pm2 restarted {svc}")
        except subprocess.CalledProcessError as e:
            print("❌ pm2 restart failed:", e)
            raise


# =========================
# 🚀 Kafka Config (Aiven SSL)
# =========================
conf = {
    'bootstrap.servers': 'kafka-13e2318a-demo1.d.aivencloud.com:22770',
    'group.id': 'devops-group',
    'auto.offset.reset': 'earliest',

    'security.protocol': 'SSL',
    'ssl.ca.location': './ca.pem', #ca certificate from Aiven
    'ssl.certificate.location': './service.cert', #service certificate from Aiven
    'ssl.key.location': './service.key' #service key from Aiven
}

consumer = Consumer(conf)
consumer.subscribe(['logs'])

print("🚀 Kafka Consumer started...")

# =========================
# 🧠 MAIN FLOW
# =========================
conn = connect_db()

if conn:
    test_db(conn)
else:
    print("⚠️ Continuing without DB logging...")

running = True

def shutdown(sig, frame):
    global running
    print("🛑 Shutdown signal received:", sig)
    running = False

signal.signal(signal.SIGINT, shutdown)
signal.signal(signal.SIGTERM, shutdown)

try:
    while running:
        try:
            msg = consumer.poll(1.0)

            if msg is None:
                continue

            if msg.error():
                print("❌ Kafka Error:", msg.error())
                continue

            raw_val = msg.value()
            try:
                log = json.loads(raw_val.decode('utf-8')) if isinstance(raw_val, (bytes, bytearray)) else json.loads(raw_val)
            except Exception:
                # if payload is plain string, keep as-is
                try:
                    log = raw_val.decode('utf-8') if isinstance(raw_val, (bytes, bytearray)) else str(raw_val)
                except Exception:
                    log = raw_val

            print("📥 Received:", log)

            issue, details = detect_issue(log)

            if issue:
                service = None
                if isinstance(log, dict):
                    service = log.get('service') or log.get('app') or log.get('name')
                if not service:
                    service = 'unknown-service'

                # 🔥 Valkey duplicate check
                if is_duplicate(issue, service):
                    print(f"⏩ Skipping duplicate issue: {issue} for {service}")
                    continue

                print(f"⚠️ Issue detected: {issue} for {service}")

                action = decide_action(issue)

                # Prepare raw payload and details for DB and LLM
                raw_payload = None
                try:
                    raw_payload = json.dumps(log) if isinstance(log, dict) else str(log)
                except Exception:
                    raw_payload = str(log)

                # call Gemini/LLM to suggest reason and solution (non-blocking timeout)
                solution_text = None
                try:
                    sol = call_gemini_for_solution(service, issue, details, raw_payload)
                    print("🔔 LLM suggestion object:", sol)
                    if isinstance(sol, dict):
                        reason = sol.get("reason")
                        suggested = sol.get("suggested_solution") or sol.get("solution")
                        # only set solution_text if we have something meaningful
                        if reason or suggested:
                            solution_text = json.dumps({"reason": reason, "solution": suggested}, ensure_ascii=False)
                        else:
                            solution_text = None
                    else:
                        solution_text = str(sol) if sol is not None else None
                except Exception as e:
                    print("⚠️ LLM call failed:", e)
                    solution_text = None

                # store details & raw & solution into save_incident temp attributes (used by insert)
                try:
                    save_incident._last_details_json = json.dumps(details) if details else None
                except Exception:
                    save_incident._last_details_json = None
                save_incident._last_raw = raw_payload
                save_incident._last_solution = solution_text

                if action:
                    try:
                        execute_action(action, service)

                        if conn:
                            save_incident(
                                conn,
                                service,
                                issue,
                                action,
                                "success"
                            )

                        print(f"✅ Action executed: {action}")

                    except Exception as e:
                        print("❌ Action execution failed:", e)
                        traceback.print_exc()
                        if conn:
                            # mark failed and persist details
                            try:
                                save_incident._last_details_json = json.dumps(details) if details else None
                            except Exception:
                                save_incident._last_details_json = None
                            save_incident._last_raw = json.dumps(log) if isinstance(log, dict) else str(log)
                            save_incident._last_solution = solution_text
                            save_incident(
                                conn,
                                service,
                                issue,
                                action,
                                "failed"
                            )

        except Exception as e:
            print("🔥 Unexpected loop error:", e)
            traceback.print_exc()
            time.sleep(2)
finally:
    print("🧹 Shutting down consumer and cleaning up...")
    try:
        consumer.close()
        print("🔌 Kafka consumer closed")
    except Exception as e:
        print("⚠️ Error closing consumer:", e)
    try:
        if conn:
            conn.close()
            print("🔌 DB connection closed")
    except Exception as e:
        print("⚠️ Error closing DB connection:", e)
    print("🛑 Exited")
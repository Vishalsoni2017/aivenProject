const { Kafka } = require('kafkajs');
const fs = require('fs');
const path = require('path');
const os = require('os');

let TailFile;
try {
  const tfMod = require('tail-file');
  TailFile = tfMod.TailFile || tfMod.default || tfMod;
  if (typeof TailFile !== 'function') throw new Error('TailFile export is not a constructor');
} catch (e) {
  console.error('❌ Could not load "tail-file" module as a constructor, falling back to simple watcher:', e.message || e);
  TailFile = null;
}

const kafka = new Kafka({
  clientId: 'app',
  brokers: ['KAFKA_BROKERS'], #KAFKA_BROKERS env var can override this

  ssl: {
    rejectUnauthorized: true,
    ca: [fs.readFileSync('./ca.pem', 'utf-8')], //ca.pem should contain the full certificate chain (root + intermediates) if applicable
    key: fs.readFileSync('./service.key', 'utf-8'), //service.key should be the private key for the client certificate
    cert: fs.readFileSync('./service.cert', 'utf-8'), //service.cert should be the client certificate (can be the same as the one in ca.pem if it's a self-signed cert, but often is a separate cert signed by the CA)
  },
});

const producer = kafka.producer();
const pm2LogDir = process.env.PM2_LOG_DIR || path.join(process.env.USERPROFILE || process.env.HOME || '.', '.pm2', 'logs');
const tails = new Map();

// aggregation state to join multiline log entries (stack traces, wrapped lines, etc.)
const aggState = new Map();
const AGGREGATE_FLUSH_MS = Number(process.env.PM2_AGGREGATE_MS || 200);

function pushAggregatedLine(file, line) {
  // always print incoming line immediately for visibility
  console.log('🖨 PM2 line:', file, line);

  let s = aggState.get(file);
  if (!s) {
    s = { lines: [], timer: null };
    aggState.set(file, s);
  }

  s.lines.push(line);
  if (s.timer) clearTimeout(s.timer);

  s.timer = setTimeout(async () => {
    aggState.delete(file);

    const raw = s.lines.join('\n');
    const payload = {
      source: 'pm2-file',
      file,
      stream: file.toLowerCase().includes('error') ? 'stderr' : 'stdout',
      raw,
      time: new Date().toISOString(),
    };

    // try to parse JSON logs (common for structured apps)
    try { payload.parsed = JSON.parse(raw); } catch (_) {}

    // detect common log level tokens from first line
    const lvl = (s.lines[0] || '').match(/\b(ERROR|WARN|INFO|DEBUG|TRACE)\b/i);
    if (lvl) payload.level = lvl[1].toUpperCase();

    // try to extract Error/Exception name + message from whole block
    const errMatch = raw.match(/([A-Za-z0-9_$\.]+(?:Error|Exception))[:\s-]+(.+)/i);
    if (errMatch) {
      payload.error_name = errMatch[1];
      payload.error_message = errMatch[2].trim();
    }

    // Send aggregated payload to Kafka
    try {
      const res = await producer.send({
        topic: 'logs',
        messages: [{ key: file, value: JSON.stringify(payload) }],
      });
      console.log('✅ Kafka send result (aggregated):', file, res);
    } catch (err) {
      console.error('❌ Failed to forward aggregated PM2 block to Kafka:', err);
    }
  }, AGGREGATE_FLUSH_MS);
}

// Return CPU usage percent (0-100). On Linux uses /proc/stat sampling; falls back to os.cpus() sampling.
async function getCpuPercent(sampleMs = 500) {
  function sleep(ms) { return new Promise(r => setTimeout(r, ms)); }

  // Linux /proc/stat fast path
  if (process.platform === 'linux') {
    try {
      const parseStat = (raw) => {
        const line = raw.split('\n')[0].trim();
        const parts = line.split(/\s+/).slice(1).map(Number); // skip 'cpu'
        const idle = parts[3]; // idle is the 4th field
        const total = parts.reduce((a, b) => a + b, 0);
        return { idle, total };
      };

      const a = parseStat(await fs.promises.readFile('/proc/stat', 'utf8'));
      await sleep(sampleMs);
      const b = parseStat(await fs.promises.readFile('/proc/stat', 'utf8'));

      const idleDelta = b.idle - a.idle;
      const totalDelta = b.total - a.total;
      if (totalDelta <= 0) return 0;
      const usage = 1 - idleDelta / totalDelta;
      return Math.round(usage * 100);
    } catch (e) {
      // fall through to os.cpus() approach
    }
  }

  // Fallback: sample os.cpus() times (works cross-platform)
  try {
    const snapshot = () => {
      const cpus = os.cpus();
      let idle = 0, total = 0;
      for (const c of cpus) {
        const t = c.times;
        const cpuTotal = t.user + t.nice + t.sys + t.irq + t.idle;
        idle += t.idle;
        total += cpuTotal;
      }
      return { idle, total };
    };

    const a = snapshot();
    await sleep(sampleMs);
    const b = snapshot();
    const idleDelta = b.idle - a.idle;
    const totalDelta = b.total - a.total;
    if (totalDelta <= 0) return 0;
    const usage = 1 - idleDelta / totalDelta;
    return Math.round(usage * 100);
  } catch (e) {
    return 0;
  }
}

function startSimpleTail(fullPath, onLine) {
  // simple tail implementation: read appended content on file change
  let pos = 0;
  let buffer = '';

  // initialize position to current file size if the file exists
  try {
    const st = fs.statSync(fullPath);
    pos = st.size;
  } catch (e) {
    pos = 0;
  }

  const watcher = fs.watch(fullPath, { persistent: true }, async (eventType) => {
    if (eventType !== 'change') return;
    try {
      const st = await fs.promises.stat(fullPath);
      if (st.size <= pos) return;
      const rs = fs.createReadStream(fullPath, { encoding: 'utf8', start: pos, end: st.size - 1 });
      rs.on('data', (chunk) => {
        buffer += chunk;
        let lines = buffer.split(/\r?\n/);
        buffer = lines.pop();
        for (const line of lines) {
          onLine(line);
        }
      });
      rs.on('end', () => {
        pos = st.size;
      });
      rs.on('error', (err) => {
        console.error('❌ readStream error for', fullPath, err);
      });
    } catch (err) {
      // file might be removed between events
    }
  });

  return {
    quit: async () => {
      try { watcher.close(); } catch (e) {}
    }
  };
}

async function safeStartTail(tail, full) {
  // Some tail implementations have .start() returning a promise, others don't.
  if (tail && typeof tail.start === 'function') {
    try {
      await tail.start();
      return;
    } catch (err) {
      console.error('❌ Tail start failed for', full, err);
      return;
    }
  }

  // Older/shipped implementations might use .watch() or already start on construct.
  if (tail && typeof tail.watch === 'function') {
    try {
      tail.watch();
      return;
    } catch (err) {
      console.error('❌ Tail watch failed for', full, err);
      return;
    }
  }

  // If neither exists, assume tail is already active (no-op)
  return;
}

async function tailPM2Logs(dir) {
  try {
    const files = await fs.promises.readdir(dir);
    const logFiles = files.filter(f => f.endsWith('.log') || f.endsWith('.out') || f.endsWith('.err'));

    for (const file of logFiles) {
      const full = path.join(dir, file);
      if (tails.has(full)) continue;

      let tail;
      if (TailFile) {
        try {
          tail = new TailFile(full, { startPos: 'end', encoding: 'utf8' });
        } catch (err) {
          console.warn('⚠️ TailFile construction failed for', full, err, '- falling back to simple tail');
          tail = null;
        }
      }

      if (!tail) {
        // pass each raw line into the aggregator (it prints immediately and sends aggregated blocks)
        tail = startSimpleTail(full, (line) => {
          pushAggregatedLine(file, line);
        });
        tails.set(full, tail);
        console.log('🔔 Simple tailing PM2 log:', full);
        continue;
      }

      // attach listeners only if event API exists
      if (typeof tail.on === 'function') {
        // feed module tail lines into the same aggregator so multiline entries are grouped
        tail.on('line', (line) => {
          pushAggregatedLine(file, line);
        });

        tail.on('error', (err) => {
          console.error('❌ Tail error for', full, err);
        });
      } else {
        console.warn('⚠️ Tail instance for', full, 'does not support .on(event). Events will not be forwarded for this file.');
      }

      // start tail if applicable (safe)
      await safeStartTail(tail, full);
      tails.set(full, tail);
      console.log('🔔 Tailing PM2 log (module):', full);
    }
  } catch (err) {
    console.error('❌ Failed to scan PM2 log dir:', dir, err);
  }
}

async function stopTails() {
  for (const [file, tail] of tails) {
    try {
      // try common shutdown methods in order
      if (tail) {
        if (typeof tail.quit === 'function') {
          await tail.quit();
        } else if (typeof tail.stop === 'function') {
          await tail.stop();
        } else if (typeof tail.close === 'function') {
          await tail.close();
        } else if (typeof tail.destroy === 'function') {
          await tail.destroy();
        }
      }
      console.log('🛑 Stopped tailing:', file);
    } catch (e) {
      console.warn('⚠️ Error stopping tail for', file, e);
    }
  }
  tails.clear();
}

const run = async () => {
  await producer.connect();
  console.log('✅ Connected to Kafka');

  // start tailing existing PM2 log files (if available)
  await tailPM2Logs(pm2LogDir);

  // watch for new log files in the PM2 log directory
  try {
    fs.watch(pm2LogDir, { persistent: true }, async (eventType, filename) => {
      if (!filename) return;
      const full = path.join(pm2LogDir, filename);
      if (eventType === 'rename') {
        // file created or removed - try to tail if exists
        try {
          const st = await fs.promises.stat(full);
          if (st.isFile()) {
            await tailPM2Logs(pm2LogDir);
          }
        } catch (_) {}
      }
    });
  } catch (e) {
    console.warn('⚠️ Could not watch PM2 log dir:', pm2LogDir, e);
  }

  // Replace synthetic cpu generator with real sampling on the server
  setInterval(async () => {
    let cpuPercent = 0;
    try {
      cpuPercent = await getCpuPercent(800); // sample for ~800ms
    } catch (e) {
      console.warn('⚠️ getCpuPercent failed, using 0:', e);
      cpuPercent = 0;
    }

    const log = {
      service: 'whatscrm',
      cpu: cpuPercent,
      status: Math.random() > 0.8 ? 'crash' : 'running',
      time: new Date().toISOString(),
    };

    try {
      await producer.send({
        topic: 'logs',
        messages: [{ value: JSON.stringify(log) }],
      });
      console.log('📤 Sent:', log);
    } catch (err) {
      console.error('❌ Failed to send synthetic log:', err);
    }
  }, 9000);
};

process.on('SIGINT', async () => {
  console.log('🛑 SIGINT received, shutting down...');
  try { await stopTails(); } catch (e) {}
  try { await producer.disconnect(); console.log('🔌 Producer disconnected'); } catch (e) { console.error(e); }
  process.exit(0);
});

process.on('SIGTERM', async () => {
  console.log('🛑 SIGTERM received, shutting down...');
  try { await stopTails(); } catch (e) {}
  try { await producer.disconnect(); console.log('🔌 Producer disconnected'); } catch (e) { console.error(e); }
  process.exit(0);
});

run().catch(err => {
  console.error('🔥 Fatal error in run():', err);
  process.exit(1);
});
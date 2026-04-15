import crypto from "crypto";
import express from "express";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import dotenv from "dotenv";
import Database from "better-sqlite3";

dotenv.config();

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.PORT) || 3000;
// Trim: Windows .env often carries \r on the line, which would break === with the browser input.
const CONDUCTOR_SECRET = String(process.env.CONDUCTOR_SECRET ?? "").trim();
const PUBLIC_BASE_URL = (process.env.PUBLIC_BASE_URL || "").replace(/\/$/, "");

const DATA_DIR = path.join(__dirname, "data");
const DB_PATH = path.join(DATA_DIR, "survey.db");

if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });

const db = new Database(DB_PATH);

db.exec(`
  CREATE TABLE IF NOT EXISTS participants (
    id TEXT PRIMARY KEY,
    label TEXT NOT NULL,
    token TEXT NOT NULL,
    created_at INTEGER NOT NULL,
    updated_at INTEGER NOT NULL,
    survey_json TEXT NOT NULL DEFAULT '{}'
  );
`);

function randomToken() {
  return crypto.randomBytes(32).toString("hex");
}

function defaultSurveyJson() {
  return JSON.stringify({
    locations: [],
    routes: {
      current: { segments: [], totalCostGold: 0 },
      ibx: { segments: [], totalCostGold: 0 }
    },
    ibxLine: { loaded: false }
  });
}

function authParticipant(req, res, row) {
  const auth = req.headers.authorization || "";
  const m = auth.match(/^Bearer\s+(.+)$/i);
  const token = m ? m[1].trim() : "";
  if (!token || token !== row.token) {
    res.status(401).json({ error: "Invalid or missing participant token" });
    return false;
  }
  return true;
}

function authConductor(req, res) {
  return true;
}

const app = express();
app.use(express.json({ limit: "20mb" }));

app.post("/api/participants", (req, res) => {
  if (!authConductor(req, res)) return;
  const label = String(req.body?.label ?? "").trim() || "Participant";
  const id = crypto.randomUUID();
  const token = randomToken();
  const now = Date.now();
  const survey_json = defaultSurveyJson();

  db.prepare(
    `INSERT INTO participants (id, label, token, created_at, updated_at, survey_json)
     VALUES (@id, @label, @token, @created_at, @updated_at, @survey_json)`
  ).run({ id, label, token, created_at: now, updated_at: now, survey_json });

  const base =
    PUBLIC_BASE_URL ||
    `${req.protocol}://${req.get("host")}`;
  const url = `${base}/index.html?participant=${encodeURIComponent(id)}&token=${encodeURIComponent(token)}`;

  res.json({ id, label, token, shareUrl: url });
});

app.get("/api/participant/:id", (req, res) => {
  const row = db.prepare(`SELECT * FROM participants WHERE id = ?`).get(req.params.id);
  if (!row) return res.status(404).json({ error: "Not found" });
  if (!authParticipant(req, res, row)) return;
  res.json(JSON.parse(row.survey_json));
});

app.put("/api/participant/:id", (req, res) => {
  const row = db.prepare(`SELECT * FROM participants WHERE id = ?`).get(req.params.id);
  if (!row) return res.status(404).json({ error: "Not found" });
  if (!authParticipant(req, res, row)) return;

  const body = req.body;
  if (!body || typeof body !== "object") {
    return res.status(400).json({ error: "Expected JSON object" });
  }
  const survey_json = JSON.stringify(body);
  const now = Date.now();
  db.prepare(
    `UPDATE participants SET survey_json = @survey_json, updated_at = @updated_at WHERE id = @id`
  ).run({ survey_json, updated_at: now, id: req.params.id });
  res.json({ ok: true, updatedAt: now });
});

app.get("/api/conductor/participants", (req, res) => {
  if (!authConductor(req, res)) return;
  const rows = db.prepare(`SELECT id, label, created_at, updated_at FROM participants ORDER BY updated_at DESC`).all();
  const list = rows.map((r) => {
    let locCount = 0;
    let segCurrent = 0;
    let segIbx = 0;
    try {
      const s = JSON.parse(db.prepare(`SELECT survey_json FROM participants WHERE id = ?`).get(r.id).survey_json);
      locCount = s.locations?.length ?? 0;
      segCurrent = s.routes?.current?.segments?.length ?? 0;
      segIbx = s.routes?.ibx?.segments?.length ?? 0;
    } catch {
      // ignore
    }
    return {
      id: r.id,
      label: r.label,
      createdAt: r.created_at,
      updatedAt: r.updated_at,
      counts: { locations: locCount, currentSegments: segCurrent, ibxSegments: segIbx }
    };
  });
  res.json(list);
});

app.get("/api/conductor/participants/:id", (req, res) => {
  if (!authConductor(req, res)) return;
  const row = db.prepare(`SELECT id, label, survey_json, updated_at FROM participants WHERE id = ?`).get(req.params.id);
  if (!row) return res.status(404).json({ error: "Not found" });
  res.json({
    id: row.id,
    label: row.label,
    updatedAt: row.updated_at,
    state: JSON.parse(row.survey_json)
  });
});

app.use(express.static(__dirname));

app.listen(PORT, () => {
  console.log(`Survey server http://localhost:${PORT}`);
  console.log(`Main (conductor): http://localhost:${PORT}/main.html`);
  if (CONDUCTOR_SECRET) {
    console.warn("Note: CONDUCTOR_SECRET is set but conductor auth is currently disabled.");
  }
});

import crypto from "crypto";
import express from "express";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import dotenv from "dotenv";
import { spawnSync } from "child_process";

dotenv.config();

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const PORT = Number(process.env.PORT) || 3000;
const CONDUCTOR_SECRET = String(process.env.CONDUCTOR_SECRET ?? "").trim();
const PUBLIC_BASE_URL = (process.env.PUBLIC_BASE_URL || "").replace(/\/$/, "");

const DATA_DIR = path.join(__dirname, "data");
const LEGACY_SURVEY_DB = path.join(DATA_DIR, "survey.db");
const PARTICIPANTS_DIR = path.join(DATA_DIR, "Participants_Data");
// One JSON file per participant under Participants_Data/. Bearer tokens are in each file—treat repo access accordingly.

const UUID_RE = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/i;

function isParticipantId(id) {
  return typeof id === "string" && UUID_RE.test(id.trim());
}

if (!fs.existsSync(DATA_DIR)) fs.mkdirSync(DATA_DIR, { recursive: true });
if (!fs.existsSync(PARTICIPANTS_DIR)) fs.mkdirSync(PARTICIPANTS_DIR, { recursive: true });

function ensureMigratedFromLegacySurveyDb() {
  if (!fs.existsSync(LEGACY_SURVEY_DB)) return;
  const migrateScript = path.join(__dirname, "scripts", "migrate-legacy-survey-db.mjs");
  const { status, error } = spawnSync(process.execPath, [migrateScript], {
    cwd: __dirname,
    stdio: "inherit"
  });
  if (error) {
    console.error("Legacy survey.db migration spawn error:", error);
    process.exit(1);
  }
  if (status !== 0) {
    console.error("Legacy survey.db migration failed.");
    process.exit(1);
  }
}

ensureMigratedFromLegacySurveyDb();

function participantFilePath(id) {
  return path.join(PARTICIPANTS_DIR, `${id}.json`);
}

function readParticipant(id) {
  const fp = participantFilePath(id);
  if (!fs.existsSync(fp)) return null;
  try {
    return JSON.parse(fs.readFileSync(fp, "utf8"));
  } catch {
    return null;
  }
}

function writeParticipantAtomic(record) {
  const fp = participantFilePath(record.id);
  const dir = path.dirname(fp);
  const tmp = path.join(dir, `.${record.id}.${process.pid}.${Date.now()}.tmp`);
  fs.writeFileSync(tmp, JSON.stringify(record));
  fs.renameSync(tmp, fp);
}

function listParticipantsSortedByUpdatedDesc() {
  let names = [];
  try {
    names = fs.readdirSync(PARTICIPANTS_DIR).filter((n) => n.endsWith(".json"));
  } catch {
    return [];
  }
  const out = [];
  for (const n of names) {
    const id = n.slice(0, -5);
    if (!isParticipantId(id)) continue;
    const rec = readParticipant(id);
    if (!rec || typeof rec !== "object" || typeof rec.updatedAt !== "number") continue;
    out.push(rec);
  }
  out.sort((a, b) => (b.updatedAt ?? 0) - (a.updatedAt ?? 0));
  return out;
}

function randomToken() {
  return crypto.randomBytes(32).toString("hex");
}

function defaultSurvey() {
  return {
    industry: {
      companyName: "",
      roleKey: "",
      roleOtherDetail: "",
      goodsCategoryKeys: [],
      goodsOtherDetail: "",
      rawMaterials: [],
      rawMaterialBranches: [],
      products: [],
      productBranches: []
    },
    locations: [],
    routes: {
      current: { segments: [], totalCostGold: 0 },
      ibx: { segments: [], totalCostGold: 0 }
    },
    ibxLine: { loaded: false }
  };
}

/** @param {Record<string, unknown>} row participant record row */
function authParticipant(req, res, row) {
  const auth = req.headers.authorization || "";
  const m = auth.match(/^Bearer\s+(.+)$/i);
  const token = m ? m[1].trim() : "";
  const rowToken = typeof row.token === "string" ? row.token : "";
  if (!token || token !== rowToken) {
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

  const record = {
    id,
    label,
    token,
    createdAt: now,
    updatedAt: now,
    survey: defaultSurvey()
  };

  writeParticipantAtomic(record);

  const base = PUBLIC_BASE_URL || `${req.protocol}://${req.get("host")}`;
  const url = `${base}/sub.html?participant=${encodeURIComponent(id)}&token=${encodeURIComponent(token)}`;

  res.json({ id, label, token, shareUrl: url });
});

app.get("/api/participant/:id", (req, res) => {
  const pid = req.params.id;
  if (!isParticipantId(pid)) return res.status(400).json({ error: "Invalid participant id" });
  const row = readParticipant(pid);
  if (!row) return res.status(404).json({ error: "Not found" });
  if (!authParticipant(req, res, row)) return;
  res.json(row.survey ?? {});
});

app.put("/api/participant/:id", (req, res) => {
  const pid = req.params.id;
  if (!isParticipantId(pid)) return res.status(400).json({ error: "Invalid participant id" });
  const row = readParticipant(pid);
  if (!row) return res.status(404).json({ error: "Not found" });
  if (!authParticipant(req, res, row)) return;

  const body = req.body;
  if (!body || typeof body !== "object") {
    return res.status(400).json({ error: "Expected JSON object" });
  }
  const now = Date.now();
  const next = { ...row, survey: body, updatedAt: now };
  writeParticipantAtomic(next);
  res.json({ ok: true, updatedAt: now });
});

app.get("/api/conductor/participants", (req, res) => {
  if (!authConductor(req, res)) return;
  const rows = listParticipantsSortedByUpdatedDesc();
  const list = rows.map((r) => {
    let locCount = 0;
    let segCurrent = 0;
    let segIbx = 0;
    let industryCompany = "";
    let rawMaterialsCount = 0;
    let productsCount = 0;
    try {
      const s = r.survey;
      if (s && typeof s === "object") {
        locCount = s.locations?.length ?? 0;
        segCurrent = s.routes?.current?.segments?.length ?? 0;
        segIbx = s.routes?.ibx?.segments?.length ?? 0;
        industryCompany = String(s.industry?.companyName ?? "").trim();
        rawMaterialsCount = Array.isArray(s.industry?.rawMaterials) ? s.industry.rawMaterials.length : 0;
        productsCount = Array.isArray(s.industry?.products) ? s.industry.products.length : 0;
      }
    } catch {
      // ignore
    }
    return {
      id: r.id,
      label: r.label,
      createdAt: r.createdAt,
      updatedAt: r.updatedAt,
      industryCompany: industryCompany ? industryCompany.slice(0, 80) : "",
      counts: { locations: locCount, currentSegments: segCurrent, ibxSegments: segIbx, rawMaterials: rawMaterialsCount, products: productsCount }
    };
  });
  res.json(list);
});

app.get("/api/conductor/participants/:id", (req, res) => {
  if (!authConductor(req, res)) return;
  const pid = req.params.id;
  if (!isParticipantId(pid)) return res.status(400).json({ error: "Invalid participant id" });
  const row = readParticipant(pid);
  if (!row) return res.status(404).json({ error: "Not found" });
  res.json({
    id: row.id,
    label: row.label,
    updatedAt: row.updatedAt,
    state: row.survey ?? {}
  });
});

app.delete("/api/conductor/participants/:id", (req, res) => {
  if (!authConductor(req, res)) return;
  const id = String(req.params.id || "");
  if (!id) return res.status(400).json({ error: "Missing id" });
  if (!isParticipantId(id)) return res.status(400).json({ error: "Invalid participant id" });
  const fp = participantFilePath(id);
  if (!fs.existsSync(fp)) return res.status(404).json({ error: "Not found" });
  fs.unlinkSync(fp);
  res.json({ ok: true });
});

app.use(express.static(__dirname));

app.listen(PORT, () => {
  console.log(`Survey server http://localhost:${PORT}`);
  console.log(`Main (conductor): http://localhost:${PORT}/index.html`);
  if (CONDUCTOR_SECRET) {
    console.warn("Note: CONDUCTOR_SECRET is set but conductor auth is currently disabled.");
  }
});

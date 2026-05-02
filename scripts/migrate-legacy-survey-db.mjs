import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import Database from "better-sqlite3";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "..");
const DATA_DIR = path.join(ROOT, "data");
const DB_PATH = path.join(DATA_DIR, "survey.db");
const PARTICIPANTS_DIR = path.join(DATA_DIR, "Participants_Data");
/** Presence means participant JSON files are authoritative; safe to skip re-reading survey.db */
const SQLITE_MIGRATED_SENTINEL = path.join(DATA_DIR, ".survey_sqlite_migrated");

function participantPath(id) {
  return path.join(PARTICIPANTS_DIR, `${id}.json`);
}

function atomicWrite(jsonPath, data) {
  const dir = path.dirname(jsonPath);
  const basename = path.basename(jsonPath);
  const tmp = path.join(dir, `.${basename}.${process.pid}.${Date.now()}.tmp`);
  fs.writeFileSync(tmp, JSON.stringify(data));
  fs.renameSync(tmp, jsonPath);
}

if (!fs.existsSync(DB_PATH)) {
  console.log("No legacy survey.db; nothing to migrate.");
  process.exit(0);
}

if (fs.existsSync(SQLITE_MIGRATED_SENTINEL)) {
  console.log("Legacy SQLite already migrated (sentinel present); skipping.");
  process.exit(0);
}

if (!fs.existsSync(PARTICIPANTS_DIR)) {
  fs.mkdirSync(PARTICIPANTS_DIR, { recursive: true });
}

const db = new Database(DB_PATH, { readonly: true });
try {
  const rows = db.prepare(`SELECT id, label, token, created_at, updated_at, survey_json FROM participants`).all();
  for (const row of rows) {
    let survey;
    try {
      survey = JSON.parse(row.survey_json || "{}");
    } catch {
      survey = {};
    }
    const record = {
      id: row.id,
      label: row.label,
      token: row.token,
      createdAt: row.created_at,
      updatedAt: row.updated_at,
      survey
    };
    atomicWrite(participantPath(row.id), record);
  }
  console.log(`Migrated ${rows.length} participant(s) to ${PARTICIPANTS_DIR}`);
} finally {
  db.close();
}

/** Mark JSON authoritative before touching survey.db (rename can fail EBUSY if another process holds the DB). */
fs.writeFileSync(SQLITE_MIGRATED_SENTINEL, `${new Date().toISOString()}\n`);

const backupPath = `${DB_PATH}.migrated_backup`;
try {
  if (fs.existsSync(backupPath)) fs.rmSync(backupPath, { force: true });
  fs.renameSync(DB_PATH, backupPath);
  console.log(`Renamed survey.db to ${path.basename(backupPath)}`);
} catch (e) {
  if (e.code === "EBUSY" || e.code === "EPERM") {
    console.warn(
      `Could not rename survey.db (${e.code}): stop any other servers using this file, then delete or rename survey.db manually. Participant data is stored in Participants_Data/*.json`
    );
  } else {
    throw e;
  }
}

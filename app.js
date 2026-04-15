/* global L, turf, proj4 */

const STORAGE_KEY = "nyc_supply_survey_v1";

const SURVEY_MODE =
  document.body?.dataset?.surveyMode === "conductor"
    ? "conductor"
    : document.body?.dataset?.surveyMode === "participant"
      ? "participant"
      : "standalone";

let readOnly = SURVEY_MODE === "conductor";
let participantId = null;
let participantToken = null;
let saveDebounceTimer = null;
let viewingParticipantId = null;
let viewingParticipantLabel = "";

const COST_PER_MILE_GOLD = {
  huge: 1.0,
  small: 0.5,
  train: 0.7,
  ibx: 0.7
};

const TRANSFER_COST_GOLD = {
  // Symmetric transfers between modes (your confirmation).
  "huge->small": 0.2,
  "small->huge": 0.2,
  "train->small": 0.1,
  "small->train": 0.1,
  "train->huge": 0.5,
  "huge->train": 0.5
};

const LOCATION_TYPES = {
  workplace: { label: "Workplace (your company)", color: "#c94f7c" },
  import: { label: "Import", color: "#5bbcff" },
  export: { label: "Export", color: "#4d7cff" },
  manufacturing: { label: "Manufacturing", color: "#6ee7b7" },
  warehouse: { label: "Warehouse", color: "#ffd166" },
  subdistribution: { label: "Subdistribution", color: "#f78fb3" },
  other: { label: "Other", color: "#c3a6ff" }
};

function formatGold(n) {
  const rounded = Math.round((n + Number.EPSILON) * 100) / 100;
  return String(rounded);
}

function uid(prefix) {
  return `${prefix}_${Math.random().toString(16).slice(2)}_${Date.now()}`;
}

function escapeHtml(s) {
  return String(s)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;");
}

function downloadFile(filename, content, mimeType) {
  const blob = new Blob([content], { type: mimeType });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = filename;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

function latLngToGeoJSONLine(latlngs) {
  // GeoJSON expects [lng, lat]
  return {
    type: "Feature",
    properties: {},
    geometry: {
      type: "LineString",
      coordinates: latlngs.map((p) => [p.lng, p.lat])
    }
  };
}

function haversineDistanceMiles(a, b) {
  // Fallback only; turf.length is preferred.
  const R = 3958.7613; // Earth radius in miles
  const toRad = (deg) => (deg * Math.PI) / 180;
  const dLat = toRad(b.lat - a.lat);
  const dLng = toRad(b.lng - a.lng);
  const lat1 = toRad(a.lat);
  const lat2 = toRad(b.lat);
  const h =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(lat1) * Math.cos(lat2) * Math.sin(dLng / 2) * Math.sin(dLng / 2);
  return 2 * R * Math.asin(Math.sqrt(h));
}

function lineLengthMiles(latlngs) {
  if (latlngs.length < 2) return 0;
  try {
    const line = latLngToGeoJSONLine(latlngs);
    return turf.length(line, { units: "miles" });
  } catch {
    let sum = 0;
    for (let i = 1; i < latlngs.length; i++) sum += haversineDistanceMiles(latlngs[i - 1], latlngs[i]);
    return sum;
  }
}

function ensureProjDefs() {
  // EPSG:2263 (NAD83 / New York Long Island ft US). Use NAD83 + to_meter form so proj4
  // matches common GIS exports (avoids epsg.io-style +units=us-ft mismatch for IBX stations).
  proj4.defs(
    "EPSG:2263",
    "+proj=lcc +lat_1=41.03333333333333 +lat_2=40.66666666666666 +lat_0=40.16666666666666 +lon_0=-74 +x_0=300000.0000000001 +y_0=0 +ellps=GRS80 +datum=NAD83 +to_meter=0.3048006096012192 +no_defs"
  );
}

function gpsToEPSG2263(lng, lat) {
  // Returns [x, y]
  return proj4("EPSG:4326", "EPSG:2263", [lng, lat]);
}

function epsg2263ToGPS(lng, lat) {
  // Misnamed params: EPSG:2263 uses (x, y). proj4 returns (lng, lat).
  return proj4("EPSG:2263", "EPSG:4326", [lng, lat]);
}

function epsg2263XYToLatLng(x, y) {
  const [lng, lat] = proj4("EPSG:2263", "EPSG:4326", [x, y]);
  return L.latLng(lat, lng);
}

function surveyPointsToLatLngs(points) {
  // Converts persisted survey points (EPSG:2263 x/y OR legacy Leaflet lat/lng) into Leaflet lat/lng.
  if (!Array.isArray(points)) return [];
  return points
    .map((p) => {
      if (Array.isArray(p) && p.length >= 2) return epsg2263XYToLatLng(p[0], p[1]);
      if (p && typeof p.lat === "number" && typeof p.lng === "number") return L.latLng(p.lat, p.lng);
      return null;
    })
    .filter(Boolean);
}

function looksLikeEPSG2263(geojson) {
  const crsName = geojson?.crs?.properties?.name;
  if (typeof crsName === "string" && crsName.includes("2263")) return true;

  // Heuristic fallback: NYC EPSG:2263 projected coordinates tend to be in the
  // hundreds of thousands (x) and tens/hundreds of thousands (y), unlike
  // lng/lat which are within [-180..180] and [-90..90].
  const candidate = (function findFirstPair(v) {
    if (!Array.isArray(v)) return null;
    if (v.length >= 2 && typeof v[0] === "number" && typeof v[1] === "number") return [v[0], v[1]];
    for (const item of v) {
      const res = findFirstPair(item);
      if (res) return res;
    }
    return null;
  })(geojson);

  if (!candidate) return false;
  const [x, y] = candidate;
  return Math.abs(x) > 180 && Math.abs(y) > 90;
}

function transformCoordsRecursive(value) {
  // Recursively transforms [x,y] => [lng,lat] using EPSG:2263 -> EPSG:4326.
  if (!Array.isArray(value)) return value;
  if (value.length >= 2 && typeof value[0] === "number" && typeof value[1] === "number") {
    const [lng, lat] = proj4("EPSG:2263", "EPSG:4326", [value[0], value[1]]);
    return [lng, lat];
  }
  return value.map((v) => transformCoordsRecursive(v));
}

function normalizeIBXGeoJSONForLeaflet(geojson) {
  if (!geojson) return geojson;

  const inferred2263 = looksLikeEPSG2263(geojson);

  if (!inferred2263) {
    // EPSG:4326 / CRS84 (GeoJSON [lng, lat] in degrees): Leaflet expects geographic coordinates.
    if (geojson.crs) {
      const out = JSON.parse(JSON.stringify(geojson));
      delete out.crs;
      return out;
    }
    return geojson;
  }

  // Create a transformed copy so we don't mutate the input object.
  const out = JSON.parse(JSON.stringify(geojson));

  function transformGeometry(geom) {
    if (!geom) return geom;
    if (geom.type === "GeometryCollection") {
      geom.geometries = geom.geometries.map((g) => transformGeometry(g));
      return geom;
    }
    if (geom.coordinates) geom.coordinates = transformCoordsRecursive(geom.coordinates);
    return geom;
  }

  if (out.type === "FeatureCollection" && Array.isArray(out.features)) {
    out.features = out.features.map((f) => {
      if (f && f.geometry) transformGeometry(f.geometry);
      return f;
    });
    // After transforming, CRS no longer matches leaflets expectations.
    delete out.crs;
    return out;
  }

  if (out.type === "Feature") {
    transformGeometry(out.geometry);
    delete out.crs;
    return out;
  }

  // Geometry
  transformGeometry(out);
  delete out.crs;

  return out;
}

function computeTransferCost(prevModeBase, nextModeBase) {
  // Your prompt defines transfer costs only among huge/small/train.
  // For IBX transfers, we treat IBX as "train" for transfer direction applicability.
  const from = prevModeBase === "ibx" ? "train" : prevModeBase;
  const to = nextModeBase === "ibx" ? "train" : nextModeBase;
  const key = `${from}->${to}`;
  return TRANSFER_COST_GOLD[key] ?? 0;
}

function segmentCostGold({ modeKey, lengthMiles, transferCostGold }) {
  const perMile = COST_PER_MILE_GOLD[modeKey] ?? 0;
  return perMile * lengthMiles + (transferCostGold ?? 0);
}

function createModePolylineStyle(color, isDraft = false) {
  return {
    color,
    weight: isDraft ? 4 : 5,
    opacity: isDraft ? 0.7 : 0.95
  };
}

function modeKeyFromUI(routeModeKey) {
  // current section uses base keys directly (huge/small/train)
  // ibx section uses routeModeKey to map to base keys for cost/transfer.
  if (routeModeKey === "huge_ibx") return "huge";
  if (routeModeKey === "small_ibx") return "small";
  if (routeModeKey === "ibx") return "ibx";
  return routeModeKey;
}

const ui = {
  activeStep: "locations",
  locationType: "none",
  activeRouteModeCurrent: "huge",
  activeRouteModeIbx: "ibx",
  drawing: {
    active: false,
    section: null, // "current" | "ibx"
    modeUIKey: null,
    modeBaseKey: null,
    points: [],
    // Survey geometry stored in EPSG:2263 (x/y), while `points` is the Leaflet display lat/lng.
    points2263: [],
    polyline: null,
    // Transfer costs are paid when a segment starts after a mode change.
    transferCostGoldPreview: 0,
    transferAppliedGoldPreview: false
  }
};

const state = {
  industry: { companyName: "", roleKey: "", roleOtherDetail: "" },
  locations: [],
  routes: {
    current: { segments: [], totalCostGold: 0 },
    ibx: { segments: [], totalCostGold: 0 }
  },
  ibxLine: { loaded: false }
};

function migrateLocationsFromParsed(locations) {
  let didMigrate = false;
  const out = (locations ?? []).map((loc) => {
    const has2263 = typeof loc.x === "number" && typeof loc.y === "number";
    if (has2263) return loc;

    const hasLatLng = typeof loc.lat === "number" && typeof loc.lng === "number";
    if (!hasLatLng) return loc;

    const [x, y] = gpsToEPSG2263(loc.lng, loc.lat);
    didMigrate = true;
    return {
      ...loc,
      x,
      y,
      lat: undefined,
      lng: undefined
    };
  });
  return { locations: out, didMigrate };
}

function migrateSegments(segments) {
  let didMigrate = false;
  if (!Array.isArray(segments)) return { segments: [], didMigrate: false };
  const out = segments.map((seg) => {
    if (!seg || !Array.isArray(seg.points)) return seg;
    if (seg.points.length === 0) return seg;

    const p0 = seg.points[0];
    if (Array.isArray(p0) && p0.length >= 2 && typeof p0[0] === "number" && typeof p0[1] === "number") {
      return seg;
    }

    if (p0 && typeof p0.lat === "number" && typeof p0.lng === "number") {
      didMigrate = true;
      const points2263 = seg.points.map((p) => gpsToEPSG2263(p.lng, p.lat));
      return { ...seg, points: points2263 };
    }

    return seg;
  });
  return { segments: out, didMigrate };
}

/** Applies migrated survey JSON into global `state`. */
function applySurveyPayload(parsed, options = {}) {
  const persist = options.persist !== false;
  if (!parsed || !parsed.locations || !parsed.routes) return;

  let didMigrate = false;

  const locRes = migrateLocationsFromParsed(parsed.locations);
  state.locations = locRes.locations;
  didMigrate ||= locRes.didMigrate;

  const cur = migrateSegments(parsed.routes?.current?.segments);
  const ibx = migrateSegments(parsed.routes?.ibx?.segments);
  didMigrate ||= cur.didMigrate;
  didMigrate ||= ibx.didMigrate;

  state.routes = {
    current: {
      segments: cur.segments,
      totalCostGold: parsed.routes?.current?.totalCostGold ?? 0
    },
    ibx: {
      segments: ibx.segments,
      totalCostGold: parsed.routes?.ibx?.totalCostGold ?? 0
    }
  };

  state.ibxLine = parsed.ibxLine ?? { loaded: state.ibxLine?.loaded ?? false };

  state.industry = {
    companyName: String(parsed.industry?.companyName ?? ""),
    roleKey: String(parsed.industry?.roleKey ?? ""),
    roleOtherDetail: String(parsed.industry?.roleOtherDetail ?? "")
  };

  if (didMigrate && persist) saveState();
}

function loadStateFromLocalStorage() {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return;
    const parsed = JSON.parse(raw);
    applySurveyPayload(parsed, { persist: false });
  } catch {
    // ignore
  }
}

function saveState() {
  if (SURVEY_MODE === "participant" && participantId && participantToken) {
    if (saveDebounceTimer) clearTimeout(saveDebounceTimer);
    saveDebounceTimer = setTimeout(() => {
      void flushSaveToServer();
    }, 450);
    return;
  }
  if (SURVEY_MODE === "standalone") {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
  }
}

async function flushSaveToServer() {
  if (SURVEY_MODE !== "participant" || !participantId || !participantToken) return;
  try {
    const res = await fetch(`/api/participant/${encodeURIComponent(participantId)}`, {
      method: "PUT",
      headers: {
        Authorization: `Bearer ${participantToken}`,
        "Content-Type": "application/json"
      },
      body: JSON.stringify(state)
    });
    if (!res.ok) console.warn("Save failed", res.status);
  } catch (e) {
    console.warn("Save error", e);
  }
}

function resetDraftDrawing() {
  ui.drawing.active = false;
  ui.drawing.section = null;
  ui.drawing.modeUIKey = null;
  ui.drawing.modeBaseKey = null;
  ui.drawing.points = [];
  ui.drawing.points2263 = [];
  ui.drawing.polyline = null;
  ui.drawing.transferCostGoldPreview = 0;
  ui.drawing.transferAppliedGoldPreview = false;
}

let map;
let layers;

function markerIcon(color) {
  // Colored circle via DivIcon.
  return L.divIcon({
    className: "",
    html: `<div style="width:14px;height:14px;border-radius:50%;background:${color};border:2px solid rgba(255,255,255,0.9)"></div>`,
    iconSize: [14, 14],
    iconAnchor: [7, 7]
  });
}

function getSegmentColor(section, seg) {
  const key = seg.modeKey;
  if (section === "current") {
    return key === "huge" ? "#5bbcff" : key === "small" ? "#6ee7b7" : "#ffd166";
  }
  // ibx
  if (key === "huge") return "#5bbcff";
  if (key === "small") return "#6ee7b7";
  return "#ffd166"; // ibx rail
}

function buildSegmentPopup(seg, sectionLabel) {
  const lengthStr = `${formatGold(seg.lengthMiles)} mi`;
  const transferStr = seg.transferAppliedGold
    ? `Transfer cost: ${formatGold(seg.transferCostGold)}`
    : `Transfer cost: 0 (no mode change)`;
  const segmentStr = `Segment: ${seg.modeLabel} (${formatGold(seg.costGold)} gold)`;
  const totalStr = `Total (${sectionLabel}): ${formatGold(seg.sectionTotalCostGoldAfter)}`;
  return [segmentStr, `Length: ${lengthStr}`, transferStr, totalStr].join("<br/>");
}

function rebuildFromState() {
  layers.locations.clearLayers();
  layers.currentRoutes.clearLayers();
  layers.ibxRoutes.clearLayers();

  for (const loc of state.locations) {
    const meta = LOCATION_TYPES[loc.locationType] ?? LOCATION_TYPES.other;
    const latlng =
      typeof loc.x === "number" && typeof loc.y === "number"
        ? epsg2263XYToLatLng(loc.x, loc.y)
        : L.latLng(loc.lat, loc.lng);
    const marker = L.marker(latlng, { icon: markerIcon(meta.color), draggable: false });
    const companyNote =
      loc.locationType === "workplace" && state.industry?.companyName
        ? `<br/>${escapeHtml(state.industry.companyName)}`
        : "";
    marker.bindPopup(`${meta.label}${companyNote}<br/>ID: ${loc.id}`);
    layers.locations.addLayer(marker);
  }

  for (const seg of state.routes.current.segments) {
    const latlngs = surveyPointsToLatLngs(seg.points);
    const line = L.polyline(latlngs, createModePolylineStyle(getSegmentColor("current", seg), false));
    line.bindPopup(buildSegmentPopup(seg, "Current"));
    layers.currentRoutes.addLayer(line);
  }

  for (const seg of state.routes.ibx.segments) {
    const latlngs = surveyPointsToLatLngs(seg.points);
    const line = L.polyline(latlngs, createModePolylineStyle(getSegmentColor("ibx", seg), false));
    line.bindPopup(buildSegmentPopup(seg, "IBX Assumption"));
    layers.ibxRoutes.addLayer(line);
  }

  uiUpdateStats();
}

function uiUpdateStats() {
  const set = (id, text) => {
    const el = document.getElementById(id);
    if (el) el.textContent = text;
  };
  set("locationsCount", String(state.locations.length));
  set("currentTotalGold", formatGold(state.routes.current.totalCostGold));
  set("ibxTotalGold", formatGold(state.routes.ibx.totalCostGold));
  set("exportLocationsCount", String(state.locations.length));
  set("exportCurrentSegments", String(state.routes.current.segments.length));
  set("exportIbXSegments", String(state.routes.ibx.segments.length));

  const indEl = document.getElementById("participantIndustrySummary");
  if (indEl) {
    const company = String(state.industry?.companyName ?? "").trim();
    const rk = String(state.industry?.roleKey ?? "").trim();
    let roleText = "";
    if (rk === "manager") roleText = "Manager";
    else if (rk === "worker") roleText = "Worker";
    else if (rk === "transporter") roleText = "Transporter";
    else if (rk === "other") {
      const d = String(state.industry?.roleOtherDetail ?? "").trim();
      roleText = d ? `Others (${d})` : "Others";
    }
    let text = "";
    if (company) text = `Company / industry you entered: ${company}`;
    if (roleText) text = text ? `${text} · Role: ${roleText}` : `Role: ${roleText}`;
    indEl.textContent = text;
    indEl.classList.toggle("is-hidden", !text);
  }

  const condInd = document.getElementById("conductorIndustryLabel");
  if (condInd && SURVEY_MODE === "conductor") {
    const c = String(state.industry?.companyName ?? "").trim();
    const rk = String(state.industry?.roleKey ?? "").trim();
    let roleSuffix = "";
    if (rk === "manager") roleSuffix = " · Manager";
    else if (rk === "worker") roleSuffix = " · Worker";
    else if (rk === "transporter") roleSuffix = " · Transporter";
    else if (rk === "other") {
      const d = String(state.industry?.roleOtherDetail ?? "").trim();
      roleSuffix = d ? ` · Others (${d})` : " · Others";
    }
    condInd.textContent = c ? `Industry / company: ${c}${roleSuffix}` : "";
    condInd.classList.toggle("is-hidden", !c || !viewingParticipantId);
  }

  updateParticipantCompanyBanner();
}

function updateParticipantCompanyBanner() {
  const el = document.getElementById("participantCompanyBanner");
  if (!el) return;
  const hasWorkplace = state.locations.some((l) => l.locationType === "workplace");
  const mapGatesOpen = Boolean(
    document.getElementById("industryGate")?.classList.contains("is-open") ||
      document.getElementById("roleGate")?.classList.contains("is-open")
  );
  const show =
    SURVEY_MODE === "participant" &&
    !readOnly &&
    ui.activeStep === "locations" &&
    !hasWorkplace &&
    !mapGatesOpen;
  el.classList.toggle("is-hidden", !show);
}

function setStep(step) {
  ui.activeStep = step;
  document.querySelectorAll(".tab").forEach((el) => {
    el.classList.toggle("is-active", el.dataset.step === step);
  });

  document.querySelectorAll("[data-step-panel]").forEach((panel) => {
    panel.classList.toggle("is-hidden", panel.dataset.stepPanel !== step);
  });

  const pill = document.getElementById("activeStepPill");
  const labelMap = {
    locations: "Step 1: Locations",
    currentRoutes: "Step 2: Current Distribution",
    ibxRoutes: "Step 3: IBX Assumption",
    export: "Step 4: Export"
  };
  if (pill) pill.textContent = labelMap[step] ?? "Survey";

  updateParticipantCompanyBanner();

  // Conductor review UX: show only the relevant layer(s) for the selected step.
  if (!map) return;
  if (layers?.locations) {
    if (step === "locations") {
      if (!map.hasLayer(layers.locations)) layers.locations.addTo(map);
    } else if (map.hasLayer(layers.locations)) {
      layers.locations.removeFrom(map);
    }
  }

  if (layers?.currentRoutes) {
    if (step === "currentRoutes") {
      if (!map.hasLayer(layers.currentRoutes)) layers.currentRoutes.addTo(map);
    } else if (map.hasLayer(layers.currentRoutes)) {
      layers.currentRoutes.removeFrom(map);
    }
  }

  if (layers?.ibxRoutes) {
    if (step === "ibxRoutes") {
      if (!map.hasLayer(layers.ibxRoutes)) layers.ibxRoutes.addTo(map);
    } else if (map.hasLayer(layers.ibxRoutes)) {
      layers.ibxRoutes.removeFrom(map);
    }
  }

  // IBX railway context should appear in Step 3.
  if (step === "ibxRoutes") {
    if (!map.hasLayer(layers.ibxLine)) layers.ibxLine.addTo(map);
    if (!map.hasLayer(layers.ibxStations)) layers.ibxStations.addTo(map);
  } else {
    if (map.hasLayer(layers.ibxLine)) layers.ibxLine.removeFrom(map);
    if (map.hasLayer(layers.ibxStations)) layers.ibxStations.removeFrom(map);
  }
}

function startSegment(section) {
  if (readOnly) return;
  if (ui.drawing.active) return;
  const modeUIKey = section === "current" ? ui.activeRouteModeCurrent : ui.activeRouteModeIbx;
  const modeBaseKey = modeKeyFromUI(modeUIKey);

  ui.drawing.active = true;
  ui.drawing.section = section;
  ui.drawing.modeUIKey = modeUIKey;
  ui.drawing.modeBaseKey = modeBaseKey;
  ui.drawing.points = [];
  ui.drawing.points2263 = [];
  const prevSegs = state.routes[section].segments;
  const prevModeBase = prevSegs.length > 0 ? prevSegs[prevSegs.length - 1].modeKey : null;
  ui.drawing.transferAppliedGoldPreview = !!prevModeBase && prevModeBase !== modeBaseKey;
  ui.drawing.transferCostGoldPreview = ui.drawing.transferAppliedGoldPreview
    ? computeTransferCost(prevModeBase, modeBaseKey)
    : 0;

  const color = getSegmentColor(section, { modeKey: modeBaseKey });
  ui.drawing.polyline = L.polyline([], createModePolylineStyle(color, true));
  ui.drawing.polyline.addTo(section === "current" ? layers.currentRoutes : layers.ibxRoutes);

  const finishBtn = section === "current"
    ? document.getElementById("finishSegmentCurrent")
    : document.getElementById("finishSegmentIbX");
  const undoBtn = section === "current"
    ? document.getElementById("undoPointCurrent")
    : document.getElementById("undoPointIbX");
  const cancelBtn = section === "current"
    ? document.getElementById("cancelSegmentCurrent")
    : document.getElementById("cancelSegmentIbX");

  finishBtn.disabled = false;
  undoBtn.disabled = false;
  cancelBtn.disabled = false;
  document.getElementById("mapHint").textContent = "Drawing... click to add points, then Finish segment.";
}

function cancelSegment() {
  if (readOnly) return;
  if (!ui.drawing.active) return;
  const section = ui.drawing.section; // capture before reset

  try {
    if (ui.drawing.polyline) ui.drawing.polyline.remove();
  } catch {
    // ignore
  }

  resetDraftDrawing();

  const isCurrent = section === "current";
  const finishBtn = isCurrent ? document.getElementById("finishSegmentCurrent") : document.getElementById("finishSegmentIbX");
  const undoBtn = isCurrent ? document.getElementById("undoPointCurrent") : document.getElementById("undoPointIbX");
  const cancelBtn = isCurrent ? document.getElementById("cancelSegmentCurrent") : document.getElementById("cancelSegmentIbX");
  finishBtn.disabled = true;
  undoBtn.disabled = true;
  cancelBtn.disabled = true;

  document.getElementById("mapHint").textContent = "Select a tool on the left, then click the map to add points or draw segments.";
}

function updateDraftPolyline() {
  if (!ui.drawing.polyline) return;
  ui.drawing.polyline.setLatLngs(ui.drawing.points);

  // Live draft cost preview while drawing.
  const draftLen = ui.drawing.points.length >= 2 ? lineLengthMiles(ui.drawing.points) : 0;
  const perMile = COST_PER_MILE_GOLD[ui.drawing.modeBaseKey] ?? 0;
  const draftCost = perMile * draftLen + (ui.drawing.transferCostGoldPreview ?? 0);
  document.getElementById("mapHint").textContent = `Drawing... ${formatGold(draftLen)} mi, draft cost: ${formatGold(draftCost)} gold`;
}

function undoPoint() {
  if (readOnly) return;
  if (!ui.drawing.active) return;
  if (ui.drawing.points.length === 0) return;
  ui.drawing.points.pop();
  ui.drawing.points2263.pop();
  updateDraftPolyline();
}

function finishSegment() {
  if (readOnly) return;
  if (!ui.drawing.active) return;
  if (ui.drawing.points.length < 2) return;

  const section = ui.drawing.section;
  const modeBaseKey = ui.drawing.modeBaseKey;
  const pointsDisplay = [...ui.drawing.points];
  const points2263 = [...ui.drawing.points2263];

  const lengthMiles = lineLengthMiles(pointsDisplay);
  const prevSegs = state.routes[section].segments;
  const prevModeBase = prevSegs.length > 0 ? prevSegs[prevSegs.length - 1].modeKey : null;

  const transferAppliedGold = !!prevModeBase && prevModeBase !== modeBaseKey;
  const transferCostGold = transferAppliedGold ? computeTransferCost(prevModeBase, modeBaseKey) : 0;
  const costGold = segmentCostGold({
    modeKey: modeBaseKey,
    lengthMiles,
    transferCostGold: transferCostGold
  });

  const modeLabel =
    section === "current"
      ? modeBaseKey === "huge"
        ? "Huge Truck"
        : modeBaseKey === "small"
          ? "Small Truck"
          : "Train"
      : modeBaseKey === "huge"
        ? "Huge Truck"
        : modeBaseKey === "small"
          ? "Small Truck"
          : "IBX Rail";

  const seg = {
    id: uid("seg"),
    section,
    modeUIKey: ui.drawing.modeUIKey,
    modeKey: modeBaseKey,
    modeLabel,
    // Persist survey geometry in EPSG:2263 (x/y) for correct CRS export.
    points: points2263,
    lengthMiles,
    transferAppliedGold,
    transferCostGold,
    costGold,
    sectionTotalCostGoldAfter: 0
  };

  state.routes[section].segments.push(seg);
  state.routes[section].totalCostGold += costGold;
  seg.sectionTotalCostGoldAfter = state.routes[section].totalCostGold;

  try {
    ui.drawing.polyline.setStyle(createModePolylineStyle(getSegmentColor(section, seg), false));
    ui.drawing.polyline.bindPopup(buildSegmentPopup(seg, section === "current" ? "Current" : "IBX Assumption"));
  } catch {
    // ignore
  }

  resetDraftDrawing();

  const isCurrent = section === "current";
  const finishBtn = isCurrent ? document.getElementById("finishSegmentCurrent") : document.getElementById("finishSegmentIbX");
  const undoBtn = isCurrent ? document.getElementById("undoPointCurrent") : document.getElementById("undoPointIbX");
  const cancelBtn = isCurrent ? document.getElementById("cancelSegmentCurrent") : document.getElementById("cancelSegmentIbX");
  finishBtn.disabled = true;
  undoBtn.disabled = true;
  cancelBtn.disabled = true;

  saveState();
  uiUpdateStats();
  document.getElementById("mapHint").textContent = "Select a tool on the left, then click the map to add points or draw segments.";
}

function clearRoutes(section) {
  if (readOnly) return;
  state.routes[section].segments = [];
  state.routes[section].totalCostGold = 0;
  layers[section === "current" ? "currentRoutes" : "ibxRoutes"].clearLayers();
  saveState();
  uiUpdateStats();
}

function addLocation(type, latlng) {
  if (readOnly) return;
  if (type === "none") return;
  if (type === "workplace") {
    state.locations = state.locations.filter((l) => l.locationType !== "workplace");
  }
  const [x, y] = gpsToEPSG2263(latlng.lng, latlng.lat);
  state.locations.push({
    id: uid("loc"),
    locationType: type,
    x,
    y
  });

  rebuildFromState();
  saveState();
  uiUpdateStats();
  if (SURVEY_MODE === "participant" && type === "workplace") {
    maybeOpenParticipantRoleGate();
  }
}

function clearLocations() {
  if (readOnly) return;
  state.locations = [];
  state.industry = {
    ...state.industry,
    roleKey: "",
    roleOtherDetail: ""
  };
  layers.locations.clearLayers();
  saveState();
  uiUpdateStats();
}

function loadIBXGeoJSONToLayer(geojson) {
  const normalized = normalizeIBXGeoJSONForLeaflet(geojson);
  layers.ibxLine.clearLayers();

  const layer = L.geoJSON(normalized, {
    style: {
      color: "#ff0000",
      weight: 10,
      opacity: 1
    },
    pointToLayer: (feature, latlng) => {
      return L.circleMarker(latlng, {
        radius: 15,
        color: "#ff0000",
        fillColor: "#ff0000",
        fillOpacity: 1
      });
    }
  });
  layer.addTo(layers.ibxLine);
  state.ibxLine.loaded = true;
}

function loadIBXStationsGeoJSONToLayer(geojson) {
  const normalized = normalizeIBXGeoJSONForLeaflet(geojson);
  layers.ibxStations.clearLayers();

  const layer = L.geoJSON(normalized, {
    pointToLayer: (feature, latlng) => {
      return L.circleMarker(latlng, {
        radius: 4,
        color: "#ff8c1a",
        weight: 2,
        fillColor: "#ff8c1a",
        fillOpacity: 1
      });
    }
  });
  layer.addTo(layers.ibxStations);
}

async function tryLoadGeoJSONFromFile(filename) {
  try {
    const res = await fetch(filename);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const text = await res.text();
    const parsed = JSON.parse(text);
    return parsed;
  } catch {
    return null;
  }
}

function getRoutesGeoJSON2263FromState(stateObj, extraProps = {}) {
  const features = [];
  const sections = [
    { key: "current", label: "Current" },
    { key: "ibx", label: "IBX Assumption" }
  ];

  for (const sec of sections) {
    for (const seg of stateObj.routes[sec.key].segments) {
      const coords = seg.points
        .map((p) => {
          if (Array.isArray(p) && p.length >= 2) return [p[0], p[1]];
          if (p && typeof p.lat === "number" && typeof p.lng === "number") {
            const [x, y] = gpsToEPSG2263(p.lng, p.lat);
            return [x, y];
          }
          return null;
        })
        .filter(Boolean);

      features.push({
        type: "Feature",
        properties: {
          ...extraProps,
          id: seg.id,
          section: sec.label,
          mode: seg.modeLabel,
          modeKey: seg.modeKey,
          lengthMiles: seg.lengthMiles,
          transferAppliedGold: seg.transferAppliedGold,
          transferCostGold: seg.transferCostGold,
          segmentCostGold: seg.costGold,
          sectionTotalCostGoldAfter: seg.sectionTotalCostGoldAfter
        },
        geometry: {
          type: "LineString",
          coordinates: coords
        }
      });
    }
  }

  return {
    type: "FeatureCollection",
    crs: { type: "name", properties: { name: "urn:ogc:def:crs:EPSG:2263" } },
    features
  };
}

function getRoutesGeoJSON2263() {
  return getRoutesGeoJSON2263FromState(state, {});
}

function csvEscape(s) {
  const str = String(s ?? "");
  if (/[,"\n]/.test(str)) return `"${str.replace(/"/g, '""')}"`;
  return str;
}

function exportAllDataCSVFromState(stateObj, participantMeta = null) {
  const rows = [];
  const pid = participantMeta?.id;
  const plab = participantMeta?.label ?? "";
  const withP = pid != null && pid !== "";
  const industryCo = String(stateObj.industry?.companyName ?? "");
  const industryRole = String(stateObj.industry?.roleKey ?? "");
  const industryRoleDetail = String(stateObj.industry?.roleOtherDetail ?? "");
  const baseHeader = [
    "record_type",
    "section",
    "location_type",
    "id",
    "x_2263",
    "y_2263",
    "lat",
    "lng",
    "route_mode",
    "route_mode_key",
    "length_miles",
    "transfer_applied_gold",
    "transfer_cost_gold",
    "segment_cost_gold",
    "section_total_cost_gold_after"
  ];
  rows.push(
    withP
      ? ["participant_id", "participant_label", "industry_company", "industry_role", "industry_role_detail", ...baseHeader]
      : baseHeader
  );

  const rowPrefix = withP ? [pid, plab, industryCo, industryRole, industryRoleDetail] : [];

  for (const loc of stateObj.locations) {
    const has2263 = typeof loc.x === "number" && typeof loc.y === "number";
    const [x2263, y2263] = has2263 ? [loc.x, loc.y] : gpsToEPSG2263(loc.lng, loc.lat);
    const latlng = has2263 ? epsg2263XYToLatLng(loc.x, loc.y) : L.latLng(loc.lat, loc.lng);
    rows.push([
      ...rowPrefix,
      "location",
      "",
      loc.locationType,
      loc.id,
      x2263,
      y2263,
      latlng.lat,
      latlng.lng,
      "",
      "",
      "",
      "",
      "",
      "",
      ""
    ]);
  }

  for (const seg of stateObj.routes.current.segments) {
    rows.push([
      ...rowPrefix,
      "route_segment",
      "Current",
      "",
      seg.id,
      "",
      "",
      "",
      "",
      seg.modeLabel,
      seg.modeKey,
      seg.lengthMiles,
      seg.transferAppliedGold,
      seg.transferCostGold,
      seg.costGold,
      seg.sectionTotalCostGoldAfter
    ]);
  }

  for (const seg of stateObj.routes.ibx.segments) {
    rows.push([
      ...rowPrefix,
      "route_segment",
      "IBX Assumption",
      "",
      seg.id,
      "",
      "",
      "",
      "",
      seg.modeLabel,
      seg.modeKey,
      seg.lengthMiles,
      seg.transferAppliedGold,
      seg.transferCostGold,
      seg.costGold,
      seg.sectionTotalCostGoldAfter
    ]);
  }

  return rows.map((r) => r.map((cell) => csvEscape(cell)).join(",")).join("\n");
}

function exportAllDataCSV() {
  return exportAllDataCSVFromState(state, null);
}

function exportCSV() {
  if (SURVEY_MODE === "conductor") {
    if (!viewingParticipantId) {
      window.alert("Select a participant from the list first.");
      return;
    }
    const csv = exportAllDataCSVFromState(state, {
      id: viewingParticipantId,
      label: viewingParticipantLabel
    });
    const ts = new Date().toISOString().slice(0, 19).replace(/[:T]/g, "-");
    const safe = String(viewingParticipantLabel || "participant").replace(/[^\w\-]+/g, "_");
    downloadFile(`nyc_supply_survey_${safe}_${ts}.csv`, csv, "text/csv;charset=utf-8");
    return;
  }
  const csv = exportAllDataCSVFromState(state, null);
  const ts = new Date().toISOString().slice(0, 19).replace(/[:T]/g, "-");
  downloadFile(`nyc_supply_survey_all_data_${ts}.csv`, csv, "text/csv;charset=utf-8");
}

function exportRoutesGeoJSON() {
  if (SURVEY_MODE === "conductor") {
    if (!viewingParticipantId) {
      window.alert("Select a participant from the list first.");
      return;
    }
    const geojson = getRoutesGeoJSON2263FromState(state, {
      participant_id: viewingParticipantId,
      participant_label: viewingParticipantLabel
    });
    const ts = new Date().toISOString().slice(0, 19).replace(/[:T]/g, "-");
    const safe = String(viewingParticipantLabel || "participant").replace(/[^\w\-]+/g, "_");
    downloadFile(
      `nyc_supply_survey_routes_${safe}_${ts}.geojson`,
      JSON.stringify(geojson, null, 2),
      "application/geo+json;charset=utf-8"
    );
    return;
  }
  const geojson = getRoutesGeoJSON2263();
  const ts = new Date().toISOString().slice(0, 19).replace(/[:T]/g, "-");
  downloadFile(
    `nyc_supply_survey_routes_EPSG2263_${ts}.geojson`,
    JSON.stringify(geojson, null, 2),
    "application/geo+json;charset=utf-8"
  );
}

async function exportAllParticipantsCSV() {
  const res = await fetch("/api/conductor/participants", {
    headers: {}
  });
  if (!res.ok) {
    window.alert("Could not load participants.");
    return;
  }
  const list = await res.json();
  const headerRow =
    "participant_id,participant_label,industry_company,industry_role,industry_role_detail,record_type,section,location_type,id,x_2263,y_2263,lat,lng,route_mode,route_mode_key,length_miles,transfer_applied_gold,transfer_cost_gold,segment_cost_gold,section_total_cost_gold_after";
  const chunks = [headerRow];
  for (const p of list) {
    const r = await fetch(`/api/conductor/participants/${encodeURIComponent(p.id)}`);
    if (!r.ok) continue;
    const data = await r.json();
    const body = exportAllDataCSVFromState(data.state, { id: data.id, label: data.label });
    const lines = body.split("\n");
    chunks.push(...lines.slice(1));
  }
  const ts = new Date().toISOString().slice(0, 19).replace(/[:T]/g, "-");
  downloadFile(`nyc_supply_survey_all_participants_${ts}.csv`, chunks.join("\n"), "text/csv;charset=utf-8");
}

async function exportAllParticipantsGeoJSON() {
  const res = await fetch("/api/conductor/participants", {
    headers: {}
  });
  if (!res.ok) {
    window.alert("Could not load participants.");
    return;
  }
  const list = await res.json();
  const allFeatures = [];
  for (const p of list) {
    const r = await fetch(`/api/conductor/participants/${encodeURIComponent(p.id)}`);
    if (!r.ok) continue;
    const data = await r.json();
    const fc = getRoutesGeoJSON2263FromState(data.state, {
      participant_id: data.id,
      participant_label: data.label
    });
    allFeatures.push(...fc.features);
  }
  const out = {
    type: "FeatureCollection",
    crs: { type: "name", properties: { name: "urn:ogc:def:crs:EPSG:2263" } },
    features: allFeatures
  };
  const ts = new Date().toISOString().slice(0, 19).replace(/[:T]/g, "-");
  downloadFile(
    `nyc_supply_survey_all_participants_routes_${ts}.geojson`,
    JSON.stringify(out, null, 2),
    "application/geo+json;charset=utf-8"
  );
}

function initIBXPlaceholderLine() {
  // Placeholder line so the IBX panel has something visible.
  // Replace by uploading real IBX railway GeoJSON (recommended).
  const placeholder = {
    type: "FeatureCollection",
    features: [
      {
        type: "Feature",
        properties: { name: "IBX (placeholder)" },
        geometry: {
          type: "LineString",
          coordinates: [
            [-73.985664, 40.753182],
            [-73.973121, 40.768094],
            [-73.958121, 40.781],
            [-73.945121, 40.7955]
          ]
        }
      }
    ]
  };
  loadIBXGeoJSONToLayer(placeholder);
}

function setupUI() {
  document.querySelectorAll(".tab").forEach((btn) => {
    btn.addEventListener("click", () => setStep(btn.dataset.step));
  });

  // Location mode buttons
  document.querySelectorAll("[data-location-type]").forEach((btn) => {
    btn.addEventListener("click", () => {
      ui.locationType = btn.dataset.locationType;
      document.getElementById("mapHint").textContent =
        ui.locationType === "none"
          ? "No placement mode selected. Switch modes to place points."
          : `Placement: click the map to add a ${
              LOCATION_TYPES[ui.locationType]?.label ?? ui.locationType
            } point.`;
    });
  });

  document.getElementById("clearLocations")?.addEventListener("click", clearLocations);

  // Current route mode buttons
  document.querySelectorAll('[data-route-mode="huge"]').forEach((b) => b.addEventListener("click", () => (ui.activeRouteModeCurrent = "huge")));
  document.querySelectorAll('[data-route-mode="small"]').forEach((b) => b.addEventListener("click", () => (ui.activeRouteModeCurrent = "small")));
  document.querySelectorAll('[data-route-mode="train"]').forEach((b) => b.addEventListener("click", () => (ui.activeRouteModeCurrent = "train")));

  document.getElementById("startSegmentCurrent")?.addEventListener("click", () => startSegment("current"));
  document.getElementById("finishSegmentCurrent")?.addEventListener("click", finishSegment);
  document.getElementById("undoPointCurrent")?.addEventListener("click", undoPoint);
  document.getElementById("cancelSegmentCurrent")?.addEventListener("click", cancelSegment);
  document.getElementById("clearCurrentRoutes")?.addEventListener("click", () => clearRoutes("current"));

  // IBX route mode buttons
  document.querySelectorAll('[data-route-mode="huge_ibx"]').forEach((b) => b.addEventListener("click", () => (ui.activeRouteModeIbx = "huge_ibx")));
  document.querySelectorAll('[data-route-mode="small_ibx"]').forEach((b) => b.addEventListener("click", () => (ui.activeRouteModeIbx = "small_ibx")));
  document.querySelectorAll('[data-route-mode="ibx"]').forEach((b) => b.addEventListener("click", () => (ui.activeRouteModeIbx = "ibx")));

  document.getElementById("startSegmentIbX")?.addEventListener("click", () => startSegment("ibx"));
  document.getElementById("finishSegmentIbX")?.addEventListener("click", finishSegment);
  document.getElementById("undoPointIbX")?.addEventListener("click", undoPoint);
  document.getElementById("cancelSegmentIbX")?.addEventListener("click", cancelSegment);
  document.getElementById("clearIbxRoutes")?.addEventListener("click", () => clearRoutes("ibx"));

  document.getElementById("exportCSV")?.addEventListener("click", exportCSV);
  document.getElementById("exportRoutesGeoJSON")?.addEventListener("click", exportRoutesGeoJSON);
  document.getElementById("exportAllParticipantsCSV")?.addEventListener("click", () => void exportAllParticipantsCSV());
  document.getElementById("exportAllParticipantsGeoJSON")?.addEventListener("click", () => void exportAllParticipantsGeoJSON());
}

function setupMap() {
  if (map) return;
  map = L.map("map", { zoomControl: true }).setView([40.7128, -74.006], 11);

  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a>'
  }).addTo(map);

  layers = {
    locations: L.layerGroup(),
    currentRoutes: L.layerGroup(),
    ibxLine: L.layerGroup(),
    ibxStations: L.layerGroup(),
    ibxRoutes: L.layerGroup()
  };

  layers.locations.addTo(map);
  layers.currentRoutes.addTo(map);
  layers.ibxRoutes.addTo(map);
  // ibxLine is only shown when user enters Step 3 (handled in setStep()).

  map.on("click", (e) => {
    if (readOnly) return;
    if (ui.drawing.active) {
      ui.drawing.points.push(e.latlng);
      const [x, y] = gpsToEPSG2263(e.latlng.lng, e.latlng.lat);
      ui.drawing.points2263.push([x, y]);
      updateDraftPolyline();
      return;
    }

    if (ui.locationType !== "none") addLocation(ui.locationType, e.latlng);
  });
}

async function loadIBXAssets() {
  const [lineGeojson, stationsGeojson] = await Promise.all([
    tryLoadGeoJSONFromFile("./IBX_Line_4326.geojson"),
    tryLoadGeoJSONFromFile("./IBX_Stations.geojson")
  ]);

  if (lineGeojson) loadIBXGeoJSONToLayer(lineGeojson);
  else initIBXPlaceholderLine();
  if (stationsGeojson) loadIBXStationsGeoJSONToLayer(stationsGeojson);
}

function setParticipantMapHintAfterIndustryGate() {
  const hasWorkplace = state.locations.some((l) => l.locationType === "workplace");
  const hintEl = document.getElementById("mapHint");
  if (!hintEl) return;
  if (!hasWorkplace) {
    ui.locationType = "workplace";
    hintEl.textContent =
      "Workplace (your company) is selected. Click the map once to mark where that company is located. You can switch modes below to add other site types.";
  } else {
    ui.locationType = "none";
    hintEl.textContent =
      "Select a location type on the left, then click the map to add points or draw routes on later steps.";
  }
}

function syncParticipantMapGateOverlay() {
  if (SURVEY_MODE !== "participant") return;
  const mapWrap = document.querySelector("#app .mapWrap");
  if (!mapWrap) return;
  const ind = document.getElementById("industryGate");
  const role = document.getElementById("roleGate");
  const anyOpen = Boolean(
    ind?.classList.contains("is-open") || role?.classList.contains("is-open")
  );
  mapWrap.classList.toggle("map-gate-open", anyOpen);
  if (anyOpen) document.body.dataset.mapGateOpen = "1";
  else delete document.body.dataset.mapGateOpen;
  updateParticipantCompanyBanner();
}

function participantHasWorkplaceLocation() {
  return state.locations.some((l) => l.locationType === "workplace");
}

const PARTICIPANT_ROLE_KEYS = new Set(["manager", "worker", "transporter", "other"]);

function participantIndustryRoleIsComplete() {
  const k = String(state.industry?.roleKey ?? "").trim();
  if (!k || !PARTICIPANT_ROLE_KEYS.has(k)) return false;
  if (k === "other") return String(state.industry?.roleOtherDetail ?? "").trim().length > 0;
  return true;
}

function participantNeedsRoleGate() {
  return (
    SURVEY_MODE === "participant" &&
    participantHasWorkplaceLocation() &&
    !participantIndustryRoleIsComplete()
  );
}

function openParticipantRoleGate() {
  const gate = document.getElementById("roleGate");
  if (!gate) return;
  gate.classList.add("is-open");
  syncParticipantMapGateOverlay();

  const otherWrap = document.getElementById("roleOtherWrap");
  const otherInput = document.getElementById("roleOtherInput");
  const k = String(state.industry?.roleKey ?? "").trim();

  document.querySelectorAll('input[name="participantRole"]').forEach((r) => {
    r.checked = k !== "" && r.value === k;
  });

  if (k === "other") {
    otherWrap?.classList.remove("is-hidden");
    if (otherInput) otherInput.value = String(state.industry?.roleOtherDetail ?? "");
  } else {
    otherWrap?.classList.add("is-hidden");
    if (otherInput) otherInput.value = "";
  }

  requestAnimationFrame(() => {
    document.querySelector('input[name="participantRole"]')?.focus();
  });
}

function initParticipantRoleGateOnce() {
  if (SURVEY_MODE !== "participant" || document.body.dataset.participantRoleGateBound === "1") return;
  const gate = document.getElementById("roleGate");
  if (!gate) return;
  document.body.dataset.participantRoleGateBound = "1";

  const btn = document.getElementById("roleContinueBtn");
  const otherWrap = document.getElementById("roleOtherWrap");
  const otherInput = document.getElementById("roleOtherInput");

  document.querySelectorAll('input[name="participantRole"]').forEach((radio) => {
    radio.addEventListener("change", () => {
      const show = radio.value === "other" && radio.checked;
      otherWrap?.classList.toggle("is-hidden", !show);
      if (show) otherInput?.focus();
    });
  });

  otherInput?.addEventListener("keydown", (e) => {
    if (e.key === "Enter") btn?.click();
  });

  btn?.addEventListener("click", () => {
    const sel = document.querySelector('input[name="participantRole"]:checked');
    if (!sel) {
      window.alert("Please select your role.");
      return;
    }
    let detail = "";
    if (sel.value === "other") {
      detail = String(otherInput?.value ?? "").trim();
      if (!detail) {
        window.alert("Please describe your role.");
        return;
      }
    }
    state.industry = {
      ...state.industry,
      roleKey: sel.value,
      roleOtherDetail: sel.value === "other" ? detail : ""
    };
    gate.classList.remove("is-open");
    syncParticipantMapGateOverlay();
    void flushSaveToServer();
    rebuildFromState();
    uiUpdateStats();
    if (map) {
      requestAnimationFrame(() => map.invalidateSize());
      setTimeout(() => map.invalidateSize(), 200);
    }
  });
}

function maybeOpenParticipantRoleGate() {
  if (participantNeedsRoleGate()) openParticipantRoleGate();
}

async function finishParticipantBoot() {
  setupUI();
  setupMap();
  rebuildFromState();
  await loadIBXAssets();
  if (map) {
    requestAnimationFrame(() => map.invalidateSize());
    setTimeout(() => map.invalidateSize(), 200);
  }

  setStep("locations");
  uiUpdateStats();
  setParticipantMapHintAfterIndustryGate();
  initParticipantRoleGateOnce();

  if (!document.body.dataset.participantPagehideBound) {
    document.body.dataset.participantPagehideBound = "1";
    window.addEventListener("pagehide", () => {
      if (SURVEY_MODE !== "participant" || !participantId || !participantToken) return;
      if (saveDebounceTimer) clearTimeout(saveDebounceTimer);
      try {
        void fetch(`/api/participant/${encodeURIComponent(participantId)}`, {
          method: "PUT",
          headers: {
            Authorization: `Bearer ${participantToken}`,
            "Content-Type": "application/json"
          },
          body: JSON.stringify(state),
          keepalive: true
        });
      } catch {
        // ignore
      }
    });
  }
}

async function bootParticipant() {
  const params = new URLSearchParams(location.search);
  const id = params.get("participant") || "";
  const token = params.get("token") || "";
  if (!id || !token) {
    return;
  }

  if (sessionStorage.getItem("participantWelcomeConfirmed") !== "1") {
    const btn = document.getElementById("welcomeConfirmBtn");
    if (btn && !btn.dataset.bound) {
      btn.dataset.bound = "1";
      btn.addEventListener("click", () => {
        sessionStorage.setItem("participantWelcomeConfirmed", "1");
        document.documentElement.classList.add("participant-welcome-ok");
        const gate = document.getElementById("welcomeGate");
        if (gate) gate.style.display = "none";
        void bootParticipant();
      });
    }
    return;
  }

  document.querySelector('[data-step="export"]')?.classList.add("is-hidden");
  document.querySelector('[data-step-panel="export"]')?.classList.add("is-hidden");

  participantId = id;
  participantToken = token;

  ensureProjDefs();
  let loadedPayload = null;
  try {
    const res = await fetch(`/api/participant/${encodeURIComponent(id)}`, {
      headers: { Authorization: `Bearer ${token}` }
    });
    if (!res.ok) throw new Error("bad status");
    loadedPayload = await res.json();
    applySurveyPayload(loadedPayload, { persist: false });
  } catch {
    document.documentElement.classList.remove("participant-token-ok");
    window.alert("Could not load your survey. Check your link or contact the facilitator.");
    return;
  }

  await finishParticipantBoot();

  const hasIndustryField = Object.prototype.hasOwnProperty.call(loadedPayload, "industry");
  const hasSurveyProgress =
    (state.locations?.length ?? 0) > 0 ||
    (state.routes?.current?.segments?.length ?? 0) > 0 ||
    (state.routes?.ibx?.segments?.length ?? 0) > 0;
  const needsIndustry =
    hasIndustryField &&
    !String(state.industry?.companyName ?? "").trim() &&
    !hasSurveyProgress;
  if (needsIndustry) {
    const gate = document.getElementById("industryGate");
    const input = document.getElementById("industryCompanyInput");
    const btn = document.getElementById("industryContinueBtn");
    const submit = () => {
      const name = String(input?.value ?? "").trim();
      if (!name) {
        window.alert("Please enter the industry or company you work in.");
        return;
      }
      state.industry = { ...state.industry, companyName: name };
      gate?.classList.remove("is-open");
      syncParticipantMapGateOverlay();
      void flushSaveToServer();
      rebuildFromState();
      uiUpdateStats();
      if (map) {
        requestAnimationFrame(() => map.invalidateSize());
        setTimeout(() => map.invalidateSize(), 200);
      }
    };
    if (btn) btn.onclick = submit;
    if (input) {
      input.onkeydown = (e) => {
        if (e.key === "Enter") submit();
      };
      input.value = "";
    }
    gate?.classList.add("is-open");
    syncParticipantMapGateOverlay();
    if (input) requestAnimationFrame(() => input.focus());
    return;
  }

  if (participantNeedsRoleGate()) {
    openParticipantRoleGate();
  }
}

let conductorInitialized = false;
let conductorParticipantsCache = [];

function matchesParticipantQuery(p, query) {
  const q = String(query || "").trim().toLowerCase();
  if (!q) return true;
  const label = String(p?.label ?? "").toLowerCase();
  const id = String(p?.id ?? "").toLowerCase();
  const ind = String(p?.industryCompany ?? "").toLowerCase();
  return label.includes(q) || id.includes(q) || ind.includes(q);
}

function renderFilteredParticipantList() {
  const ul = document.getElementById("participantList");
  const search = document.getElementById("participantSearch");
  if (!ul) return;
  const q = String(search?.value ?? "");
  const filtered = conductorParticipantsCache.filter((p) => matchesParticipantQuery(p, q));
  populateParticipantDeleteList(ul, filtered, viewingParticipantId);
}

function populateParticipantDeleteList(ul, list, currentId) {
  ul.innerHTML = "";
  for (const p of list) {
    const li = document.createElement("li");
    const row = document.createElement("div");
    row.className = "participantRow";

    const selectBtn = document.createElement("button");
    selectBtn.type = "button";
    selectBtn.className = "participantRow__select";
    if (p.id === currentId) selectBtn.classList.add("is-active");
    const t = new Date(p.updatedAt);
    const ind = p.industryCompany ? ` · ${p.industryCompany}` : "";
    selectBtn.textContent = `${p.label} · ${t.toLocaleString()} · ${p.counts.locations} loc / ${p.counts.currentSegments + p.counts.ibxSegments} seg${ind}`;
    selectBtn.addEventListener("click", () => void selectConductorParticipant(p.id));

    const delBtn = document.createElement("button");
    delBtn.type = "button";
    delBtn.className = "dangerBtn participantRow__delete";
    delBtn.textContent = "Delete";
    delBtn.addEventListener("click", (e) => {
      e.preventDefault();
      e.stopPropagation();
      void deleteConductorParticipant(p);
    });

    row.appendChild(selectBtn);
    row.appendChild(delBtn);
    li.appendChild(row);
    ul.appendChild(li);
  }
}

async function deleteConductorParticipant(p) {
  const label = String(p?.label ?? "this participant");
  const ok = window.confirm(`Delete "${label}"?\n\nThis will permanently remove the participant and their survey data.`);
  if (!ok) return;

  const res = await fetch(`/api/conductor/participants/${encodeURIComponent(p.id)}`, {
    method: "DELETE"
  });
  if (!res.ok) {
    window.alert("Could not delete that participant.");
    return;
  }

  if (viewingParticipantId === p.id) {
    viewingParticipantId = null;
    viewingParticipantLabel = "";
    state.industry = { companyName: "", roleKey: "", roleOtherDetail: "" };
    state.locations = [];
    state.routes = {
      current: { segments: [], totalCostGold: 0 },
      ibx: { segments: [], totalCostGold: 0 }
    };
    rebuildFromState();
    uiUpdateStats();
    const lbl = document.getElementById("conductorViewingLabel");
    if (lbl) lbl.textContent = "";
  }

  await refreshParticipantList();
}

/**
 * Verifies stored conductor secret against the server.
 * @returns {{ ok: true } | { ok: false, code: string, message?: string }}
 */
async function verifyConductorSession() {
  const raw = sessionStorage.getItem("conductorSecret");
  const s = raw ? String(raw).trim().replace(/[\r\n]/g, "") : "";
  if (!s) return { ok: false, code: "empty" };
  try {
    const res = await fetch("/api/conductor/participants", {
      headers: { Authorization: `Bearer ${s}` }
    });
    if (res.status === 503) {
      sessionStorage.removeItem("conductorSecret");
      return { ok: false, code: "no_server_secret" };
    }
    if (!res.ok) {
      sessionStorage.removeItem("conductorSecret");
      return { ok: false, code: "unauthorized" };
    }
    return { ok: true };
  } catch (e) {
    sessionStorage.removeItem("conductorSecret");
    return { ok: false, code: "network", message: e?.message || String(e) };
  }
}

function setConductorLoginStatus(message, kind) {
  const el = document.getElementById("conductorLoginStatus");
  if (!el) return;
  el.textContent = message || "";
  el.classList.toggle("conductorLogin__status--ok", kind === "ok");
}

function alertConductorFailure(result) {
  if (result.ok) return;
  const inline = {
    empty: "Enter the secret above, then try again.",
    unauthorized:
      "Does not match the server. Your .env line must be exactly CONDUCTOR_SECRET= and the same characters (capitalization matters). If the server was restarted after editing .env, type that value here.",
    no_server_secret: "Server has no CONDUCTOR_SECRET. Add it to .env and restart npm start.",
    network: `Cannot reach the server: ${result.message || "network error"}. Is npm start running?`
  };
  setConductorLoginStatus(inline[result.code] ?? `Unlock failed (${result.code}).`, null);
  if (result.code === "network" || result.code === "no_server_secret") {
    window.alert(
      result.code === "no_server_secret"
        ? "The server has no CONDUCTOR_SECRET. Set CONDUCTOR_SECRET in .env and restart npm start."
        : `Could not reach the survey server.\n\n${result.message || ""}`
    );
  }
}

function setupParticipantFilterToggle() {
  const btn = document.getElementById("participantFilterToggle");
  const panel = document.getElementById("participantFilterPanel");
  const label = document.getElementById("participantFilterToggleLabel");
  if (!btn || !panel || btn.dataset.bound) return;
  btn.dataset.bound = "1";

  const sync = () => {
    const open = !panel.classList.contains("is-hidden");
    btn.setAttribute("aria-expanded", String(open));
    btn.classList.toggle("participantFilterToggle--open", open);
    if (label) label.textContent = open ? "Hide participant filter" : "Show participant filter";
  };

  btn.addEventListener("click", () => {
    panel.classList.toggle("is-hidden");
    sync();
    if (map && !panel.classList.contains("is-hidden")) {
      requestAnimationFrame(() => {
        map.invalidateSize();
        setTimeout(() => map.invalidateSize(), 200);
      });
    }
  });

  sync();
}

async function refreshParticipantList() {
  const search = document.getElementById("participantSearch");
  const ul = document.getElementById("participantList");
  if (!ul) return;
  const res = await fetch("/api/conductor/participants");
  if (!res.ok) return;
  const list = await res.json();
  conductorParticipantsCache = Array.isArray(list) ? list : [];

  renderFilteredParticipantList();
  setupParticipantFilterToggle();

  if (search && !search.dataset.bound) {
    search.dataset.bound = "1";
    search.addEventListener("input", () => renderFilteredParticipantList());
    search.addEventListener("keydown", (e) => {
      if (e.key !== "Enter") return;
      const query = String(search.value || "");
      const filtered = conductorParticipantsCache.filter((p) => matchesParticipantQuery(p, query));
      if (filtered.length > 0) void selectConductorParticipant(filtered[0].id);
    });
  }

  if (!viewingParticipantId && conductorParticipantsCache.length > 0) {
    void selectConductorParticipant(conductorParticipantsCache[0].id);
  }
}

async function selectConductorParticipant(id) {
  const res = await fetch(`/api/conductor/participants/${encodeURIComponent(id)}`);
  if (!res.ok) {
    window.alert("Could not load that participant.");
    return;
  }
  const data = await res.json();
  viewingParticipantId = data.id;
  viewingParticipantLabel = data.label;
  applySurveyPayload(data.state, { persist: false });
  rebuildFromState();
  uiUpdateStats();
  document.getElementById("conductorViewingLabel").textContent = data.label;
  renderFilteredParticipantList();
  setStep("locations");
}

function setupConductorCreateLink() {
  document.getElementById("createParticipantBtn")?.addEventListener("click", async () => {
    const labelIn = document.getElementById("newParticipantLabel");
    const label = String(labelIn?.value ?? "").trim() || "Participant";
    const res = await fetch("/api/participants", {
      method: "POST",
      headers: {
        "Content-Type": "application/json"
      },
      body: JSON.stringify({ label })
    });
    if (!res.ok) {
      window.alert("Could not create link.");
      return;
    }
    const data = await res.json();
    const out = document.getElementById("shareUrlOutput");
    if (out) {
      out.value = data.shareUrl;
      out.classList.remove("is-hidden");
    }
    labelIn.value = "";
    await refreshParticipantList();
  });
}

async function initConductorSurveyUi() {
  if (conductorInitialized) return;
  conductorInitialized = true;
  document.getElementById("app")?.classList.add("app--readonly");
  setupUI();
  setupMap();
  rebuildFromState();
  await loadIBXAssets();
  setStep("locations");
  uiUpdateStats();
  const hint = document.getElementById("mapHint");
  if (hint) {
    hint.textContent =
      "Open “Show participant filter” to search and select a participant, then use tabs 1–3 to review their map (read-only).";
  }
  setupConductorCreateLink();
  // Map was created after #app became visible; still refresh tile/layout after flex settles.
  if (map) {
    requestAnimationFrame(() => {
      map.invalidateSize();
    });
    setTimeout(() => map.invalidateSize(), 200);
  }
}

async function showConductorDashboard(loginEl, workspace) {
  loginEl?.classList.add("is-hidden");
  if (loginEl) loginEl.hidden = true;
  workspace?.classList.remove("is-hidden");
  if (workspace) workspace.hidden = false;
  setConductorLoginStatus("", null);
  try {
    await initConductorSurveyUi();
    await refreshParticipantList();
  } catch (e) {
    console.error(e);
    conductorInitialized = false;
    loginEl?.classList.remove("is-hidden");
    if (loginEl) loginEl.hidden = false;
    workspace?.classList.add("is-hidden");
    if (workspace) workspace.hidden = true;
    window.alert(
      `The dashboard failed to load: ${e?.message || e}\n\nOpen the browser console (F12) for details. If you see errors from "content.js" or extensions, try Incognito mode or disable extensions.`
    );
  }
}

async function bootConductor() {
  readOnly = true;
  ensureProjDefs();
  await initConductorSurveyUi();
  await refreshParticipantList();
}

async function bootStandalone() {
  ensureProjDefs();
  loadStateFromLocalStorage();
  setupUI();
  setupMap();
  rebuildFromState();
  await loadIBXAssets();
  setStep("locations");
  uiUpdateStats();
  document.getElementById("mapHint").textContent =
    "Select a tool on the left, then click the map to add points or draw segments.";
}

if (SURVEY_MODE === "participant") void bootParticipant();
else if (SURVEY_MODE === "conductor") void bootConductor();
else bootStandalone();


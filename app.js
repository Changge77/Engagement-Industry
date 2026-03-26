/* global L, turf, proj4 */

const STORAGE_KEY = "nyc_supply_survey_v1";

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
  locations: [],
  routes: {
    current: { segments: [], totalCostGold: 0 },
    ibx: { segments: [], totalCostGold: 0 }
  },
  ibxLine: { loaded: false }
};

function loadState() {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return;
    const parsed = JSON.parse(raw);
    if (parsed && parsed.locations && parsed.routes) {
      let didMigrate = false;

      state.locations = (parsed.locations ?? []).map((loc) => {
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
          // Remove legacy lat/lng to keep the app consistently EPSG:2263.
          lat: undefined,
          lng: undefined
        };
      });

      const migrateSegments = (segments) => {
        if (!Array.isArray(segments)) return [];
        return segments.map((seg) => {
          if (!seg || !Array.isArray(seg.points)) return seg;
          if (seg.points.length === 0) return seg;

          const p0 = seg.points[0];
          // New schema: points are [x,y] arrays.
          if (Array.isArray(p0) && p0.length >= 2 && typeof p0[0] === "number" && typeof p0[1] === "number") {
            return seg;
          }

          // Legacy schema: points are Leaflet lat/lng objects.
          if (p0 && typeof p0.lat === "number" && typeof p0.lng === "number") {
            didMigrate = true;
            const points2263 = seg.points.map((p) => gpsToEPSG2263(p.lng, p.lat));
            return { ...seg, points: points2263 };
          }

          return seg;
        });
      };

      const migratedRoutes = {
        current: {
          segments: migrateSegments(parsed.routes?.current?.segments),
          totalCostGold: parsed.routes?.current?.totalCostGold ?? 0
        },
        ibx: {
          segments: migrateSegments(parsed.routes?.ibx?.segments),
          totalCostGold: parsed.routes?.ibx?.totalCostGold ?? 0
        }
      };

      state.routes = migratedRoutes;

      if (didMigrate) saveState();
    }
  } catch {
    // ignore
  }
}

function saveState() {
  localStorage.setItem(STORAGE_KEY, JSON.stringify(state));
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
    marker.bindPopup(`${meta.label}<br/>ID: ${loc.id}`);
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
  document.getElementById("locationsCount").textContent = String(state.locations.length);
  document.getElementById("currentTotalGold").textContent = formatGold(state.routes.current.totalCostGold);
  document.getElementById("ibxTotalGold").textContent = formatGold(state.routes.ibx.totalCostGold);

  document.getElementById("exportLocationsCount").textContent = String(state.locations.length);
  document.getElementById("exportCurrentSegments").textContent = String(state.routes.current.segments.length);
  document.getElementById("exportIbXSegments").textContent = String(state.routes.ibx.segments.length);
}

function setStep(step) {
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
  pill.textContent = labelMap[step] ?? "Survey";

  // IBX railway highlight should appear in Step 3.
  if (step === "ibxRoutes") {
    if (!map.hasLayer(layers.ibxLine)) layers.ibxLine.addTo(map);
    if (!map.hasLayer(layers.ibxStations)) layers.ibxStations.addTo(map);
  } else {
    if (map.hasLayer(layers.ibxLine)) layers.ibxLine.removeFrom(map);
    if (map.hasLayer(layers.ibxStations)) layers.ibxStations.removeFrom(map);
  }
}

function startSegment(section) {
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
  if (!ui.drawing.active) return;
  if (ui.drawing.points.length === 0) return;
  ui.drawing.points.pop();
  ui.drawing.points2263.pop();
  updateDraftPolyline();
}

function finishSegment() {
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
  state.routes[section].segments = [];
  state.routes[section].totalCostGold = 0;
  layers[section === "current" ? "currentRoutes" : "ibxRoutes"].clearLayers();
  saveState();
  uiUpdateStats();
}

function addLocation(type, latlng) {
  if (type === "none") return;
  const [x, y] = gpsToEPSG2263(latlng.lng, latlng.lat);
  state.locations.push({
    id: uid("loc"),
    locationType: type,
    x,
    y
  });

  const meta = LOCATION_TYPES[type] ?? LOCATION_TYPES.other;
  const marker = L.marker([latlng.lat, latlng.lng], { icon: markerIcon(meta.color), draggable: false });
  marker.bindPopup(`${meta.label}<br/>ID: ${state.locations[state.locations.length - 1].id}`);
  layers.locations.addLayer(marker);

  saveState();
  uiUpdateStats();
}

function clearLocations() {
  state.locations = [];
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

function getRoutesGeoJSON2263() {
  const features = [];
  const sections = [
    { key: "current", label: "Current" },
    { key: "ibx", label: "IBX Assumption" }
  ];

  for (const sec of sections) {
    for (const seg of state.routes[sec.key].segments) {
      const coords = seg.points
        .map((p) => {
          // New schema: points are EPSG:2263 [x,y]
          if (Array.isArray(p) && p.length >= 2) return [p[0], p[1]];
          // Legacy schema: points were Leaflet lat/lng objects
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

function csvEscape(s) {
  const str = String(s ?? "");
  if (/[,"\n]/.test(str)) return `"${str.replace(/"/g, '""')}"`;
  return str;
}

function exportAllDataCSV() {
  const rows = [];
  rows.push([
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
  ]);

  for (const loc of state.locations) {
    const has2263 = typeof loc.x === "number" && typeof loc.y === "number";
    const [x2263, y2263] = has2263 ? [loc.x, loc.y] : gpsToEPSG2263(loc.lng, loc.lat);
    const latlng = has2263 ? epsg2263XYToLatLng(loc.x, loc.y) : L.latLng(loc.lat, loc.lng);
    rows.push([
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

  for (const seg of state.routes.current.segments) {
    rows.push([
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

  for (const seg of state.routes.ibx.segments) {
    rows.push([
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

function exportCSV() {
  const csv = exportAllDataCSV();
  const ts = new Date().toISOString().slice(0, 19).replace(/[:T]/g, "-");
  downloadFile(
    `nyc_supply_survey_all_data_${ts}.csv`,
    csv,
    "text/csv;charset=utf-8"
  );
}

function exportRoutesGeoJSON() {
  const geojson = getRoutesGeoJSON2263();
  const ts = new Date().toISOString().slice(0, 19).replace(/[:T]/g, "-");
  downloadFile(
    `nyc_supply_survey_routes_EPSG2263_${ts}.geojson`,
    JSON.stringify(geojson, null, 2),
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

  document.getElementById("clearLocations").addEventListener("click", clearLocations);

  // Current route mode buttons
  document.querySelectorAll('[data-route-mode="huge"]').forEach((b) => b.addEventListener("click", () => (ui.activeRouteModeCurrent = "huge")));
  document.querySelectorAll('[data-route-mode="small"]').forEach((b) => b.addEventListener("click", () => (ui.activeRouteModeCurrent = "small")));
  document.querySelectorAll('[data-route-mode="train"]').forEach((b) => b.addEventListener("click", () => (ui.activeRouteModeCurrent = "train")));

  document.getElementById("startSegmentCurrent").addEventListener("click", () => startSegment("current"));
  document.getElementById("finishSegmentCurrent").addEventListener("click", finishSegment);
  document.getElementById("undoPointCurrent").addEventListener("click", undoPoint);
  document.getElementById("cancelSegmentCurrent").addEventListener("click", cancelSegment);
  document.getElementById("clearCurrentRoutes").addEventListener("click", () => clearRoutes("current"));

  // IBX route mode buttons
  document.querySelectorAll('[data-route-mode="huge_ibx"]').forEach((b) => b.addEventListener("click", () => (ui.activeRouteModeIbx = "huge_ibx")));
  document.querySelectorAll('[data-route-mode="small_ibx"]').forEach((b) => b.addEventListener("click", () => (ui.activeRouteModeIbx = "small_ibx")));
  document.querySelectorAll('[data-route-mode="ibx"]').forEach((b) => b.addEventListener("click", () => (ui.activeRouteModeIbx = "ibx")));

  document.getElementById("startSegmentIbX").addEventListener("click", () => startSegment("ibx"));
  document.getElementById("finishSegmentIbX").addEventListener("click", finishSegment);
  document.getElementById("undoPointIbX").addEventListener("click", undoPoint);
  document.getElementById("cancelSegmentIbX").addEventListener("click", cancelSegment);
  document.getElementById("clearIbxRoutes").addEventListener("click", () => clearRoutes("ibx"));

  document.getElementById("exportCSV").addEventListener("click", exportCSV);
  document.getElementById("exportRoutesGeoJSON").addEventListener("click", exportRoutesGeoJSON);
}

function setupMap() {
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

async function boot() {
  ensureProjDefs();
  loadState();
  setupUI();
  setupMap();
  rebuildFromState();

  // Auto-load IBX GeoJSON when served over HTTP (fetch fails on file://).
  const [lineGeojson, stationsGeojson] = await Promise.all([
    tryLoadGeoJSONFromFile("./IBX_Line_4326.geojson"),
    tryLoadGeoJSONFromFile("./IBX_Stations.geojson")
  ]);

  if (lineGeojson) loadIBXGeoJSONToLayer(lineGeojson);
  else initIBXPlaceholderLine();
  if (stationsGeojson) loadIBXStationsGeoJSONToLayer(stationsGeojson);

  setStep("locations");
  uiUpdateStats();
  document.getElementById("mapHint").textContent = "Select a tool on the left, then click the map to add points or draw segments.";
}

boot();


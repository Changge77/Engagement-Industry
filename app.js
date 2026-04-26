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

/** Participant map only: short company-location hint follows pointer while over the map. */
let companyBannerPointerOnMap = false;
let rawMaterialOriginBannerPointerOnMap = false;

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

const LOCATION_TYPE_ICONS = {
  workplace: "Icons/Locations/Workplace.svg",
  import: "Icons/Locations/Port.svg",
  export: "Icons/Locations/Airport.svg",
  manufacturing: "Icons/Locations/Manufactoring.svg",
  warehouse: "Icons/Locations/Storage.svg",
  subdistribution: "Icons/Locations/Distribution.svg"
};

const ORIGIN_TYPE_ICONS = {
  storage_facility: "Icons/Locations/Storage.svg",
  distribution_center: "Icons/Locations/Distribution.svg",
  manufacturing_facility: "Icons/Locations/Manufactoring.svg",
  airport: "Icons/Locations/Airport.svg",
  port: "Icons/Locations/Port.svg",
  client: "Icons/General/Client.svg"
};

const SC_LOC_TO_LOCATION_TYPE = {
  storage_facility: "warehouse",
  distribution_center: "subdistribution",
  manufacturing_facility: "manufacturing",
  airport: "export",
  port: "import",
  client: "other",
  other: "other"
};

/** Participant industry questionnaire: multi-select goods / products (keys match checkbox values in sub.html). */
const GOODS_CATEGORY_OPTIONS = [
  { key: "recyclables", label: "Recyclables" },
  { key: "food", label: "Food" },
  { key: "construction", label: "Construction Materials" },
  { key: "automotive", label: "Automotive Parts" },
  { key: "light_fabrication", label: "Light Fabrication" },
  { key: "high_tech", label: "High-tech Productions" },
  { key: "other", label: "Others" }
];
const GOODS_CATEGORY_KEYS = new Set(GOODS_CATEGORY_OPTIONS.map((o) => o.key));

/** Icon paths for participant profile list (match sub.html goodsGate). */
const GOODS_CATEGORY_ICON_SRC = {
  recyclables: "Icons/Products/Recyclables.svg",
  food: "Icons/Products/Food.svg",
  construction: "Icons/Products/COnstruction_materials.svg",
  automotive: "Icons/Products/Auto_parts.svg",
  light_fabrication: "Icons/Products/Light_fabrication.svg",
  high_tech: "Icons/Products/Tech.svg"
};

function participantGoodsProfileListInnerHtml(industry) {
  const ind = industry ?? state.industry;
  const keys = ind?.goodsCategoryKeys;
  if (!Array.isArray(keys) || keys.length === 0) return "";
  const parts = [];
  for (const k of keys) {
    if (!GOODS_CATEGORY_KEYS.has(k)) continue;
    let label = "";
    if (k === "other") {
      const d = String(ind.goodsOtherDetail ?? "").trim();
      label = d ? `Others (${d})` : "Others";
    } else {
      label = GOODS_CATEGORY_OPTIONS.find((o) => o.key === k)?.label ?? k;
    }
    const iconSrc = GOODS_CATEGORY_ICON_SRC[k];
    if (iconSrc) {
      parts.push(
        `<li class="participantProfileProductList__item"><img class="participantProfileProductList__icon" src="${escapeHtml(
          iconSrc
        )}" alt="" aria-hidden="true"/><span class="participantProfileProductList__label">${escapeHtml(label)}</span></li>`
      );
    } else {
      parts.push(
        `<li class="participantProfileProductList__item participantProfileProductList__item--other"><span class="participantProfileProductList__iconFallback" aria-hidden="true">…</span><span class="participantProfileProductList__label">${escapeHtml(
          label
        )}</span></li>`
      );
    }
  }
  return parts.join("");
}

function participantCompanyProfileListInnerHtml() {
  const company = String(state.industry?.companyName ?? "").trim();
  if (!company) return "";
  return `<li class="participantProfileProductList__item participantProfileProductList__item--textOnly"><span class="participantProfileProductList__label">${escapeHtml(
    company
  )}</span></li>`;
}

function participantRoleProfileListInnerHtml() {
  const roleText = formatParticipantRoleSummary(state.industry);
  if (!roleText) return "";
  return `<li class="participantProfileProductList__item participantProfileProductList__item--textOnly"><span class="participantProfileProductList__label">${escapeHtml(
    roleText
  )}</span></li>`;
}

/** Per raw material: first branch — where it originates (keys match sub.html radios). */
const RAW_MATERIAL_ORIGIN_OPTIONS = [
  { key: "storage_facility", label: "Storage Facility" },
  { key: "distribution_center", label: "Distribution center" },
  { key: "manufacturing_facility", label: "Manufacturing Facility" },
  { key: "airport", label: "Airport" },
  { key: "port", label: "Port" },
  { key: "other", label: "Others" }
];
const RAW_MATERIAL_ORIGIN_KEYS = new Set(RAW_MATERIAL_ORIGIN_OPTIONS.map((o) => o.key));

/** Second branch: A/C location types (includes Port). */
const SUPPLY_CHAIN_LOCATION_OPTIONS = [
  { key: "storage_facility", label: "Storage Facility" },
  { key: "distribution_center", label: "Distribution center" },
  { key: "manufacturing_facility", label: "Manufacturing Facility" },
  { key: "airport", label: "Airport" },
  { key: "port", label: "Port" },
  { key: "client", label: "Client" },
  { key: "other", label: "Others" }
];
const SUPPLY_CHAIN_LOCATION_KEYS = new Set(SUPPLY_CHAIN_LOCATION_OPTIONS.map((o) => o.key));

/** Node B — transportation mode change (keys match diagram selects). */
const SUPPLY_CHAIN_MODAL_CHANGE_OPTIONS = [
  { key: "train_to_huge_truck", label: "Train to Huge Truck" },
  { key: "train_to_small_truck", label: "Train to Small Truck" },
  { key: "small_truck_to_train", label: "Small Truck to Train" },
  { key: "huge_truck_to_train", label: "Huge Truck to Train" },
  { key: "huge_truck_to_small_truck", label: "Huge Truck to Small Truck" },
  { key: "small_truck_to_huge_truck", label: "Small Truck to Huge Truck" },
  { key: "other", label: "Others" }
];
const SUPPLY_CHAIN_MODAL_CHANGE_KEYS = new Set(SUPPLY_CHAIN_MODAL_CHANGE_OPTIONS.map((o) => o.key));

/** Transportation segments between nodes. */
const SUPPLY_CHAIN_TRANSPORT_MODE_OPTIONS = [
  { key: "train", label: "Train" },
  { key: "huge_truck", label: "Huge Truck" },
  { key: "small_truck", label: "Small Truck" },
  { key: "other", label: "Others" }
];
const SUPPLY_CHAIN_TRANSPORT_MODE_KEYS = new Set(SUPPLY_CHAIN_TRANSPORT_MODE_OPTIONS.map((o) => o.key));
const TRANSPORT_MODE_ICONS = {
  train: "Icons/Modes/Rail.svg",
  huge_truck: "Icons/Modes/Large-Truck.svg",
  small_truck: "Icons/Modes/Small_truck.svg"
};
const ANIMATION_MODE_ICONS = {
  ...TRANSPORT_MODE_ICONS,
  train: "Icons/Modes/Train_front.svg"
};
/** Participant skipped the whole origin branch (category + map) for this material. */
const RAW_MATERIAL_ORIGIN_SKIPPED_KEY = "skipped";

function normalizeRawMaterialsFromPayload(raw) {
  if (!Array.isArray(raw)) return [];
  return raw
    .map((s) => String(s ?? "").trim())
    .filter(Boolean)
    .slice(0, 40);
}

function normalizeProductsFromPayload(raw) {
  return normalizeRawMaterialsFromPayload(raw);
}

function normalizeRawMaterialBranchesFromPayload(rawMaterials, branchesIn) {
  const n = rawMaterials.length;
  const arr = Array.isArray(branchesIn) ? branchesIn : [];
  const out = [];
  for (let i = 0; i < n; i++) {
    const b = arr[i] || {};
    const key = String(b.originCategoryKey ?? "").trim();
    let originCategoryKey = "";
    if (key === RAW_MATERIAL_ORIGIN_SKIPPED_KEY) originCategoryKey = RAW_MATERIAL_ORIGIN_SKIPPED_KEY;
    else if (RAW_MATERIAL_ORIGIN_KEYS.has(key)) originCategoryKey = key;
    const ox = b.originX;
    const oy = b.originY;
    const diagram = normalizeSupplyChainDiagram(b.supplyChainDiagram);
    let tripFrequencyCount = null;
    const tc = b.tripFrequencyCount;
    if (typeof tc === "number" && Number.isFinite(tc) && tc >= 1) tripFrequencyCount = Math.floor(tc);
    else if (tc != null && String(tc).trim() !== "") {
      const n = Number(tc);
      if (Number.isFinite(n) && n >= 1) tripFrequencyCount = Math.floor(n);
    }
    out.push({
      originCategoryKey,
      originOtherDetail: String(b.originOtherDetail ?? ""),
      originX: typeof ox === "number" && Number.isFinite(ox) ? ox : null,
      originY: typeof oy === "number" && Number.isFinite(oy) ? oy : null,
      originMapSkipped: Boolean(b.originMapSkipped),
      supplyChainIntroAcknowledged: Boolean(b.supplyChainIntroAcknowledged),
      supplyChainDiagram: diagram,
      supplyChainTransportRoutes: normalizeSupplyChainTransportRoutesForBranch(b.supplyChainTransportRoutes, diagram),
      tripFrequencyCount,
      tripFrequencyPeriod: normalizeTripFrequencyPeriod(b.tripFrequencyPeriod)
    });
  }
  return out;
}

function ensureRawMaterialBranchesAligned() {
  const mats = state.industry.rawMaterials ?? [];
  let branches = state.industry.rawMaterialBranches;
  if (!Array.isArray(branches)) branches = [];
  const next = [];
  for (let i = 0; i < mats.length; i++) {
    const prev = branches[i];
    if (prev && typeof prev === "object") {
      const diagram = normalizeSupplyChainDiagram(prev.supplyChainDiagram ?? defaultSupplyChainDiagram());
      let tripFrequencyCount = null;
      const tfc = prev.tripFrequencyCount;
      if (typeof tfc === "number" && Number.isFinite(tfc) && tfc >= 1) tripFrequencyCount = Math.floor(tfc);
      next.push({
        originCategoryKey: String(prev.originCategoryKey ?? ""),
        originOtherDetail: String(prev.originOtherDetail ?? ""),
        originX: typeof prev.originX === "number" && Number.isFinite(prev.originX) ? prev.originX : null,
        originY: typeof prev.originY === "number" && Number.isFinite(prev.originY) ? prev.originY : null,
        originMapSkipped: Boolean(prev.originMapSkipped),
        supplyChainIntroAcknowledged: Boolean(prev.supplyChainIntroAcknowledged),
        supplyChainDiagram: diagram,
        supplyChainTransportRoutes: normalizeSupplyChainTransportRoutesForBranch(
          prev.supplyChainTransportRoutes,
          diagram
        ),
        tripFrequencyCount,
        tripFrequencyPeriod: normalizeTripFrequencyPeriod(prev.tripFrequencyPeriod)
      });
    } else {
      const diagram = defaultSupplyChainDiagram();
      next.push({
        originCategoryKey: "",
        originOtherDetail: "",
        originX: null,
        originY: null,
        originMapSkipped: false,
        supplyChainIntroAcknowledged: false,
        supplyChainDiagram: diagram,
        supplyChainTransportRoutes: normalizeSupplyChainTransportRoutesForBranch([], diagram),
        tripFrequencyCount: null,
        tripFrequencyPeriod: ""
      });
    }
  }
  state.industry.rawMaterialBranches = next;
}

function ensureProductBranchesAligned() {
  const mats = state.industry.products ?? [];
  let branches = state.industry.productBranches;
  if (!Array.isArray(branches)) branches = [];
  const next = [];
  for (let i = 0; i < mats.length; i++) {
    const prev = branches[i];
    if (prev && typeof prev === "object") {
      const diagram = normalizeSupplyChainDiagram(prev.supplyChainDiagram ?? defaultSupplyChainDiagram());
      let tripFrequencyCount = null;
      const tfc = prev.tripFrequencyCount;
      if (typeof tfc === "number" && Number.isFinite(tfc) && tfc >= 1) tripFrequencyCount = Math.floor(tfc);
      next.push({
        originCategoryKey: String(prev.originCategoryKey ?? ""),
        originOtherDetail: String(prev.originOtherDetail ?? ""),
        originX: typeof prev.originX === "number" && Number.isFinite(prev.originX) ? prev.originX : null,
        originY: typeof prev.originY === "number" && Number.isFinite(prev.originY) ? prev.originY : null,
        originMapSkipped: Boolean(prev.originMapSkipped),
        supplyChainIntroAcknowledged: Boolean(prev.supplyChainIntroAcknowledged),
        supplyChainDiagram: diagram,
        supplyChainTransportRoutes: normalizeSupplyChainTransportRoutesForBranch(
          prev.supplyChainTransportRoutes,
          diagram
        ),
        tripFrequencyCount,
        tripFrequencyPeriod: normalizeTripFrequencyPeriod(prev.tripFrequencyPeriod)
      });
    } else {
      const diagram = defaultSupplyChainDiagram();
      next.push({
        originCategoryKey: "",
        originOtherDetail: "",
        originX: null,
        originY: null,
        originMapSkipped: false,
        supplyChainIntroAcknowledged: false,
        supplyChainDiagram: diagram,
        supplyChainTransportRoutes: normalizeSupplyChainTransportRoutesForBranch([], diagram),
        tripFrequencyCount: null,
        tripFrequencyPeriod: ""
      });
    }
  }
  state.industry.productBranches = next;
}

/** Which industry list supply-chain UI is operating on (raw materials vs output products). */
const BRANCH_KIND_RAW = "rawMaterial";
const BRANCH_KIND_PRODUCT = "product";

function industryBranchArrays(kind) {
  if (kind === BRANCH_KIND_PRODUCT) {
    return {
      itemsKey: "products",
      branchesKey: "productBranches",
      ensure: ensureProductBranchesAligned,
      items: state.industry.products ?? [],
      branches: state.industry.productBranches ?? []
    };
  }
  return {
    itemsKey: "rawMaterials",
    branchesKey: "rawMaterialBranches",
    ensure: ensureRawMaterialBranchesAligned,
    items: state.industry.rawMaterials ?? [],
    branches: state.industry.rawMaterialBranches ?? []
  };
}

function branchRow(kind, index) {
  const { ensure, branchesKey } = industryBranchArrays(kind);
  ensure();
  return state.industry[branchesKey][index];
}

function setBranchRow(kind, index, row) {
  const { ensure, branchesKey } = industryBranchArrays(kind);
  ensure();
  state.industry[branchesKey][index] = row;
}

function rawMaterialOriginBranchNeedsCategoryGate(b) {
  if (!b?.originCategoryKey) return true;
  if (b.originCategoryKey === RAW_MATERIAL_ORIGIN_SKIPPED_KEY) return false;
  if (b.originCategoryKey === "other" && !String(b.originOtherDetail ?? "").trim()) return true;
  return false;
}

function rawMaterialOriginBranchNeedsMap(b) {
  if (!b?.originCategoryKey || b.originCategoryKey === RAW_MATERIAL_ORIGIN_SKIPPED_KEY) return false;
  if (b.originCategoryKey === "other" && !String(b.originOtherDetail ?? "").trim()) return false;
  const hasCoords =
    typeof b.originX === "number" &&
    Number.isFinite(b.originX) &&
    typeof b.originY === "number" &&
    Number.isFinite(b.originY);
  if (hasCoords) return false;
  if (b.originMapSkipped) return false;
  return true;
}

/** Shown once per raw material before the origin-type branch questions. */
function rawMaterialSupplyChainBranchNeedsIntroGate(b) {
  return rawMaterialOriginBranchNeedsCategoryGate(b) && !Boolean(b?.supplyChainIntroAcknowledged);
}

function defaultSupplyChainDiagram() {
  return {
    destinationCategoryKey: "",
    destinationOtherDetail: "",
    modalChangeNodes: [{ modalChangeKey: "", otherDetail: "" }],
    transportLegs: [
      { modeKey: "", otherDetail: "" },
      { modeKey: "", otherDetail: "" }
    ]
  };
}

function normalizeSupplyLocationKeyForDiagram(k) {
  const s = String(k ?? "").trim();
  return SUPPLY_CHAIN_LOCATION_KEYS.has(s) ? s : "";
}

function normalizeModalChangeKey(k) {
  const s = String(k ?? "").trim();
  return SUPPLY_CHAIN_LOCATION_KEYS.has(s) ? s : "";
}

function normalizeTransportModeKey(k) {
  const s = String(k ?? "").trim();
  return SUPPLY_CHAIN_TRANSPORT_MODE_KEYS.has(s) ? s : "";
}

function normalizeSupplyChainDiagram(raw) {
  const base = raw && typeof raw === "object" ? raw : {};
  const destKey = normalizeSupplyLocationKeyForDiagram(base.destinationCategoryKey);
  const destOther = String(base.destinationOtherDetail ?? "");
  let nodes = Array.isArray(base.modalChangeNodes) ? base.modalChangeNodes : [];
  nodes = nodes.map((n) => ({
    modalChangeKey: normalizeModalChangeKey(n?.modalChangeKey),
    otherDetail: String(n?.otherDetail ?? "")
  }));
  let legs = Array.isArray(base.transportLegs) ? base.transportLegs : [];
  legs = legs.map((t) => ({
    modeKey: normalizeTransportModeKey(t?.modeKey),
    otherDetail: String(t?.otherDetail ?? "")
  }));
  if (nodes.length === 0) {
    while (legs.length < 1) legs.push({ modeKey: "", otherDetail: "" });
    legs = legs.slice(0, 1).map((t) => ({
      modeKey: normalizeTransportModeKey(t?.modeKey),
      otherDetail: String(t?.otherDetail ?? "")
    }));
  } else {
    while (legs.length < nodes.length + 1) legs.push({ modeKey: "", otherDetail: "" });
    legs = legs.slice(0, nodes.length + 1).map((t) => ({
      modeKey: normalizeTransportModeKey(t?.modeKey),
      otherDetail: String(t?.otherDetail ?? "")
    }));
  }
  return {
    destinationCategoryKey: destKey,
    destinationOtherDetail: destOther,
    modalChangeNodes: nodes,
    transportLegs: legs
  };
}

function modalChangeFromMode(modalChangeKey) {
  switch (String(modalChangeKey ?? "").trim()) {
    case "train_to_huge_truck":
    case "train_to_small_truck":
      return "train";
    case "small_truck_to_train":
    case "small_truck_to_huge_truck":
      return "small_truck";
    case "huge_truck_to_train":
    case "huge_truck_to_small_truck":
      return "huge_truck";
    default:
      return null;
  }
}

function modalChangeToMode(modalChangeKey) {
  switch (String(modalChangeKey ?? "").trim()) {
    case "train_to_huge_truck":
    case "small_truck_to_huge_truck":
      return "huge_truck";
    case "train_to_small_truck":
    case "huge_truck_to_small_truck":
      return "small_truck";
    case "small_truck_to_train":
    case "huge_truck_to_train":
      return "train";
    default:
      return null;
  }
}

function allowedModalChangeKeysForIncomingMode(incomingModeKey) {
  const m = normalizeTransportModeKey(incomingModeKey);
  if (m === "huge_truck") return new Set(["huge_truck_to_train", "huge_truck_to_small_truck", "other"]);
  if (m === "small_truck") return new Set(["small_truck_to_train", "small_truck_to_huge_truck", "other"]);
  if (m === "train") return new Set(["train_to_huge_truck", "train_to_small_truck", "other"]);
  return null; // no restriction (empty/other)
}

/**
 * Applies cascading constraints based on previous answers.
 * - B(i) modal-change options depend on incoming Transportation(i+1) mode.
 * - Transportation(i+2) is forced based on selected B(i) modal-change (when not "other").
 */
function applySupplyChainDiagramConstraints(dIn) {
  return normalizeSupplyChainDiagram(dIn);
}

function isSupplyChainDiagramComplete(d) {
  if (!d || typeof d !== "object") return false;
  const destKey = normalizeSupplyLocationKeyForDiagram(d.destinationCategoryKey);
  if (!destKey) return false;
  if (destKey === "other" && !String(d.destinationOtherDetail ?? "").trim()) return false;
  const nodes = d.modalChangeNodes ?? [];
  const legs = d.transportLegs ?? [];
  if (nodes.length === 0) {
    if (legs.length !== 1) return false;
  } else if (legs.length !== nodes.length + 1) {
    return false;
  }
  for (const n of nodes) {
    const mk = normalizeModalChangeKey(n?.modalChangeKey);
    if (!mk) return false;
    if (mk === "other" && !String(n?.otherDetail ?? "").trim()) return false;
  }
  for (const t of legs) {
    const mode = normalizeTransportModeKey(t?.modeKey);
    if (!mode) return false;
    if (mode === "other" && !String(t?.otherDetail ?? "").trim()) return false;
  }
  return true;
}

/** Second branch: supply-chain diagram (after Q1 origin category + map/skip). */
function rawMaterialSupplyChainBranchNeedsDiagramGate(b) {
  if (!b) return false;
  if (b.originCategoryKey === RAW_MATERIAL_ORIGIN_SKIPPED_KEY) return false;
  if (rawMaterialOriginBranchNeedsCategoryGate(b) || rawMaterialOriginBranchNeedsMap(b)) return false;
  return !isSupplyChainDiagramComplete(b.supplyChainDiagram);
}

function normalizeSupplyChainTransportRoutesForBranch(raw, diagram) {
  const d = normalizeSupplyChainDiagram(diagram ?? defaultSupplyChainDiagram());
  const n = d.transportLegs.length;
  const arr = Array.isArray(raw) ? raw : [];
  const out = [];
  for (let i = 0; i < n; i++) {
    const leg = arr[i];
    const pts = Array.isArray(leg?.points) ? leg.points : [];
    const cleaned = pts
      .filter(
        (p) =>
          Array.isArray(p) &&
          p.length >= 2 &&
          Number.isFinite(Number(p[0])) &&
          Number.isFinite(Number(p[1]))
      )
      .map((p) => [Number(p[0]), Number(p[1])]);
    out.push({ points: cleaned });
  }
  return out;
}

function isSupplyChainTransportRoutesComplete(b) {
  if (!b?.supplyChainDiagram) return false;
  const d = normalizeSupplyChainDiagram(b.supplyChainDiagram);
  const routes = normalizeSupplyChainTransportRoutesForBranch(b.supplyChainTransportRoutes, b.supplyChainDiagram);
  const n = d.transportLegs.length;
  if (routes.length < n) return false;
  for (let i = 0; i < n; i++) {
    const pts = routes[i]?.points;
    if (!Array.isArray(pts) || pts.length < 2) return false;
  }
  return true;
}

/** After the supply-chain diagram is complete: draw each transportation leg on the map. */
function rawMaterialSupplyChainBranchNeedsTransportRoutes(b) {
  if (!b) return false;
  if (b.originCategoryKey === RAW_MATERIAL_ORIGIN_SKIPPED_KEY) return false;
  if (rawMaterialOriginBranchNeedsCategoryGate(b) || rawMaterialOriginBranchNeedsMap(b)) return false;
  if (!isSupplyChainDiagramComplete(b.supplyChainDiagram)) return false;
  return !isSupplyChainTransportRoutesComplete(b);
}

const TRIP_FREQUENCY_PERIOD_KEYS = new Set(["day", "week", "month"]);

function normalizeTripFrequencyPeriod(k) {
  const s = String(k ?? "")
    .trim()
    .toLowerCase();
  return TRIP_FREQUENCY_PERIOD_KEYS.has(s) ? s : "";
}

function isTripFrequencyComplete(b) {
  const per = normalizeTripFrequencyPeriod(b?.tripFrequencyPeriod);
  if (!per) return false;
  const n = b?.tripFrequencyCount;
  const num = typeof n === "number" ? n : Number(n);
  return Number.isFinite(num) && num >= 1 && Math.floor(num) === num;
}

/** After all transport routes are drawn: how often does this trip occur (popup, then left panel). */
function rawMaterialSupplyChainBranchNeedsTripFrequencyGate(b) {
  if (!b) return false;
  if (b.originCategoryKey === RAW_MATERIAL_ORIGIN_SKIPPED_KEY) return false;
  if (rawMaterialOriginBranchNeedsCategoryGate(b) || rawMaterialOriginBranchNeedsMap(b)) return false;
  if (!isSupplyChainDiagramComplete(b.supplyChainDiagram)) return false;
  if (!isSupplyChainTransportRoutesComplete(b)) return false;
  return !isTripFrequencyComplete(b);
}

function supplyChainTransportKeyToRouteBaseKey(k) {
  const m = normalizeTransportModeKey(k);
  if (m === "huge_truck") return "huge";
  if (m === "small_truck") return "small";
  if (m === "train") return "train";
  return "small";
}

function getSupplyChainTransportRouteColor(modeKey) {
  const base = supplyChainTransportKeyToRouteBaseKey(modeKey);
  return getSegmentColor("current", { modeKey: base });
}

function formatSupplyChainTransportModeLabel(leg) {
  const m = normalizeTransportModeKey(leg?.modeKey);
  if (m === "other") return String(leg?.otherDetail ?? "").trim() || "Other";
  const opt = SUPPLY_CHAIN_TRANSPORT_MODE_OPTIONS.find((o) => o.key === m);
  return opt ? opt.label : m;
}

function getSupplyChainRouteStartLatLng(branch, legIndex) {
  if (legIndex === 0) {
    if (typeof branch?.originX !== "number" || typeof branch?.originY !== "number") return null;
    return epsg2263XYToLatLng(branch.originX, branch.originY);
  }
  const routes = normalizeSupplyChainTransportRoutesForBranch(branch?.supplyChainTransportRoutes, branch?.supplyChainDiagram);
  const prev = routes[legIndex - 1]?.points;
  if (!Array.isArray(prev) || prev.length < 1) return null;
  const last = prev[prev.length - 1];
  return epsg2263XYToLatLng(last[0], last[1]);
}

/** Last point of the final transportation leg (diagram node C), when all legs are drawn. */
function getSupplyChainRouteDestinationLatLng(branch) {
  if (!branch?.supplyChainDiagram) return null;
  if (!isSupplyChainTransportRoutesComplete(branch)) return null;
  const d = normalizeSupplyChainDiagram(branch.supplyChainDiagram);
  const routes = normalizeSupplyChainTransportRoutesForBranch(branch.supplyChainTransportRoutes, branch.supplyChainDiagram);
  const n = d.transportLegs.length;
  if (n < 1) return null;
  const pts = routes[n - 1]?.points;
  if (!Array.isArray(pts) || pts.length < 2) return null;
  const last = pts[pts.length - 1];
  return epsg2263XYToLatLng(last[0], last[1]);
}

function formatSupplyChainDestinationDesc(dIn) {
  const d = normalizeSupplyChainDiagram(dIn ?? {});
  const destKey = normalizeSupplyLocationKeyForDiagram(d.destinationCategoryKey);
  if (!destKey) return "—";
  if (destKey === "other") {
    const t = String(d.destinationOtherDetail ?? "").trim();
    return t ? `Others (${t})` : "Others";
  }
  const opt = SUPPLY_CHAIN_LOCATION_OPTIONS.find((o) => o.key === destKey);
  return opt ? opt.label : destKey;
}

function markerIconForSupplyChainDestinationCategory(dIn) {
  const d = normalizeSupplyChainDiagram(dIn ?? {});
  const k = normalizeSupplyLocationKeyForDiagram(d.destinationCategoryKey);
  const svg = k && k !== "other" ? ORIGIN_TYPE_ICONS[k] : null;
  if (svg) return markerIconFromSvgInverted(svg);
  return markerIconColored("#7c3aed");
}

/** Clear map geometry for transportation leg `fromLegIndex` and all following legs; earlier legs unchanged. */
function clearSupplyChainTransportRoutesFromLeg(branchKind, branchIndex, fromLegIndex) {
  const { ensure, branchesKey } = industryBranchArrays(branchKind);
  ensure();
  const row = state.industry[branchesKey][branchIndex];
  if (!row) return;
  const d = normalizeSupplyChainDiagram(row.supplyChainDiagram);
  const n = d.transportLegs.length;
  if (fromLegIndex < 0 || fromLegIndex >= n) return;
  const routes = normalizeSupplyChainTransportRoutesForBranch(row.supplyChainTransportRoutes, row.supplyChainDiagram);
  for (let i = fromLegIndex; i < n; i++) {
    routes[i] = { points: [] };
  }
  state.industry[branchesKey][branchIndex] = {
    ...row,
    supplyChainTransportRoutes: routes,
    tripFrequencyCount: null,
    tripFrequencyPeriod: ""
  };
}

function canRedrawSupplyChainTransportLeg(branch, legIndex) {
  return getSupplyChainRouteStartLatLng(branch, legIndex) != null;
}

/** Participant left panel: erase this leg and later routes, then start drawing this leg on the map. */
function participantRedrawSupplyChainRouteFromLeg(branchKind, branchIndex, legIndex) {
  if (readOnly || SURVEY_MODE !== "participant") return;
  clearPendingLocation();
  clearSupplyChainTransportRoutesFromLeg(branchKind, branchIndex, legIndex);
  void flushSaveToServer();
  rebuildFromState();
  uiUpdateStats();
  setParticipantMapHintAfterIndustryGate();
  syncParticipantMapGateOverlay();
  startSupplyChainTransportRouteLeg(branchKind, branchIndex, legIndex);
}

function removeSupplyChainRoutePreviewPolyline() {
  if (supplyChainRoutePreviewPolyline) {
    try {
      supplyChainRoutePreviewPolyline.remove();
    } catch {
      // ignore
    }
    supplyChainRoutePreviewPolyline = null;
  }
}

function updateSupplyChainRoutePreview() {
  const dr = ui.scRouteDrawing;
  removeSupplyChainRoutePreviewPolyline();
  if (!dr || !layers?.supplyChainDraft) return;
  if (!dr.previewCursorLatLng || dr.points.length < 1) return;
  const last = dr.points[dr.points.length - 1];
  const cursor = dr.previewCursorLatLng;
  const color = getSupplyChainTransportRouteColor(dr.modeKey);
  supplyChainRoutePreviewPolyline = L.polyline([last, cursor], {
    color,
    weight: 4,
    opacity: 0.55,
    dashArray: "6 10",
    lineCap: "round",
    lineJoin: "round"
  }).addTo(layers.supplyChainDraft);
}

function resetSupplyChainRouteDraft() {
  removeSupplyChainRoutePreviewPolyline();
  if (layers?.supplyChainDraft) layers.supplyChainDraft.clearLayers();
  ui.scRouteDrawing = null;
  syncParticipantLeftPanel();
  updateParticipantSupplyChainRouteEditingUI();
}

function supplyChainRouteConfirmLabel(d, legIndex) {
  const nodes = d.modalChangeNodes ?? [];
  const last = legIndex >= (d.transportLegs?.length ?? 0) - 1;
  if (last) return "This is Destination";
  const bLab = nodes.length <= 1 ? "B" : `B${legIndex + 1}`;
  return `This is Stopping Point ${bLab}`;
}

/** Start/end node labels (A, B1, …, C) for the map “Currently Editing” banner. */
function supplyChainRouteEditingEndpointLabels(d, legIndex) {
  const nodes = d.modalChangeNodes ?? [];
  const legs = d.transportLegs ?? [];
  if (legIndex < 0 || legIndex >= legs.length) return { startLabel: "?", endLabel: "?" };
  const last = legIndex >= legs.length - 1;
  let startLabel = "A";
  if (legIndex > 0) {
    startLabel = nodes.length <= 1 ? "B" : `B${legIndex}`;
  }
  let endLabel = "C";
  if (!last) {
    endLabel = nodes.length <= 1 ? "B" : `B${legIndex + 1}`;
  }
  return { startLabel, endLabel };
}

function positionParticipantSupplyChainRouteCursorHint(clientX, clientY) {
  const el = document.getElementById("participantSupplyChainRouteCursorHint");
  const wrap = el?.closest(".mapWrap");
  if (!el || !wrap || el.classList.contains("is-hidden")) return;
  const rect = wrap.getBoundingClientRect();
  const offX = 18;
  const offY = 18;
  let x = clientX - rect.left + offX;
  let y = clientY - rect.top + offY;
  const bw = el.offsetWidth || 1;
  const bh = el.offsetHeight || 1;
  const pad = 8;
  x = Math.min(Math.max(pad, x), Math.max(pad, rect.width - bw - pad));
  y = Math.min(Math.max(pad, y), Math.max(pad, rect.height - bh - pad));
  el.style.left = `${x}px`;
  el.style.top = `${y}px`;
}

function updateParticipantSupplyChainRouteEditingUI() {
  const banner = document.getElementById("participantSupplyChainRouteBanner");
  const hint = document.getElementById("participantSupplyChainRouteCursorHint");
  if (!banner && !hint) return;
  const dr = ui.scRouteDrawing;
  const show = SURVEY_MODE === "participant" && !readOnly && dr != null;
  if (banner) {
    banner.classList.toggle("is-hidden", !show);
    banner.setAttribute("aria-hidden", show ? "false" : "true");
    if (show) {
      const row = branchRow(dr.branchKind, dr.branchIndex);
      const d = normalizeSupplyChainDiagram(row?.supplyChainDiagram);
      const leg = d.transportLegs[dr.legIndex] ?? { modeKey: "", otherDetail: "" };
      const modeLab = formatSupplyChainTransportModeLabel(leg);
      const { startLabel, endLabel } = supplyChainRouteEditingEndpointLabels(d, dr.legIndex);
      const fromEl = banner.querySelector("[data-sc-route-banner-from]");
      const toEl = banner.querySelector("[data-sc-route-banner-to]");
      const modeEl = banner.querySelector("[data-sc-route-banner-mode]");
      if (fromEl) fromEl.textContent = startLabel;
      if (toEl) toEl.textContent = endLabel;
      if (modeEl) modeEl.textContent = modeLab;
    }
  }
  if (hint) {
    if (!show) {
      hint.classList.add("is-hidden");
      hint.setAttribute("aria-hidden", "true");
      hint.style.left = "";
      hint.style.top = "";
    }
    /* While drawing, visibility/position follow the pointer via map mousemove. */
  }
  updateMapCursor();
}

function refreshSupplyChainRouteDraftVisuals() {
  const dr = ui.scRouteDrawing;
  if (!dr || !layers?.supplyChainDraft) return;
  removeSupplyChainRoutePreviewPolyline();
  layers.supplyChainDraft.clearLayers();
  const row = branchRow(dr.branchKind, dr.branchIndex);
  const d = normalizeSupplyChainDiagram(row?.supplyChainDiagram);
  const color = getSupplyChainTransportRouteColor(dr.modeKey);
  const dashStyle = { color, weight: 5, opacity: 0.92, dashArray: "7 5" };
  if (dr.points.length >= 2) {
    L.polyline(dr.points, dashStyle).addTo(layers.supplyChainDraft);
  }
  if (dr.points.length === 1) {
    L.circleMarker(dr.points[0], { radius: 6, color, weight: 2, fillColor: "#fff", fillOpacity: 0.95 }).addTo(
      layers.supplyChainDraft
    );
  }
  const labelText = supplyChainRouteConfirmLabel(d, dr.legIndex);
  if (dr.points.length >= 2) {
    const end = dr.points[dr.points.length - 1];
    const icon = L.divIcon({
      className: "supplyChainRouteConfirmWrap",
      html: `<div class="supplyChainRouteConfirmStack"><div class="supplyChainRouteConfirm"><span class="supplyChainRouteConfirm__text">${escapeHtml(
        labelText
      )}</span><button type="button" class="supplyChainRouteConfirm__btn" data-sc-route-confirm title="Confirm this stop">√</button></div><div class="supplyChainRouteUndoRow"><button type="button" class="supplyChainRouteUndoRow__btn" data-sc-route-undo-last title="Remove last point">−</button><span class="supplyChainRouteUndoRow__label">Remove Last Point</span></div></div>`,
      iconSize: [300, 136],
      iconAnchor: [150, 136]
    });
    const mk = L.marker(end, { icon, zIndexOffset: 700 });
    mk.addTo(layers.supplyChainDraft);
    requestAnimationFrame(() => {
      const el = mk.getElement();
      const undoBtn = el?.querySelector("[data-sc-route-undo-last]");
      undoBtn?.addEventListener("click", (e) => {
        L.DomEvent.stopPropagation(e);
        L.DomEvent.preventDefault(e);
        popLastSupplyChainRoutePoint();
      });
      const btn = el?.querySelector("[data-sc-route-confirm]");
      btn?.addEventListener("click", (e) => {
        L.DomEvent.stopPropagation(e);
        L.DomEvent.preventDefault(e);
        finishSupplyChainTransportRouteLegAndContinue();
      });
      mk.on("click", (e) => L.DomEvent.stopPropagation(e));
    });
  }
  updateSupplyChainRoutePreview();
}

function appendSupplyChainTransportRoutePoint(latlng) {
  const dr = ui.scRouteDrawing;
  if (!dr) return;
  dr.previewCursorLatLng = null;
  dr.points.push(latlng);
  const [x, y] = gpsToEPSG2263(latlng.lng, latlng.lat);
  dr.points2263.push([x, y]);
  refreshSupplyChainRouteDraftVisuals();
  setParticipantMapHintAfterIndustryGate();
}

function snapRouteToExistingLocation(latlng, locId) {
  const dr = ui.scRouteDrawing;
  if (!dr) return;
  dr.previewCursorLatLng = null;
  dr.points.push(latlng);
  const [x, y] = gpsToEPSG2263(latlng.lng, latlng.lat);
  dr.points2263.push([x, y]);

  if (locId != null) {
    const { branchesKey } = industryBranchArrays(dr.branchKind);
    const row = state.industry?.[branchesKey]?.[dr.branchIndex];
    const d = normalizeSupplyChainDiagram(row?.supplyChainDiagram);
    const li = dr.legIndex;
    if (li < d.modalChangeNodes.length) {
      const scLocKey = normalizeModalChangeKey(d.modalChangeNodes[li]?.modalChangeKey);
      const newLocType = SC_LOC_TO_LOCATION_TYPE[scLocKey];
      if (newLocType) {
        const locIdx = state.locations.findIndex((l) => l.id === locId);
        if (locIdx >= 0 && state.locations[locIdx].locationType !== newLocType) {
          state.locations[locIdx] = { ...state.locations[locIdx], locationType: newLocType };
        }
      }
    }
  }

  finishSupplyChainTransportRouteLegAndContinue();
}

/** Remove the last clicked point; the automatic start point (index 0) is kept. */
function popLastSupplyChainRoutePoint() {
  const dr = ui.scRouteDrawing;
  if (!dr || dr.points.length <= 1) return;
  dr.previewCursorLatLng = null;
  dr.points.pop();
  dr.points2263.pop();
  refreshSupplyChainRouteDraftVisuals();
  setParticipantMapHintAfterIndustryGate();
}

function finishSupplyChainTransportRouteLegAndContinue() {
  const dr = ui.scRouteDrawing;
  if (!dr || dr.points.length < 2) return;
  const bk = dr.branchKind;
  const bi = dr.branchIndex;
  const li = dr.legIndex;
  const { ensure, branchesKey } = industryBranchArrays(bk);
  ensure();
  const row = state.industry[branchesKey][bi];
  if (!row) {
    resetSupplyChainRouteDraft();
    return;
  }
  let routes = normalizeSupplyChainTransportRoutesForBranch(row.supplyChainTransportRoutes, row.supplyChainDiagram);
  routes[li] = { points: dr.points2263.map((p) => [p[0], p[1]]) };
  state.industry[branchesKey][bi] = {
    ...row,
    supplyChainTransportRoutes: routes
  };
  resetSupplyChainRouteDraft();
  void flushSaveToServer();
  rebuildFromState();
  uiUpdateStats();
  setParticipantMapHintAfterIndustryGate();
  const d = normalizeSupplyChainDiagram(state.industry[branchesKey][bi].supplyChainDiagram);
  if (li + 1 < d.transportLegs.length) {
    startSupplyChainTransportRouteLeg(bk, bi, li + 1);
  } else {
    resumeParticipantSupplyChainFlow();
    if (!participantAnySupplyChainBranchWorkIncomplete()) {
      setParticipantMapHintAfterIndustryGate();
      void flushSaveToServer();
    }
  }
}

function startSupplyChainTransportRouteLeg(branchKind, branchIndex, legIndex) {
  if (readOnly) return;
  clearPendingLocation();
  resetSupplyChainRouteDraft();
  const { ensure, branchesKey } = industryBranchArrays(branchKind);
  ensure();
  const row = state.industry[branchesKey][branchIndex];
  if (!row) return;
  const d = normalizeSupplyChainDiagram(row.supplyChainDiagram);
  if (legIndex < 0 || legIndex >= d.transportLegs.length) return;
  const start = getSupplyChainRouteStartLatLng(row, legIndex);
  if (!start) {
    window.alert(
      "Could not determine where this route starts. Check the originating location and earlier transportation legs."
    );
    return;
  }
  const leg = d.transportLegs[legIndex];
  const [sx, sy] = gpsToEPSG2263(start.lng, start.lat);
  ui.scKind = branchKind;
  ui.scRouteDrawing = {
    branchKind,
    branchIndex,
    legIndex,
    modeKey: leg.modeKey,
    points: [start],
    points2263: [[sx, sy]]
  };
  refreshSupplyChainRouteDraftVisuals();
  setParticipantMapHintAfterIndustryGate();
  syncParticipantMapGateOverlay();
  syncParticipantLeftPanel();
  if (map) {
    requestAnimationFrame(() => map.invalidateSize());
  }
}

function startSupplyChainTransportRouteDrawing(branchKind, branchIndex) {
  const { ensure, branchesKey } = industryBranchArrays(branchKind);
  ensure();
  const b = state.industry[branchesKey][branchIndex];
  if (!b) return;
  const d = normalizeSupplyChainDiagram(b.supplyChainDiagram);
  const routes = normalizeSupplyChainTransportRoutesForBranch(b.supplyChainTransportRoutes, b.supplyChainDiagram);
  state.industry[branchesKey][branchIndex] = { ...b, supplyChainTransportRoutes: routes };
  for (let i = 0; i < d.transportLegs.length; i++) {
    const pts = routes[i]?.points;
    if (!Array.isArray(pts) || pts.length < 2) {
      startSupplyChainTransportRouteLeg(branchKind, branchIndex, i);
      return;
    }
  }
  resumeParticipantSupplyChainFlow();
}

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
  },
  /** Participant only: { type, latlng } before confirm (√); not persisted until committed. */
  pendingLocation: null,
  /** Unified supply-chain branch context (raw materials or output products). */
  scKind: BRANCH_KIND_RAW,
  scIntroIndex: null,
  scOriginIndex: null,
  scDiagramIndex: null,
  scTripFreqIndex: null,
  /** { kind: 'rawMaterial'|'product', branchIndex: number } — map placement for origin. */
  scBranchMap: null,
  /** { kind, branchIndex, legIndex, modeKey, points, points2263, previewCursorLatLng } */
  scRouteDrawing: null,
  /** Left panel: which branch list and row are shown in the detail area. */
  participantLeftPanelKind: BRANCH_KIND_RAW,
  participantLeftPanelIndex: 0
};

const state = {
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

  clearPendingLocation();

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

  const rawGoods = parsed.industry?.goodsCategoryKeys;
  const goodsCategoryKeys =
    Array.isArray(rawGoods) && rawGoods.length > 0
      ? [...new Set(rawGoods.filter((k) => GOODS_CATEGORY_KEYS.has(String(k))))]
      : [];
  const rawMats = parsed.industry?.rawMaterials;
  const rawMaterials = normalizeRawMaterialsFromPayload(rawMats);
  const rawMaterialBranches = normalizeRawMaterialBranchesFromPayload(
    rawMaterials,
    parsed.industry?.rawMaterialBranches
  );
  const products = normalizeProductsFromPayload(parsed.industry?.products);
  const productBranches = normalizeRawMaterialBranchesFromPayload(products, parsed.industry?.productBranches);
  state.industry = {
    companyName: String(parsed.industry?.companyName ?? ""),
    roleKey: String(parsed.industry?.roleKey ?? ""),
    roleOtherDetail: String(parsed.industry?.roleOtherDetail ?? ""),
    goodsCategoryKeys,
    goodsOtherDetail: String(parsed.industry?.goodsOtherDetail ?? ""),
    rawMaterials,
    rawMaterialBranches,
    products,
    productBranches
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
  updateMapCursor();
}

let map;
let layers;
/** Rubber-band segment while drawing supply-chain routes (not persisted). */
let supplyChainRoutePreviewPolyline = null;

function markerIconFromSvg(svgSrc) {
  return L.divIcon({
    className: "",
    html: `<div style="width:32px;height:32px;border-radius:50%;background:#fff;border:2px solid #ccc;display:flex;align-items:center;justify-content:center;box-shadow:0 1px 4px rgba(0,0,0,0.25)"><img src="${svgSrc}" style="width:27px;height:27px" alt=""></div>`,
    iconSize: [32, 32],
    iconAnchor: [16, 16]
  });
}

function markerIconFromSvgInverted(svgSrc) {
  return L.divIcon({
    className: "",
    html: `<div style="width:32px;height:32px;border-radius:50%;background:#111;border:2px solid #111;display:flex;align-items:center;justify-content:center;box-shadow:0 1px 4px rgba(0,0,0,0.35)"><img src="${svgSrc}" style="width:27px;height:27px;filter:invert(1)" alt=""></div>`,
    iconSize: [32, 32],
    iconAnchor: [16, 16]
  });
}

function markerIconColored(color) {
  return L.divIcon({
    className: "",
    html: `<div style="width:14px;height:14px;border-radius:50%;background:${color};border:2px solid rgba(255,255,255,0.9)"></div>`,
    iconSize: [14, 14],
    iconAnchor: [7, 7]
  });
}

function updateMapCursor() {
  const container = map?.getContainer();
  if (!container) return;
  const active =
    ui.scRouteDrawing ||
    ui.drawing.active ||
    ui.scBranchMap !== null ||
    ui.locationType !== "none";
  container.style.cursor = active ? "crosshair" : "";
}

function markerIcon(locationType, fallbackColor) {
  const svgSrc = LOCATION_TYPE_ICONS[locationType];
  return svgSrc ? markerIconFromSvg(svgSrc) : markerIconColored(fallbackColor);
}

function buildPendingLocationConfirmIconHtml(dotColor, svgSrc) {
  const dot = svgSrc
    ? `<div class="locationPendingConfirm__dot locationPendingConfirm__dot--icon"><img src="${svgSrc}" style="width:24px;height:24px" alt=""></div>`
    : `<div class="locationPendingConfirm__dot" style="background:${dotColor}"></div>`;
  return `<div class="locationPendingConfirm">
  <div class="locationPendingConfirm__row">
    <button type="button" class="locationPendingConfirm__btn" data-pending-loc="confirm" aria-label="Confirm location">✓</button>
    <button type="button" class="locationPendingConfirm__btn locationPendingConfirm__btn--cancel" data-pending-loc="cancel" aria-label="Choose again">✕</button>
  </div>
  ${dot}
</div>`;
}

function pendingLocationDotColor(pending) {
  if (!pending) return LOCATION_TYPES.other.color;
  if (pending.kind === "branchOrigin" || pending.kind === "rawMaterialOrigin") return "#14b8a6";
  const t = pending.type ?? "other";
  const meta = LOCATION_TYPES[t] ?? LOCATION_TYPES.other;
  return meta.color;
}

function clearPendingLocation() {
  ui.pendingLocation = null;
  if (layers?.pendingLocation) layers.pendingLocation.clearLayers();
}

function renderPendingLocationMarker() {
  if (!layers?.pendingLocation || !map) return;
  layers.pendingLocation.clearLayers();
  if (!ui.pendingLocation) return;
  const { latlng } = ui.pendingLocation;
  const dotColor = pendingLocationDotColor(ui.pendingLocation);
  const pendingSvg =
    ui.pendingLocation.kind === "branchOrigin" || ui.pendingLocation.kind === "rawMaterialOrigin"
      ? ORIGIN_TYPE_ICONS[ui.pendingLocation.originCategoryKey]
      : LOCATION_TYPE_ICONS[ui.pendingLocation.type];
  const icon = L.divIcon({
    className: "locationPendingConfirm-marker",
    html: buildPendingLocationConfirmIconHtml(dotColor, pendingSvg),
    iconSize: [75, 57],
    iconAnchor: [38, 57]
  });
  const marker = L.marker(latlng, { icon, zIndexOffset: 800 });
  marker.once("add", function onPendingAdd() {
    let tries = 0;
    const attach = () => {
      const root = this.getElement?.();
      if (!root && tries++ < 24) {
        requestAnimationFrame(attach.bind(this));
        return;
      }
      if (!root) return;
      L.DomEvent.on(root, "mousedown", L.DomEvent.stopPropagation);
      L.DomEvent.on(root, "click", L.DomEvent.stopPropagation);
      L.DomEvent.on(root, "dblclick", L.DomEvent.stopPropagation);
      const confirmBtn = root.querySelector('[data-pending-loc="confirm"]');
      const cancelBtn = root.querySelector('[data-pending-loc="cancel"]');
      if (confirmBtn) {
        L.DomEvent.on(confirmBtn, "click", (e) => {
          L.DomEvent.stopPropagation(e);
          commitPendingLocation();
        });
      }
      if (cancelBtn) {
        L.DomEvent.on(cancelBtn, "click", (e) => {
          L.DomEvent.stopPropagation(e);
          cancelPendingLocation();
        });
      }
    };
    requestAnimationFrame(attach.bind(this));
  });
  marker.addTo(layers.pendingLocation);
}

function setPendingLocation(type, latlng) {
  if (readOnly) return;
  ui.pendingLocation = { kind: "surveyLocation", type, latlng };
  renderPendingLocationMarker();
}

function setPendingBranchOrigin(branchKind, branchIndex, latlng) {
  if (readOnly) return;
  const br = branchRow(branchKind, branchIndex);
  ui.pendingLocation = {
    kind: "branchOrigin",
    branchKind,
    branchIndex,
    originCategoryKey: br?.originCategoryKey,
    latlng
  };
  renderPendingLocationMarker();
}

function setPendingRawMaterialOrigin(materialIndex, latlng) {
  setPendingBranchOrigin(BRANCH_KIND_RAW, materialIndex, latlng);
}

function commitPendingLocation() {
  if (!ui.pendingLocation || readOnly) return;
  const p = ui.pendingLocation;
  if (p.kind === "branchOrigin" || p.kind === "rawMaterialOrigin") {
    const branchKind = p.kind === "rawMaterialOrigin" ? BRANCH_KIND_RAW : p.branchKind;
    const branchIndex = p.kind === "rawMaterialOrigin" ? p.materialIndex : p.branchIndex;
    const { latlng } = p;
    clearPendingLocation();
    const [x, y] = gpsToEPSG2263(latlng.lng, latlng.lat);
    const { ensure, branchesKey } = industryBranchArrays(branchKind);
    ensure();
    const row = state.industry[branchesKey][branchIndex] ?? {
      originCategoryKey: "",
      originOtherDetail: "",
      originX: null,
      originY: null,
      originMapSkipped: false
    };
    state.industry[branchesKey][branchIndex] = {
      ...row,
      originX: x,
      originY: y,
      originMapSkipped: false
    };
    ui.scBranchMap = null;
    void flushSaveToServer();
    rebuildFromState();
    uiUpdateStats();
    setParticipantMapHintAfterIndustryGate();
    advanceAfterRawMaterialOriginPlaced(branchIndex);
    return;
  }
  const type = p.type;
  const latlng = p.latlng;
  clearPendingLocation();
  addLocation(type, latlng);
}

function cancelPendingLocation() {
  clearPendingLocation();
  const hintEl = document.getElementById("mapHint");
  if (!hintEl || SURVEY_MODE !== "participant" || ui.activeStep !== "locations") return;
  if (ui.scBranchMap !== null) {
    setParticipantMapHintAfterIndustryGate();
    return;
  }
  if (ui.locationType !== "none") {
    hintEl.textContent = `Placement: click the map to add a ${
      LOCATION_TYPES[ui.locationType]?.label ?? ui.locationType
    } point.`;
  }
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

/** Originating-location pins for raw-material or product supply-chain branches. */
function addSupplyChainBranchOriginMarkersToLocationsLayer(branches, itemLabels, kind) {
  const isProduct = kind === BRANCH_KIND_PRODUCT;
  const roleLine = isProduct ? "Product origin" : "Raw material origin";
  const fallback = isProduct ? "Product" : "Material";
  const arr = Array.isArray(branches) ? branches : [];
  const labels = Array.isArray(itemLabels) ? itemLabels : [];
  for (let i = 0; i < arr.length; i++) {
    const b = arr[i];
    if (typeof b?.originX !== "number" || typeof b?.originY !== "number") continue;
    const latlng = epsg2263XYToLatLng(b.originX, b.originY);
    const itemLabel = String(labels[i] ?? "").trim() || `${fallback} ${i + 1}`;
    const originOpt = RAW_MATERIAL_ORIGIN_OPTIONS.find((o) => o.key === b.originCategoryKey);
    let originDesc = "—";
    if (originOpt) originDesc = originOpt.label;
    else if (b.originCategoryKey === "other") {
      const d = String(b.originOtherDetail ?? "").trim();
      originDesc = d ? `Others (${d})` : "Others";
    }
    const originSvg = ORIGIN_TYPE_ICONS[b.originCategoryKey];
    const mk = L.marker(latlng, {
      icon: originSvg ? markerIconFromSvgInverted(originSvg) : markerIconColored("#14b8a6"),
      draggable: false
    });
    mk.bindPopup(
      `${roleLine}<br/>${escapeHtml(itemLabel)}<br/>Where it originates: ${escapeHtml(originDesc)}`
    );
    mk.on("click", (e) => {
      if (ui.scRouteDrawing) {
        L.DomEvent.stopPropagation(e);
        mk.closePopup();
        snapRouteToExistingLocation(mk.getLatLng(), null);
      }
    });
    layers.locations.addLayer(mk);
  }
}

/** Saved supply-chain transport polylines for raw-material or product branches. */
function addSupplyChainBranchRoutePolylinesToLayer(branches, itemLabels, kind) {
  const isProduct = kind === BRANCH_KIND_PRODUCT;
  const fallback = isProduct ? "Product" : "Material";
  const routeSuffix = isProduct ? "product supply chain route" : "supply chain route";
  const arr = Array.isArray(branches) ? branches : [];
  const labels = Array.isArray(itemLabels) ? itemLabels : [];
  for (let bi = 0; bi < arr.length; bi++) {
    const b = arr[bi];
    if (!b?.supplyChainDiagram) continue;
    const d = normalizeSupplyChainDiagram(b.supplyChainDiagram);
    const routes = normalizeSupplyChainTransportRoutesForBranch(b.supplyChainTransportRoutes, b.supplyChainDiagram);
    const itemLabel = String(labels[bi] ?? "").trim() || `${fallback} ${bi + 1}`;
    for (let li = 0; li < routes.length; li++) {
      const pts = routes[li]?.points;
      if (!Array.isArray(pts) || pts.length < 2) continue;
      const latlngs = surveyPointsToLatLngs(pts);
      const modeKey = d.transportLegs[li]?.modeKey ?? "";
      const color = getSupplyChainTransportRouteColor(modeKey);
      const line = L.polyline(latlngs, createModePolylineStyle(color, false));
      const modeLab = formatSupplyChainTransportModeLabel(d.transportLegs[li]);
      line.bindPopup(
        `${escapeHtml(itemLabel)} · Transportation ${li + 1} (${escapeHtml(modeLab)}) · ${routeSuffix}`
      );
      layers.supplyChainRoutes.addLayer(line);
    }
  }
}

/** Stopping points (diagram B nodes): icon from location type; position from last point of each intermediate leg. */
function addSupplyChainBranchStoppingPointMarkersToLayer(branches, itemLabels, kind) {
  if (!layers?.supplyChainDestinations) return;
  const isProduct = kind === BRANCH_KIND_PRODUCT;
  const fallback = isProduct ? "Product" : "Material";
  const arr = Array.isArray(branches) ? branches : [];
  const labels = Array.isArray(itemLabels) ? itemLabels : [];
  for (let bi = 0; bi < arr.length; bi++) {
    const b = arr[bi];
    if (!b || b.originCategoryKey === RAW_MATERIAL_ORIGIN_SKIPPED_KEY) continue;
    const d = normalizeSupplyChainDiagram(b.supplyChainDiagram ?? defaultSupplyChainDiagram());
    const routes = normalizeSupplyChainTransportRoutesForBranch(b.supplyChainTransportRoutes, b.supplyChainDiagram);
    const itemLabel = String(labels[bi] ?? "").trim() || `${fallback} ${bi + 1}`;
    for (let ni = 0; ni < d.modalChangeNodes.length; ni++) {
      const pts = routes[ni]?.points;
      if (!Array.isArray(pts) || pts.length < 1) continue;
      const last = pts[pts.length - 1];
      const latlng = epsg2263XYToLatLng(last[0], last[1]);
      const node = d.modalChangeNodes[ni];
      const locKey = normalizeModalChangeKey(node?.modalChangeKey);
      const iconSrc = (locKey && locKey !== "other")
        ? (ORIGIN_TYPE_ICONS[locKey] ?? ORIGIN_TYPE_ICONS.storage_facility)
        : ORIGIN_TYPE_ICONS.storage_facility;
      const icon = markerIconFromSvg(iconSrc);
      const bLabel = d.modalChangeNodes.length <= 1 ? "B" : `B${ni + 1}`;
      const locOpt = SUPPLY_CHAIN_LOCATION_OPTIONS.find((o) => o.key === locKey);
      const locDesc = locKey === "other"
        ? (String(node?.otherDetail ?? "").trim() || "Others")
        : (locOpt?.label ?? locKey ?? "Stopping Point");
      const mk = L.marker(latlng, { icon, draggable: false });
      mk.bindPopup(`Stopping Point ${bLabel}<br/>${escapeHtml(itemLabel)}<br/>${escapeHtml(locDesc)}`);
      mk.on("click", (e) => {
        if (ui.scRouteDrawing) {
          L.DomEvent.stopPropagation(e);
          mk.closePopup();
          snapRouteToExistingLocation(mk.getLatLng(), null);
        }
      });
      layers.supplyChainDestinations.addLayer(mk);
    }
  }
}

/** Destination (diagram node C): icon from destination category; position from last point of last drawn leg. */
function addSupplyChainBranchDestinationMarkersToLayer(branches, itemLabels, kind) {
  if (!layers?.supplyChainDestinations) return;
  const isProduct = kind === BRANCH_KIND_PRODUCT;
  const roleLine = isProduct ? "Product destination" : "Raw material destination";
  const fallback = isProduct ? "Product" : "Material";
  const arr = Array.isArray(branches) ? branches : [];
  const labels = Array.isArray(itemLabels) ? itemLabels : [];
  for (let i = 0; i < arr.length; i++) {
    const b = arr[i];
    if (!b || b.originCategoryKey === RAW_MATERIAL_ORIGIN_SKIPPED_KEY) continue;
    const latlng = getSupplyChainRouteDestinationLatLng(b);
    if (!latlng) continue;
    const d = normalizeSupplyChainDiagram(b.supplyChainDiagram);
    const itemLabel = String(labels[i] ?? "").trim() || `${fallback} ${i + 1}`;
    const destDesc = formatSupplyChainDestinationDesc(d);
    const icon = markerIconForSupplyChainDestinationCategory(d);
    const mk = L.marker(latlng, { icon, draggable: false });
    mk.bindPopup(
      `${roleLine}<br/>${escapeHtml(itemLabel)}<br/>Destination type: ${escapeHtml(destDesc)}`
    );
    mk.on("click", (e) => {
      if (ui.scRouteDrawing) {
        L.DomEvent.stopPropagation(e);
        mk.closePopup();
        snapRouteToExistingLocation(mk.getLatLng(), null);
      }
    });
    layers.supplyChainDestinations.addLayer(mk);
  }
}

// --- Route icon animation ---
let routeAnimationHandles = [];

function stopRouteAnimations() {
  routeAnimationHandles.forEach((fn) => fn());
  routeAnimationHandles = [];
}

const FREQ_PERIOD_MULTIPLIER = { day: 2, week: 0.75, month: 0.25 };

function tripFrequencySpeedMultiplier(branch) {
  const count = branch?.tripFrequencyCount;
  const period = normalizeTripFrequencyPeriod(branch?.tripFrequencyPeriod);
  if (!(count >= 1) || !period) return 1;
  return count * (FREQ_PERIOD_MULTIPLIER[period] ?? 1);
}

function startRouteAnimationForLeg(latlngs, modeKey, speedMultiplier = 1) {
  const normalizedMode = normalizeTransportModeKey(modeKey);
  const frontSrc = ANIMATION_MODE_ICONS[normalizedMode];
  if (!frontSrc || latlngs.length < 2) return null;

  const ICON_SIZE = 56;
  const PIXELS_PER_SEC = 60 * speedMultiplier;
  const isTrain = normalizedMode === "train";
  const CAR_COUNT = 3;
  const CAR_SPACING = ICON_SIZE * 0.5;

  function makeMarker(src, zOffset) {
    const icon = L.divIcon({
      className: "",
      html: `<img class="routeAnimIcon" src="${escapeHtml(src)}" alt="" aria-hidden="true">`,
      iconSize: [ICON_SIZE, ICON_SIZE],
      iconAnchor: [ICON_SIZE / 2, ICON_SIZE / 2]
    });
    const m = L.marker(latlngs[0], { icon, interactive: false, zIndexOffset: zOffset });
    m.addTo(layers.routeAnimations);
    return m;
  }

  // Add cars first (lower z) so the front renders on top
  const carMarkers = isTrain
    ? Array.from({ length: CAR_COUNT }, (_, i) => makeMarker("Icons/Modes/Train_car.svg", 100 - i * 10))
    : [];
  const frontMarker = makeMarker(frontSrc, 200);

  let pixelsElapsed = 0;
  let lastTime = null;
  let rafId;

  function buildPixelRoute() {
    const pixelPts = latlngs.map((ll) => map.latLngToContainerPoint(ll));
    let totalPixels = 0;
    const segLens = [];
    for (let i = 0; i < pixelPts.length - 1; i++) {
      const dx = pixelPts[i + 1].x - pixelPts[i].x;
      const dy = pixelPts[i + 1].y - pixelPts[i].y;
      segLens.push(Math.sqrt(dx * dx + dy * dy));
      totalPixels += segLens[segLens.length - 1];
    }
    return { pixelPts, segLens, totalPixels };
  }

  function positionAt(pixelPts, segLens, px) {
    let remaining = Math.max(0, px);
    let pixPos = pixelPts[pixelPts.length - 1];
    let dirX = 0, dirY = 1;
    for (let i = 0; i < segLens.length; i++) {
      if (remaining <= segLens[i] || i === segLens.length - 1) {
        const t = segLens[i] > 0 ? remaining / segLens[i] : 0;
        pixPos = L.point(
          pixelPts[i].x + t * (pixelPts[i + 1].x - pixelPts[i].x),
          pixelPts[i].y + t * (pixelPts[i + 1].y - pixelPts[i].y)
        );
        dirX = pixelPts[i + 1].x - pixelPts[i].x;
        dirY = pixelPts[i + 1].y - pixelPts[i].y;
        break;
      }
      remaining -= segLens[i];
    }
    return { pixPos, dirX, dirY };
  }

  function applyToMarker(marker, pixelPts, segLens, px) {
    const { pixPos, dirX, dirY } = positionAt(pixelPts, segLens, px);
    marker.setLatLng(map.containerPointToLatLng(pixPos));
    const bearing = (Math.atan2(dirX, -dirY) * 180) / Math.PI;
    const el = marker.getElement();
    if (el) {
      const img = el.querySelector(".routeAnimIcon");
      if (img) img.style.transform = `rotate(${bearing - 90}deg)`;
    }
  }

  function step(ts) {
    if (lastTime === null) lastTime = ts;
    const dt = ts - lastTime;
    lastTime = ts;

    const { pixelPts, segLens, totalPixels } = buildPixelRoute();
    if (totalPixels < 1) { rafId = requestAnimationFrame(step); return; }

    pixelsElapsed = (pixelsElapsed + (PIXELS_PER_SEC * dt) / 1000) % totalPixels;

    applyToMarker(frontMarker, pixelPts, segLens, pixelsElapsed);

    for (let ci = 0; ci < carMarkers.length; ci++) {
      const carPx = pixelsElapsed - (ci + 1) * CAR_SPACING;
      const el = carMarkers[ci].getElement();
      if (carPx < 0) {
        if (el) el.style.visibility = "hidden";
      } else {
        if (el) el.style.visibility = "visible";
        applyToMarker(carMarkers[ci], pixelPts, segLens, carPx);
      }
    }

    rafId = requestAnimationFrame(step);
  }

  rafId = requestAnimationFrame(step);
  return () => {
    cancelAnimationFrame(rafId);
    if (layers?.routeAnimations) {
      layers.routeAnimations.removeLayer(frontMarker);
      carMarkers.forEach((m) => layers.routeAnimations.removeLayer(m));
    }
  };
}

function rebuildRouteAnimations() {
  stopRouteAnimations();
  if (!map || !layers?.routeAnimations) return;
  layers.routeAnimations.clearLayers();

  for (const kind of [BRANCH_KIND_RAW, BRANCH_KIND_PRODUCT]) {
    const { branchesKey } = industryBranchArrays(kind);
    const branches = state.industry[branchesKey] ?? [];
    for (const br of branches) {
      if (!br || br.originCategoryKey === RAW_MATERIAL_ORIGIN_SKIPPED_KEY) continue;
      const d = normalizeSupplyChainDiagram(br.supplyChainDiagram ?? defaultSupplyChainDiagram());
      const routes = normalizeSupplyChainTransportRoutesForBranch(br.supplyChainTransportRoutes, d);
      for (let li = 0; li < routes.length; li++) {
        const pts = routes[li]?.points;
        if (!Array.isArray(pts) || pts.length < 2) continue;
        const latlngs = surveyPointsToLatLngs(pts);
        if (latlngs.length < 2) continue;
        const modeKey = d.transportLegs[li]?.modeKey ?? "";
        const cancel = startRouteAnimationForLeg(latlngs, modeKey, tripFrequencySpeedMultiplier(br));
        if (cancel) routeAnimationHandles.push(cancel);
      }
    }
  }
}

function rebuildFromState() {
  layers.locations.clearLayers();
  layers.currentRoutes.clearLayers();
  layers.ibxRoutes.clearLayers();
  if (layers.supplyChainRoutes) layers.supplyChainRoutes.clearLayers();
  if (layers.supplyChainDestinations) layers.supplyChainDestinations.clearLayers();
  if (layers.supplyChainDraft) layers.supplyChainDraft.clearLayers();

  for (const loc of state.locations) {
    const meta = LOCATION_TYPES[loc.locationType] ?? LOCATION_TYPES.other;
    const latlng =
      typeof loc.x === "number" && typeof loc.y === "number"
        ? epsg2263XYToLatLng(loc.x, loc.y)
        : L.latLng(loc.lat, loc.lng);
    const marker = L.marker(latlng, { icon: markerIcon(loc.locationType, meta.color), draggable: false });
    marker._snapLocId = loc.id;
    const companyNote =
      loc.locationType === "workplace" && state.industry?.companyName
        ? `<br/>${escapeHtml(state.industry.companyName)}`
        : "";
    marker.bindPopup(`${meta.label}${companyNote}<br/>ID: ${loc.id}`);
    marker.on("click", (e) => {
      if (ui.scRouteDrawing) {
        L.DomEvent.stopPropagation(e);
        marker.closePopup();
        snapRouteToExistingLocation(marker.getLatLng(), marker._snapLocId);
      }
    });
    layers.locations.addLayer(marker);
  }

  addSupplyChainBranchOriginMarkersToLocationsLayer(
    state.industry?.rawMaterialBranches,
    state.industry?.rawMaterials,
    BRANCH_KIND_RAW
  );
  addSupplyChainBranchOriginMarkersToLocationsLayer(
    state.industry?.productBranches,
    state.industry?.products,
    BRANCH_KIND_PRODUCT
  );

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

  addSupplyChainBranchRoutePolylinesToLayer(
    state.industry?.rawMaterialBranches,
    state.industry?.rawMaterials,
    BRANCH_KIND_RAW
  );
  addSupplyChainBranchRoutePolylinesToLayer(
    state.industry?.productBranches,
    state.industry?.products,
    BRANCH_KIND_PRODUCT
  );

  addSupplyChainBranchStoppingPointMarkersToLayer(
    state.industry?.rawMaterialBranches,
    state.industry?.rawMaterials,
    BRANCH_KIND_RAW
  );
  addSupplyChainBranchStoppingPointMarkersToLayer(
    state.industry?.productBranches,
    state.industry?.products,
    BRANCH_KIND_PRODUCT
  );
  addSupplyChainBranchDestinationMarkersToLayer(
    state.industry?.rawMaterialBranches,
    state.industry?.rawMaterials,
    BRANCH_KIND_RAW
  );
  addSupplyChainBranchDestinationMarkersToLayer(
    state.industry?.productBranches,
    state.industry?.products,
    BRANCH_KIND_PRODUCT
  );

  if (ui.scRouteDrawing && layers.supplyChainDraft) {
    refreshSupplyChainRouteDraftVisuals();
  }

  rebuildRouteAnimations();

  syncParticipantLeftPanel();
  syncConductorParticipantLeftPanel();

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

  if (SURVEY_MODE === "participant") {
    const profileCard = document.getElementById("participantProfileCard");
    const companyList = document.getElementById("participantProfileCompanyList");
    const companyEmpty = document.getElementById("participantProfileCompanyEmpty");
    const roleList = document.getElementById("participantProfileRoleList");
    const roleEmpty = document.getElementById("participantProfileRoleEmpty");
    const productsList = document.getElementById("participantProfileProductsList");
    const productsEmpty = document.getElementById("participantProfileProductsEmpty");
    if (
      profileCard &&
      companyList &&
      companyEmpty &&
      roleList &&
      roleEmpty &&
      productsList &&
      productsEmpty
    ) {
      const company = String(state.industry?.companyName ?? "").trim();
      const roleText = formatParticipantRoleSummary(state.industry);
      const goodsText = formatParticipantGoodsSummary(state.industry);
      const dash = "—";

      const companyHtml = participantCompanyProfileListInnerHtml();
      companyList.innerHTML = companyHtml;
      const hasCompanyRow = companyHtml.length > 0;
      companyList.classList.toggle("is-hidden", !hasCompanyRow);
      companyEmpty.classList.toggle("is-hidden", hasCompanyRow);
      companyEmpty.textContent = dash;

      const roleHtml = participantRoleProfileListInnerHtml();
      roleList.innerHTML = roleHtml;
      const hasRoleRow = roleHtml.length > 0;
      roleList.classList.toggle("is-hidden", !hasRoleRow);
      roleEmpty.classList.toggle("is-hidden", hasRoleRow);
      roleEmpty.textContent = dash;

      const goodsListHtml = participantGoodsProfileListInnerHtml(state.industry);
      productsList.innerHTML = goodsListHtml;
      const hasProductRows = goodsListHtml.length > 0;
      productsList.classList.toggle("is-hidden", !hasProductRows);
      productsEmpty.classList.toggle("is-hidden", hasProductRows);
      productsEmpty.textContent = dash;
      const show = Boolean(company || roleText || goodsText) || participantRawMaterialsIsComplete();
      profileCard.classList.toggle("is-hidden", !show);
    }
    const redrawLocBtn = document.getElementById("participantProfileRedrawWorkplaceBtn");
    if (redrawLocBtn) {
      const hasWp = participantHasWorkplaceLocation();
      redrawLocBtn.disabled = !hasWp;
      redrawLocBtn.title = hasWp
        ? "Remove the current company location and click the map to place it again"
        : "Place your company on the map first; then you can redraw the location here";
    }
  }

  if (SURVEY_MODE === "conductor") {
    const condInd = document.getElementById("conductorIndustryLabel");
    if (condInd) {
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
      const goodsSuffix = formatParticipantGoodsSummary(state.industry);
      const goodsPart = goodsSuffix ? ` · Goods: ${goodsSuffix}` : "";
      const rawSuffix = formatParticipantRawMaterialsSummary(state.industry);
      const rawPart = rawSuffix ? ` · Raw materials: ${rawSuffix}` : "";
      const prodSuffix = formatParticipantProductsSummary(state.industry);
      const prodPart = prodSuffix ? ` · Products: ${prodSuffix}` : "";
      condInd.textContent = c ? `Industry / company: ${c}${roleSuffix}${goodsPart}${rawPart}${prodPart}` : "";
      condInd.classList.toggle("is-hidden", !c || !viewingParticipantId);
    }

    const cProfileCard = document.getElementById("conductorProfileCard");
    const cCompanyList = document.getElementById("conductorProfileCompanyList");
    const cCompanyEmpty = document.getElementById("conductorProfileCompanyEmpty");
    const cRoleList = document.getElementById("conductorProfileRoleList");
    const cRoleEmpty = document.getElementById("conductorProfileRoleEmpty");
    const cProductsList = document.getElementById("conductorProfileProductsList");
    const cProductsEmpty = document.getElementById("conductorProfileProductsEmpty");
    if (
      cProfileCard &&
      cCompanyList &&
      cCompanyEmpty &&
      cRoleList &&
      cRoleEmpty &&
      cProductsList &&
      cProductsEmpty
    ) {
      const company = String(state.industry?.companyName ?? "").trim();
      const roleText = formatParticipantRoleSummary(state.industry);
      const goodsText = formatParticipantGoodsSummary(state.industry);
      const dash = "—";

      const companyHtml = participantCompanyProfileListInnerHtml();
      cCompanyList.innerHTML = companyHtml;
      const hasCompanyRow = companyHtml.length > 0;
      cCompanyList.classList.toggle("is-hidden", !hasCompanyRow);
      cCompanyEmpty.classList.toggle("is-hidden", hasCompanyRow);
      cCompanyEmpty.textContent = dash;

      const roleHtml = participantRoleProfileListInnerHtml();
      cRoleList.innerHTML = roleHtml;
      const hasRoleRow = roleHtml.length > 0;
      cRoleList.classList.toggle("is-hidden", !hasRoleRow);
      cRoleEmpty.classList.toggle("is-hidden", hasRoleRow);
      cRoleEmpty.textContent = dash;

      const goodsListHtml = participantGoodsProfileListInnerHtml(state.industry);
      cProductsList.innerHTML = goodsListHtml;
      const hasProductRows = goodsListHtml.length > 0;
      cProductsList.classList.toggle("is-hidden", !hasProductRows);
      cProductsEmpty.classList.toggle("is-hidden", hasProductRows);
      cProductsEmpty.textContent = dash;
      const show =
        Boolean(viewingParticipantId) &&
        (Boolean(company || roleText || goodsText) || participantRawMaterialsIsComplete());
      cProfileCard.classList.toggle("is-hidden", !show);
    }
  }

  updateParticipantCompanyBanner();
  updateParticipantRawMaterialOriginBanner();
  if (SURVEY_MODE === "participant") syncRawMaterialOriginMapSkipVisibility();
}

function shouldShowParticipantCompanyBanner() {
  const hasWorkplace = state.locations.some((l) => l.locationType === "workplace");
  const mapGatesOpen = Boolean(
    document.getElementById("industryGate")?.classList.contains("is-open") ||
      document.getElementById("roleGate")?.classList.contains("is-open") ||
      document.getElementById("goodsGate")?.classList.contains("is-open") ||
      document.getElementById("rawMaterialsGate")?.classList.contains("is-open") ||
      document.getElementById("rawMaterialSupplyChainIntroGate")?.classList.contains("is-open") ||
      document.getElementById("rawMaterialSupplyChainDiagramGate")?.classList.contains("is-open") ||
      document.getElementById("rawMaterialOriginGate")?.classList.contains("is-open") ||
      document.getElementById("rawMaterialTripFrequencyGate")?.classList.contains("is-open") ||
      document.getElementById("participantProductsGate")?.classList.contains("is-open")
  );
  return (
    SURVEY_MODE === "participant" &&
    !readOnly &&
    ui.activeStep === "locations" &&
    !hasWorkplace &&
    !mapGatesOpen &&
    !ui.scRouteDrawing
  );
}

function syncParticipantCompanyBannerVisibility() {
  const el = document.getElementById("participantCompanyBanner");
  if (!el) return;
  const show = shouldShowParticipantCompanyBanner();
  if (!show) companyBannerPointerOnMap = false;
  else if (map?.getContainer()?.matches?.(":hover")) companyBannerPointerOnMap = true;
  const visible = show && companyBannerPointerOnMap;
  el.classList.toggle("is-hidden", !visible);
  el.setAttribute("aria-hidden", visible ? "false" : "true");
  if (!visible) {
    el.style.left = "";
    el.style.top = "";
    return;
  }
  if (!el.style.left) {
    const wrap = el.closest(".mapWrap");
    const r = wrap?.getBoundingClientRect();
    if (r) positionParticipantCompanyBanner(r.left + r.width / 2, r.top + r.height / 2);
  }
}

function positionParticipantCompanyBanner(clientX, clientY) {
  const el = document.getElementById("participantCompanyBanner");
  const wrap = el?.closest(".mapWrap");
  if (!el || !wrap || el.classList.contains("is-hidden")) return;
  const rect = wrap.getBoundingClientRect();
  const offX = 14;
  const offY = 14;
  let x = clientX - rect.left + offX;
  let y = clientY - rect.top + offY;
  const bw = el.offsetWidth || 1;
  const bh = el.offsetHeight || 1;
  const pad = 8;
  x = Math.min(Math.max(pad, x), Math.max(pad, rect.width - bw - pad));
  y = Math.min(Math.max(pad, y), Math.max(pad, rect.height - bh - pad));
  el.style.left = `${x}px`;
  el.style.top = `${y}px`;
}

function updateParticipantCompanyBanner() {
  syncParticipantCompanyBannerVisibility();
}

function shouldShowParticipantRawMaterialOriginBanner() {
  return (
    SURVEY_MODE === "participant" &&
    !readOnly &&
    ui.activeStep === "locations" &&
    ui.scBranchMap !== null
  );
}

function syncParticipantRawMaterialOriginBannerVisibility() {
  const el = document.getElementById("participantRawMaterialOriginBanner");
  if (!el) return;
  const show = shouldShowParticipantRawMaterialOriginBanner();
  if (!show) rawMaterialOriginBannerPointerOnMap = false;
  else if (map?.getContainer()?.matches?.(":hover")) rawMaterialOriginBannerPointerOnMap = true;
  const visible = show && rawMaterialOriginBannerPointerOnMap;
  const idx = ui.scBranchMap?.branchIndex;
  const bk = ui.scBranchMap?.kind;
  const { items } = bk != null ? industryBranchArrays(bk) : { items: state.industry.rawMaterials ?? [] };
  const matName =
    idx != null
      ? String(items[idx] ?? "").trim() ||
        (bk === BRANCH_KIND_PRODUCT ? "this product" : "this material")
      : "this material";
  el.textContent = `Please Mark the Origin for ${matName}`;
  el.classList.toggle("is-hidden", !visible);
  el.setAttribute("aria-hidden", visible ? "false" : "true");
  if (!visible) {
    el.style.left = "";
    el.style.top = "";
    return;
  }
  if (!el.style.left) {
    const wrap = el.closest(".mapWrap");
    const r = wrap?.getBoundingClientRect();
    if (r) positionParticipantRawMaterialOriginBanner(r.left + r.width / 2, r.top + r.height / 2);
  }
}

function positionParticipantRawMaterialOriginBanner(clientX, clientY) {
  const el = document.getElementById("participantRawMaterialOriginBanner");
  const wrap = el?.closest(".mapWrap");
  if (!el || !wrap || el.classList.contains("is-hidden")) return;
  const rect = wrap.getBoundingClientRect();
  const offX = 14;
  const offY = 14;
  let x = clientX - rect.left + offX;
  let y = clientY - rect.top + offY;
  const bw = el.offsetWidth || 1;
  const bh = el.offsetHeight || 1;
  const pad = 8;
  x = Math.min(Math.max(pad, x), Math.max(pad, rect.width - bw - pad));
  y = Math.min(Math.max(pad, y), Math.max(pad, rect.height - bh - pad));
  el.style.left = `${x}px`;
  el.style.top = `${y}px`;
}

function updateParticipantRawMaterialOriginBanner() {
  syncParticipantRawMaterialOriginBannerVisibility();
}

function setStep(step) {
  // Participant UI has no step tabs or per-step side panels; keep one map view and question-driven flows.
  if (SURVEY_MODE === "participant") {
    ui.activeStep = "locations";
    updateParticipantCompanyBanner();
    updateParticipantRawMaterialOriginBanner();
    syncParticipantLeftPanel();
    if (!map) return;
    if (layers?.locations && !map.hasLayer(layers.locations)) layers.locations.addTo(map);
    if (layers?.pendingLocation && !map.hasLayer(layers.pendingLocation)) layers.pendingLocation.addTo(map);
    if (layers?.currentRoutes && !map.hasLayer(layers.currentRoutes)) layers.currentRoutes.addTo(map);
    if (layers?.ibxRoutes && !map.hasLayer(layers.ibxRoutes)) layers.ibxRoutes.addTo(map);
    if (layers?.ibxLine && !map.hasLayer(layers.ibxLine)) layers.ibxLine.addTo(map);
    if (layers?.ibxStations && !map.hasLayer(layers.ibxStations)) layers.ibxStations.addTo(map);
    return;
  }

  // Conductor: no step tabs; map always shows locations, routes, supply-chain layers, and IBX line/stations.
  if (SURVEY_MODE === "conductor") {
    ui.activeStep = "locations";
    updateParticipantCompanyBanner();
    updateParticipantRawMaterialOriginBanner();
    syncConductorParticipantLeftPanel();
    if (!map) return;
    if (layers?.locations && !map.hasLayer(layers.locations)) layers.locations.addTo(map);
    if (layers?.pendingLocation && !map.hasLayer(layers.pendingLocation)) layers.pendingLocation.addTo(map);
    if (layers?.currentRoutes && !map.hasLayer(layers.currentRoutes)) layers.currentRoutes.addTo(map);
    if (layers?.ibxRoutes && !map.hasLayer(layers.ibxRoutes)) layers.ibxRoutes.addTo(map);
    if (layers?.ibxLine && !map.hasLayer(layers.ibxLine)) layers.ibxLine.addTo(map);
    if (layers?.ibxStations && !map.hasLayer(layers.ibxStations)) layers.ibxStations.addTo(map);
    return;
  }

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
  updateParticipantRawMaterialOriginBanner();

  if (step === "locations") {
    syncParticipantLeftPanel();
    syncConductorParticipantLeftPanel();
  }

  if (step !== "locations") clearPendingLocation();

  if (!map) return;
  if (layers?.locations) {
    if (step === "locations") {
      if (!map.hasLayer(layers.locations)) layers.locations.addTo(map);
    } else if (map.hasLayer(layers.locations)) {
      layers.locations.removeFrom(map);
    }
  }

  if (layers?.pendingLocation) {
    if (step === "locations") {
      if (!map.hasLayer(layers.pendingLocation)) layers.pendingLocation.addTo(map);
    } else if (map.hasLayer(layers.pendingLocation)) {
      layers.pendingLocation.removeFrom(map);
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
  clearPendingLocation();
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
  updateMapCursor();
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
    if (!participantNeedsRoleGate()) maybeOpenParticipantGoodsGate();
    if (!participantNeedsRoleGate() && !participantNeedsGoodsGate()) {
      maybeOpenParticipantRawMaterialsGate();
    }
    setParticipantMapHintAfterIndustryGate();
  }
}

function clearLocations() {
  if (readOnly) return;
  clearPendingLocation();
  state.locations = [];
  state.industry = {
    ...state.industry,
    roleKey: "",
    roleOtherDetail: "",
    goodsCategoryKeys: [],
    goodsOtherDetail: "",
    rawMaterials: [],
    rawMaterialBranches: [],
    products: [],
    productBranches: []
  };
  ui.scBranchMap = null;
  ui.scOriginIndex = null;
  ui.scIntroIndex = null;
  ui.scDiagramIndex = null;
  ui.scTripFreqIndex = null;
  resetSupplyChainRouteDraft();
  document.getElementById("roleGate")?.classList.remove("is-open");
  document.getElementById("goodsGate")?.classList.remove("is-open");
  document.getElementById("rawMaterialsGate")?.classList.remove("is-open");
  document.getElementById("rawMaterialSupplyChainIntroGate")?.classList.remove("is-open");
  document.getElementById("rawMaterialSupplyChainDiagramGate")?.classList.remove("is-open");
  document.getElementById("rawMaterialTripFrequencyGate")?.classList.remove("is-open");
  document.getElementById("participantProductsGate")?.classList.remove("is-open");
  document.getElementById("rawMaterialOriginGate")?.classList.remove("is-open");
  syncParticipantMapGateOverlay();
  layers.locations.clearLayers();
  if (layers.supplyChainRoutes) layers.supplyChainRoutes.clearLayers();
  if (layers.supplyChainDestinations) layers.supplyChainDestinations.clearLayers();
  if (layers.supplyChainDraft) layers.supplyChainDraft.clearLayers();
  saveState();
  uiUpdateStats();
}

function loadIBXGeoJSONToLayer(geojson) {
  const normalized = normalizeIBXGeoJSONForLeaflet(geojson);
  layers.ibxLine.clearLayers();

  L.geoJSON(normalized, {
    style: { color: "#111", weight: 10, opacity: 0.85, dashArray: "2 5", lineCap: "butt" }
  }).addTo(layers.ibxLine);

  state.ibxLine.loaded = true;
}

function loadIBXStationsGeoJSONToLayer(geojson) {
  const normalized = normalizeIBXGeoJSONForLeaflet(geojson);
  layers.ibxStations.clearLayers();

  const layer = L.geoJSON(normalized, {
    pointToLayer: (feature, latlng) => {
      return L.circleMarker(latlng, {
        radius: 6,
        color: "#111",
        weight: 1,
        fillColor: "#fff",
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
  const industryGoodsKeys = Array.isArray(stateObj.industry?.goodsCategoryKeys)
    ? stateObj.industry.goodsCategoryKeys.join("|")
    : "";
  const industryGoodsOther = String(stateObj.industry?.goodsOtherDetail ?? "");
  const industryRawMaterials = Array.isArray(stateObj.industry?.rawMaterials)
    ? stateObj.industry.rawMaterials.join("|")
    : "";
  let industryRawMaterialBranchesJson = "";
  try {
    const mats = stateObj.industry?.rawMaterials ?? [];
    const br = stateObj.industry?.rawMaterialBranches ?? [];
    industryRawMaterialBranchesJson = JSON.stringify(
      mats.map((label, i) => {
        const b = br[i] || {};
        return {
          material: String(label ?? "").trim(),
          origin: b.originCategoryKey ?? "",
          originOther: String(b.originOtherDetail ?? ""),
          originX: typeof b.originX === "number" ? b.originX : null,
          originY: typeof b.originY === "number" ? b.originY : null,
          originMapSkipped: Boolean(b.originMapSkipped),
          supplyChainIntroAcknowledged: Boolean(b.supplyChainIntroAcknowledged),
          supplyChainDiagram: b.supplyChainDiagram
            ? normalizeSupplyChainDiagram(b.supplyChainDiagram)
            : null,
          supplyChainTransportRoutes: normalizeSupplyChainTransportRoutesForBranch(
            b.supplyChainTransportRoutes,
            b.supplyChainDiagram
          )
        };
      })
    );
  } catch {
    industryRawMaterialBranchesJson = "";
  }
  const industryProducts = Array.isArray(stateObj.industry?.products)
    ? stateObj.industry.products.join("|")
    : "";
  let industryProductBranchesJson = "";
  try {
    const prods = stateObj.industry?.products ?? [];
    const br = stateObj.industry?.productBranches ?? [];
    industryProductBranchesJson = JSON.stringify(
      prods.map((label, i) => {
        const b = br[i] || {};
        return {
          product: String(label ?? "").trim(),
          origin: b.originCategoryKey ?? "",
          originOther: String(b.originOtherDetail ?? ""),
          originX: typeof b.originX === "number" ? b.originX : null,
          originY: typeof b.originY === "number" ? b.originY : null,
          originMapSkipped: Boolean(b.originMapSkipped),
          supplyChainIntroAcknowledged: Boolean(b.supplyChainIntroAcknowledged),
          supplyChainDiagram: b.supplyChainDiagram
            ? normalizeSupplyChainDiagram(b.supplyChainDiagram)
            : null,
          supplyChainTransportRoutes: normalizeSupplyChainTransportRoutesForBranch(
            b.supplyChainTransportRoutes,
            b.supplyChainDiagram
          )
        };
      })
    );
  } catch {
    industryProductBranchesJson = "";
  }
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
      ? [
          "participant_id",
          "participant_label",
          "industry_company",
          "industry_role",
          "industry_role_detail",
          "industry_goods_keys",
          "industry_goods_other",
          "industry_raw_materials",
          "industry_raw_material_branches_json",
          "industry_products",
          "industry_product_branches_json",
          ...baseHeader
        ]
      : baseHeader
  );

  const rowPrefix = withP
    ? [
        pid,
        plab,
        industryCo,
        industryRole,
        industryRoleDetail,
        industryGoodsKeys,
        industryGoodsOther,
        industryRawMaterials,
        industryRawMaterialBranchesJson,
        industryProducts,
        industryProductBranchesJson
      ]
    : [];

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
    "participant_id,participant_label,industry_company,industry_role,industry_role_detail,industry_goods_keys,industry_goods_other,industry_raw_materials,industry_raw_material_branches_json,industry_products,industry_product_branches_json,record_type,section,location_type,id,x_2263,y_2263,lat,lng,route_mode,route_mode_key,length_miles,transfer_applied_gold,transfer_cost_gold,segment_cost_gold,section_total_cost_gold_after";
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
      if (SURVEY_MODE === "participant") clearPendingLocation();
      ui.locationType = btn.dataset.locationType;
      document.getElementById("mapHint").textContent =
        ui.locationType === "none"
          ? "No placement mode selected. Switch modes to place points."
          : `Placement: click the map to add a ${
              LOCATION_TYPES[ui.locationType]?.label ?? ui.locationType
            } point.`;
      updateMapCursor();
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

  L.tileLayer("https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png", {
    attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>',
    subdomains: "abcd",
    maxZoom: 20
  }).addTo(map);

  layers = {
    locations: L.layerGroup(),
    pendingLocation: L.layerGroup(),
    currentRoutes: L.layerGroup(),
    supplyChainRoutes: L.layerGroup(),
    supplyChainDestinations: L.layerGroup(),
    supplyChainDraft: L.layerGroup(),
    routeAnimations: L.layerGroup(),
    ibxLine: L.layerGroup(),
    ibxStations: L.layerGroup(),
    ibxRoutes: L.layerGroup()
  };

  layers.locations.addTo(map);
  layers.pendingLocation.addTo(map);
  layers.currentRoutes.addTo(map);
  layers.supplyChainRoutes.addTo(map);
  layers.supplyChainDestinations.addTo(map);
  layers.supplyChainDraft.addTo(map);
  layers.routeAnimations.addTo(map);
  layers.ibxRoutes.addTo(map);
  // IBX line/stations: always on map for conductor; standalone toggles via setStep (ibxRoutes step).

  map.on("mousemove", (e) => {
    if (readOnly) return;
    if (SURVEY_MODE !== "participant" || ui.activeStep !== "locations") return;
    const dr = ui.scRouteDrawing;
    if (!dr) return;
    dr.previewCursorLatLng = e.latlng;
    updateSupplyChainRoutePreview();
    const oe = e.originalEvent;
    const hint = document.getElementById("participantSupplyChainRouteCursorHint");
    if (oe && hint && !readOnly) {
      hint.classList.remove("is-hidden");
      hint.setAttribute("aria-hidden", "false");
      positionParticipantSupplyChainRouteCursorHint(oe.clientX, oe.clientY);
    }
  });

  map.on("mouseout", () => {
    if (readOnly) return;
    const dr = ui.scRouteDrawing;
    if (!dr) return;
    dr.previewCursorLatLng = null;
    updateSupplyChainRoutePreview();
    const hint = document.getElementById("participantSupplyChainRouteCursorHint");
    if (hint) {
      hint.classList.add("is-hidden");
      hint.style.left = "";
      hint.style.top = "";
    }
  });

  map.on("click", (e) => {
    if (readOnly) return;
    if (
      SURVEY_MODE === "participant" &&
      ui.activeStep === "locations" &&
      ui.scRouteDrawing
    ) {
      appendSupplyChainTransportRoutePoint(e.latlng);
      return;
    }
    if (ui.drawing.active) {
      ui.drawing.points.push(e.latlng);
      const [x, y] = gpsToEPSG2263(e.latlng.lng, e.latlng.lat);
      ui.drawing.points2263.push([x, y]);
      updateDraftPolyline();
      return;
    }

    if (
      SURVEY_MODE === "participant" &&
      !readOnly &&
      ui.activeStep === "locations" &&
      ui.scBranchMap !== null
    ) {
      setPendingBranchOrigin(ui.scBranchMap.kind, ui.scBranchMap.branchIndex, e.latlng);
      return;
    }

    if (ui.locationType !== "none") {
      if (
        SURVEY_MODE === "participant" &&
        !readOnly &&
        ui.activeStep === "locations"
      ) {
        setPendingLocation(ui.locationType, e.latlng);
      } else {
        addLocation(ui.locationType, e.latlng);
      }
    }
  });

  const mapContainer = map.getContainer();
  const onParticipantCompanyBannerPointer = (e) => {
    if (!shouldShowParticipantCompanyBanner()) return;
    companyBannerPointerOnMap = true;
    syncParticipantCompanyBannerVisibility();
    positionParticipantCompanyBanner(e.clientX, e.clientY);
  };
  mapContainer.addEventListener("mousemove", onParticipantCompanyBannerPointer);
  mapContainer.addEventListener("mouseenter", onParticipantCompanyBannerPointer);
  mapContainer.addEventListener("mouseleave", () => {
    companyBannerPointerOnMap = false;
    syncParticipantCompanyBannerVisibility();
  });

  const onParticipantRawMaterialOriginBannerPointer = (e) => {
    if (!shouldShowParticipantRawMaterialOriginBanner()) return;
    rawMaterialOriginBannerPointerOnMap = true;
    syncParticipantRawMaterialOriginBannerVisibility();
    positionParticipantRawMaterialOriginBanner(e.clientX, e.clientY);
  };
  mapContainer.addEventListener("mousemove", onParticipantRawMaterialOriginBannerPointer);
  mapContainer.addEventListener("mouseenter", onParticipantRawMaterialOriginBannerPointer);
  mapContainer.addEventListener("mouseleave", () => {
    rawMaterialOriginBannerPointerOnMap = false;
    syncParticipantRawMaterialOriginBannerVisibility();
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
  const hintEl = document.getElementById("mapHint");
  try {
    if (!hintEl) return;

    if (SURVEY_MODE === "participant" && ui.scBranchMap !== null) {
      const bk = ui.scBranchMap.kind;
      const idx = ui.scBranchMap.branchIndex;
      const { items } = industryBranchArrays(bk);
      const name =
        String(items[idx] ?? "").trim() || (bk === BRANCH_KIND_PRODUCT ? "this product" : "this material");
      ui.locationType = "none";
      hintEl.textContent = `Originating location for “${name}”: click the map to place a point, then confirm with √ or cancel with ×. You can also skip if you don't know the location.`;
      syncRawMaterialOriginMapSkipVisibility();
      return;
    }

    if (SURVEY_MODE === "participant" && ui.scRouteDrawing) {
      const dr = ui.scRouteDrawing;
      const { items, branchesKey } = industryBranchArrays(dr.branchKind);
      const name =
        String(items[dr.branchIndex] ?? "").trim() ||
        (dr.branchKind === BRANCH_KIND_PRODUCT ? "this product" : "this material");
      const d = normalizeSupplyChainDiagram(
        state.industry[branchesKey][dr.branchIndex]?.supplyChainDiagram
      );
      const leg = d.transportLegs[dr.legIndex];
      const modeLab = formatSupplyChainTransportModeLabel(leg);
      const tn = dr.legIndex + 1;
      ui.locationType = "none";
      const startPhrase =
        dr.legIndex === 0
          ? "the originating location (first point is set automatically)"
          : "the previous Stopping Point (first point is set automatically)";
      hintEl.textContent = `“${name}”: draw Transportation ${tn} (${modeLab}) on the map. The route starts from ${startPhrase}. Click to add more points along the route. Click √ next to the end label when it marks the correct stop for this leg.`;
      syncRawMaterialOriginMapSkipVisibility();
      return;
    }

    const hasWorkplace = state.locations.some((l) => l.locationType === "workplace");
    if (!hasWorkplace) {
      ui.locationType = "workplace";
      hintEl.textContent =
        "Workplace (your company): click the map once to mark where that company is located.";
    } else {
      ui.locationType = "none";
      hintEl.textContent =
        "Follow the questions to continue. The map will be used when a step asks you to mark a location or draw a route.";
    }
    syncRawMaterialOriginMapSkipVisibility();
  } finally {
    updateParticipantSupplyChainRouteEditingUI();
  }
}

function syncParticipantMapGateOverlay() {
  if (SURVEY_MODE !== "participant") return;
  const mapWrap = document.querySelector("#app .mapWrap");
  if (!mapWrap) return;
  const ind = document.getElementById("industryGate");
  const role = document.getElementById("roleGate");
  const goods = document.getElementById("goodsGate");
  const rawMaterials = document.getElementById("rawMaterialsGate");
  const rawMatSupplyIntro = document.getElementById("rawMaterialSupplyChainIntroGate");
  const rawMatSupplyDiagram = document.getElementById("rawMaterialSupplyChainDiagramGate");
  const rawMatOrigin = document.getElementById("rawMaterialOriginGate");
  const rawMatTripFreq = document.getElementById("rawMaterialTripFrequencyGate");
  const anyOpen = Boolean(
    ind?.classList.contains("is-open") ||
      role?.classList.contains("is-open") ||
      goods?.classList.contains("is-open") ||
      rawMaterials?.classList.contains("is-open") ||
      rawMatSupplyIntro?.classList.contains("is-open") ||
      rawMatSupplyDiagram?.classList.contains("is-open") ||
      rawMatOrigin?.classList.contains("is-open") ||
      rawMatTripFreq?.classList.contains("is-open") ||
      document.getElementById("participantProductsGate")?.classList.contains("is-open")
  );
  mapWrap.classList.toggle("map-gate-open", anyOpen);
  if (anyOpen) document.body.dataset.mapGateOpen = "1";
  else delete document.body.dataset.mapGateOpen;
  updateParticipantCompanyBanner();
  updateParticipantRawMaterialOriginBanner();
  updateParticipantSupplyChainRouteEditingUI();
  syncRawMaterialOriginMapSkipVisibility();
  updateMapCursor();
}

function syncRawMaterialOriginMapSkipVisibility() {
  const wrap = document.getElementById("rawMaterialOriginMapSkipWrap");
  if (!wrap) return;
  const show =
    SURVEY_MODE === "participant" &&
    !readOnly &&
    ui.activeStep === "locations" &&
    ui.scBranchMap !== null;
  wrap.classList.toggle("is-hidden", !show);
  wrap.setAttribute("aria-hidden", show ? "false" : "true");
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

function participantGoodsIsComplete() {
  const keys = state.industry?.goodsCategoryKeys;
  if (!Array.isArray(keys) || keys.length === 0) return false;
  if (keys.includes("other")) {
    return String(state.industry?.goodsOtherDetail ?? "").trim().length > 0;
  }
  return true;
}

function participantNeedsGoodsGate() {
  return (
    SURVEY_MODE === "participant" &&
    participantHasWorkplaceLocation() &&
    participantIndustryRoleIsComplete() &&
    !participantGoodsIsComplete()
  );
}

function participantRawMaterialsIsComplete() {
  const arr = state.industry?.rawMaterials;
  if (!Array.isArray(arr)) return false;
  return arr.some((s) => String(s ?? "").trim().length > 0);
}

function participantNeedsRawMaterialsGate() {
  return (
    SURVEY_MODE === "participant" &&
    participantHasWorkplaceLocation() &&
    participantIndustryRoleIsComplete() &&
    participantGoodsIsComplete() &&
    !participantRawMaterialsIsComplete()
  );
}

function formatParticipantRawMaterialsSummary(industry) {
  const ind = industry ?? state.industry;
  const arr = ind?.rawMaterials;
  if (!Array.isArray(arr)) return "";
  const parts = arr.map((s) => String(s ?? "").trim()).filter(Boolean);
  return parts.length ? parts.join("; ") : "";
}

function formatParticipantProductsSummary(industry) {
  const ind = industry ?? state.industry;
  const arr = ind?.products;
  if (!Array.isArray(arr)) return "";
  const parts = arr.map((s) => String(s ?? "").trim()).filter(Boolean);
  return parts.length ? parts.join("; ") : "";
}

function formatParticipantGoodsSummary(industry) {
  const ind = industry ?? state.industry;
  if (!ind) return "";
  const keys = ind.goodsCategoryKeys;
  if (!Array.isArray(keys) || keys.length === 0) return "";
  const parts = [];
  for (const k of keys) {
    const opt = GOODS_CATEGORY_OPTIONS.find((o) => o.key === k);
    if (!opt) continue;
    if (k === "other") {
      const d = String(ind.goodsOtherDetail ?? "").trim();
      parts.push(d ? `Others (${d})` : "Others");
    } else {
      parts.push(opt.label);
    }
  }
  return parts.join("; ");
}

function formatParticipantRoleSummary(industry) {
  const rk = String(industry?.roleKey ?? "").trim();
  if (rk === "manager") return "Manager";
  if (rk === "worker") return "Worker";
  if (rk === "transporter") return "Transporter";
  if (rk === "other") {
    const d = String(industry?.roleOtherDetail ?? "").trim();
    return d ? `Others (${d})` : "Others";
  }
  return "";
}

function openParticipantIndustryGateForEdit() {
  if (SURVEY_MODE !== "participant") return;
  const gate = document.getElementById("industryGate");
  const input = document.getElementById("industryCompanyInput");
  if (!gate || !input) return;
  document.getElementById("roleGate")?.classList.remove("is-open");
  document.getElementById("goodsGate")?.classList.remove("is-open");
  input.value = String(state.industry?.companyName ?? "");
  gate.classList.add("is-open");
  syncParticipantMapGateOverlay();
  requestAnimationFrame(() => input.focus());
}

function initParticipantIndustryGateOnce() {
  if (SURVEY_MODE !== "participant" || document.body.dataset.participantIndustryGateBound === "1") return;
  const gate = document.getElementById("industryGate");
  const input = document.getElementById("industryCompanyInput");
  const btn = document.getElementById("industryContinueBtn");
  if (!gate || !input || !btn) return;
  document.body.dataset.participantIndustryGateBound = "1";

  input.addEventListener("keydown", (e) => {
    if (e.key === "Enter") btn.click();
  });

  btn.addEventListener("click", () => {
    const name = String(input?.value ?? "").trim();
    if (!name) {
      window.alert("Please enter the industry or company you work in.");
      return;
    }
    state.industry = { ...state.industry, companyName: name };
    gate.classList.remove("is-open");
    syncParticipantMapGateOverlay();
    void flushSaveToServer();
    rebuildFromState();
    uiUpdateStats();
    setParticipantMapHintAfterIndustryGate();
    if (map) {
      requestAnimationFrame(() => map.invalidateSize());
      setTimeout(() => map.invalidateSize(), 200);
    }
  });
}

/** Remove the workplace marker and enter placement mode so the participant can mark the company again on the map. */
function participantRedrawWorkplaceLocation() {
  if (readOnly || SURVEY_MODE !== "participant") return;
  if (!participantHasWorkplaceLocation()) return;
  clearPendingLocation();
  resetSupplyChainRouteDraft();
  state.locations = state.locations.filter((l) => l.locationType !== "workplace");
  void flushSaveToServer();
  rebuildFromState();
  uiUpdateStats();
  setParticipantMapHintAfterIndustryGate();
  syncParticipantMapGateOverlay();
  syncParticipantLeftPanel();
  if (map) {
    requestAnimationFrame(() => map.invalidateSize());
  }
}

function initParticipantProfileStripOnce() {
  if (SURVEY_MODE !== "participant" || document.body.dataset.participantProfileStripBound === "1") return;
  document.body.dataset.participantProfileStripBound = "1";
  document.getElementById("participantProfileCompanyBtn")?.addEventListener("click", () => {
    openParticipantIndustryGateForEdit();
  });
  document.getElementById("participantProfileRedrawWorkplaceBtn")?.addEventListener("click", () => {
    participantRedrawWorkplaceLocation();
  });
  document.getElementById("participantProfileRoleBtn")?.addEventListener("click", () => {
    openParticipantRoleGate();
  });
  document.getElementById("participantProfileProductsBtn")?.addEventListener("click", () => {
    openParticipantGoodsGate();
  });
}

function openParticipantRoleGate() {
  document.getElementById("industryGate")?.classList.remove("is-open");
  document.getElementById("goodsGate")?.classList.remove("is-open");
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
    setParticipantMapHintAfterIndustryGate();
    if (map) {
      requestAnimationFrame(() => map.invalidateSize());
      setTimeout(() => map.invalidateSize(), 200);
    }
    maybeOpenParticipantGoodsGate();
    if (!participantNeedsGoodsGate()) maybeOpenParticipantRawMaterialsGate();
  });
}

function maybeOpenParticipantRoleGate() {
  if (participantNeedsRoleGate()) openParticipantRoleGate();
}

function openParticipantGoodsGate() {
  document.getElementById("industryGate")?.classList.remove("is-open");
  document.getElementById("roleGate")?.classList.remove("is-open");
  const gate = document.getElementById("goodsGate");
  if (!gate) return;
  gate.classList.add("is-open");
  syncParticipantMapGateOverlay();

  const otherWrap = document.getElementById("goodsOtherWrap");
  const otherInput = document.getElementById("goodsOtherInput");
  const keys = new Set(state.industry?.goodsCategoryKeys ?? []);

  document.querySelectorAll('input[name="participantGoods"]').forEach((cb) => {
    cb.checked = keys.has(cb.value);
  });

  const showOther = keys.has("other");
  otherWrap?.classList.toggle("is-hidden", !showOther);
  if (otherInput) otherInput.value = String(state.industry?.goodsOtherDetail ?? "");

  requestAnimationFrame(() => {
    document.querySelector('input[name="participantGoods"]')?.focus();
  });
}

function initParticipantGoodsGateOnce() {
  if (SURVEY_MODE !== "participant" || document.body.dataset.participantGoodsGateBound === "1") return;
  const gate = document.getElementById("goodsGate");
  if (!gate) return;
  document.body.dataset.participantGoodsGateBound = "1";

  const btn = document.getElementById("goodsContinueBtn");
  const otherWrap = document.getElementById("goodsOtherWrap");
  const otherInput = document.getElementById("goodsOtherInput");

  document.querySelectorAll('input[name="participantGoods"]').forEach((cb) => {
    cb.addEventListener("change", () => {
      const anyOther = Array.from(document.querySelectorAll('input[name="participantGoods"]')).some(
        (x) => x.value === "other" && x.checked
      );
      otherWrap?.classList.toggle("is-hidden", !anyOther);
      if (anyOther) otherInput?.focus();
    });
  });

  otherInput?.addEventListener("keydown", (e) => {
    if (e.key === "Enter") btn?.click();
  });

  btn?.addEventListener("click", () => {
    const selected = Array.from(document.querySelectorAll('input[name="participantGoods"]:checked')).map(
      (el) => el.value
    );
    if (selected.length === 0) {
      window.alert("Please select at least one option.");
      return;
    }
    let otherDetail = "";
    if (selected.includes("other")) {
      otherDetail = String(otherInput?.value ?? "").trim();
      if (!otherDetail) {
        window.alert("Please describe the goods or products for “Others”.");
        return;
      }
    }
    const goodsCategoryKeys = [...new Set(selected.filter((k) => GOODS_CATEGORY_KEYS.has(k)))];
    state.industry = {
      ...state.industry,
      goodsCategoryKeys,
      goodsOtherDetail: goodsCategoryKeys.includes("other") ? otherDetail : ""
    };
    gate.classList.remove("is-open");
    syncParticipantMapGateOverlay();
    void flushSaveToServer();
    rebuildFromState();
    uiUpdateStats();
    setParticipantMapHintAfterIndustryGate();
    if (map) {
      requestAnimationFrame(() => map.invalidateSize());
      setTimeout(() => map.invalidateSize(), 200);
    }
    maybeOpenParticipantRawMaterialsGate();
  });
}

function maybeOpenParticipantGoodsGate() {
  if (participantNeedsGoodsGate()) openParticipantGoodsGate();
}

function appendRawMaterialRowToList(container, value = "", placeholder = "Raw material") {
  const row = document.createElement("div");
  row.className = "rawMaterialsRow";
  row.setAttribute("role", "listitem");

  const input = document.createElement("input");
  input.type = "text";
  input.className = "goodsGate__input";
  input.maxLength = 200;
  input.autocomplete = "off";
  input.placeholder = placeholder;
  input.value = value;

  const rm = document.createElement("button");
  rm.type = "button";
  rm.className = "ghostBtn rawMaterialsRow__remove";
  rm.textContent = "Remove";
  rm.addEventListener("click", () => {
    const rows = container.querySelectorAll(".rawMaterialsRow");
    if (rows.length <= 1) {
      input.value = "";
      input.focus();
      return;
    }
    row.remove();
  });

  row.appendChild(input);
  row.appendChild(rm);
  container.appendChild(row);
}

function openParticipantRawMaterialsGate() {
  const gate = document.getElementById("rawMaterialsGate");
  const list = document.getElementById("rawMaterialsList");
  if (!gate || !list) return;
  gate.classList.add("is-open");
  syncParticipantMapGateOverlay();

  list.innerHTML = "";
  const saved = state.industry?.rawMaterials;
  const hasSaved = Array.isArray(saved) && saved.some((s) => String(s ?? "").trim());
  const seeds = hasSaved ? saved.map((s) => String(s ?? "")) : [""];
  for (const v of seeds) appendRawMaterialRowToList(list, v);

  requestAnimationFrame(() => {
    list.querySelector("input")?.focus();
  });
}

function openParticipantProductsGate() {
  const gate = document.getElementById("participantProductsGate");
  const list = document.getElementById("productsList");
  if (!gate || !list) return;
  gate.classList.add("is-open");
  syncParticipantMapGateOverlay();

  list.innerHTML = "";
  const saved = state.industry?.products;
  const hasSaved = Array.isArray(saved) && saved.some((s) => String(s ?? "").trim());
  const seeds = hasSaved ? saved.map((s) => String(s ?? "")) : [""];
  for (const v of seeds) appendRawMaterialRowToList(list, v, "Product");

  requestAnimationFrame(() => {
    list.querySelector("input")?.focus();
  });
}

function initParticipantProductsGateOnce() {
  if (SURVEY_MODE !== "participant" || document.body.dataset.participantProductsGateBound === "1") return;
  const gate = document.getElementById("participantProductsGate");
  const list = document.getElementById("productsList");
  const addBtn = document.getElementById("productsAddBtn");
  const btn = document.getElementById("productsContinueBtn");
  if (!gate || !list) return;
  document.body.dataset.participantProductsGateBound = "1";

  addBtn?.addEventListener("click", () => {
    if (list.querySelectorAll(".rawMaterialsRow").length >= 40) {
      window.alert("You can add up to 40 products.");
      return;
    }
    appendRawMaterialRowToList(list, "", "Product");
    list.querySelector(".rawMaterialsRow:last-of-type input")?.focus();
  });

  btn?.addEventListener("click", () => {
    const inputs = list.querySelectorAll(".rawMaterialsRow input");
    const raw = [];
    inputs.forEach((el) => {
      const t = String(el.value ?? "").trim();
      if (t) raw.push(t);
    });
    if (raw.length === 0) {
      window.alert("Please enter at least one product.");
      return;
    }
    state.industry = {
      ...state.industry,
      products: normalizeProductsFromPayload(raw)
    };
    ensureProductBranchesAligned();
    gate.classList.remove("is-open");
    syncParticipantMapGateOverlay();
    void flushSaveToServer();
    rebuildFromState();
    uiUpdateStats();
    setParticipantMapHintAfterIndustryGate();
    if (map) {
      requestAnimationFrame(() => map.invalidateSize());
      setTimeout(() => map.invalidateSize(), 200);
    }
    if (state.industry.products.length > 0) {
      resumeParticipantSupplyChainFlow();
    }
  });
}

function initParticipantRawMaterialsGateOnce() {
  if (SURVEY_MODE !== "participant" || document.body.dataset.participantRawMaterialsGateBound === "1") return;
  const gate = document.getElementById("rawMaterialsGate");
  const list = document.getElementById("rawMaterialsList");
  const addBtn = document.getElementById("rawMaterialsAddBtn");
  const btn = document.getElementById("rawMaterialsContinueBtn");
  if (!gate || !list) return;
  document.body.dataset.participantRawMaterialsGateBound = "1";

  addBtn?.addEventListener("click", () => {
    if (list.querySelectorAll(".rawMaterialsRow").length >= 40) {
      window.alert("You can add up to 40 raw materials.");
      return;
    }
    appendRawMaterialRowToList(list, "");
    list.querySelector(".rawMaterialsRow:last-of-type input")?.focus();
  });

  btn?.addEventListener("click", () => {
    const inputs = list.querySelectorAll(".rawMaterialsRow input");
    const raw = [];
    inputs.forEach((el) => {
      const t = String(el.value ?? "").trim();
      if (t) raw.push(t);
    });
    if (raw.length === 0) {
      window.alert("Please enter at least one raw material.");
      return;
    }
    state.industry = {
      ...state.industry,
      rawMaterials: normalizeRawMaterialsFromPayload(raw)
    };
    ensureRawMaterialBranchesAligned();
    gate.classList.remove("is-open");
    syncParticipantMapGateOverlay();
    void flushSaveToServer();
    rebuildFromState();
    uiUpdateStats();
    setParticipantMapHintAfterIndustryGate();
    if (map) {
      requestAnimationFrame(() => map.invalidateSize());
      setTimeout(() => map.invalidateSize(), 200);
    }
    if (state.industry.rawMaterials.length > 0) {
      resumeParticipantSupplyChainFlow();
    }
  });
}

function maybeOpenParticipantRawMaterialsGate() {
  if (participantNeedsRawMaterialsGate()) openParticipantRawMaterialsGate();
}

function participantRawMaterialBranchWorkIncomplete() {
  if (SURVEY_MODE !== "participant") return false;
  if (!participantRawMaterialsIsComplete()) return false;
  const mats = state.industry.rawMaterials ?? [];
  if (mats.length === 0) return false;
  ensureRawMaterialBranchesAligned();
  for (let i = 0; i < mats.length; i++) {
    const b = state.industry.rawMaterialBranches[i];
    if (rawMaterialSupplyChainBranchNeedsIntroGate(b)) return true;
    if (rawMaterialOriginBranchNeedsCategoryGate(b)) return true;
    if (rawMaterialOriginBranchNeedsMap(b)) return true;
    if (rawMaterialSupplyChainBranchNeedsDiagramGate(b)) return true;
    if (rawMaterialSupplyChainBranchNeedsTransportRoutes(b)) return true;
    if (rawMaterialSupplyChainBranchNeedsTripFrequencyGate(b)) return true;
  }
  return false;
}

function participantProductsIsComplete() {
  const arr = state.industry?.products;
  if (!Array.isArray(arr)) return false;
  return arr.some((s) => String(s ?? "").trim().length > 0);
}

function participantProductBranchWorkIncomplete() {
  if (SURVEY_MODE !== "participant") return false;
  if (!participantProductsIsComplete()) return false;
  const mats = state.industry.products ?? [];
  if (mats.length === 0) return false;
  ensureProductBranchesAligned();
  for (let i = 0; i < mats.length; i++) {
    const b = state.industry.productBranches[i];
    if (rawMaterialSupplyChainBranchNeedsIntroGate(b)) return true;
    if (rawMaterialOriginBranchNeedsCategoryGate(b)) return true;
    if (rawMaterialOriginBranchNeedsMap(b)) return true;
    if (rawMaterialSupplyChainBranchNeedsDiagramGate(b)) return true;
    if (rawMaterialSupplyChainBranchNeedsTransportRoutes(b)) return true;
    if (rawMaterialSupplyChainBranchNeedsTripFrequencyGate(b)) return true;
  }
  return false;
}

function participantAnySupplyChainBranchWorkIncomplete() {
  return participantRawMaterialBranchWorkIncomplete() || participantProductBranchWorkIncomplete();
}

function participantNeedsProductsGate() {
  return (
    SURVEY_MODE === "participant" &&
    participantHasWorkplaceLocation() &&
    participantIndustryRoleIsComplete() &&
    participantGoodsIsComplete() &&
    participantRawMaterialsIsComplete() &&
    !participantRawMaterialBranchWorkIncomplete() &&
    !participantProductsIsComplete()
  );
}

function resumeSupplyChainBranchFlowForKind(kind) {
  const { items, ensure, branchesKey } = industryBranchArrays(kind);
  const workIncomplete =
    kind === BRANCH_KIND_PRODUCT ? participantProductBranchWorkIncomplete() : participantRawMaterialBranchWorkIncomplete();
  try {
    if (!workIncomplete) {
      ui.scIntroIndex = null;
      ui.scBranchMap = null;
      ui.scOriginIndex = null;
      ui.scDiagramIndex = null;
      ui.scTripFreqIndex = null;
      resetSupplyChainRouteDraft();
      document.getElementById("rawMaterialSupplyChainDiagramGate")?.classList.remove("is-open");
      document.getElementById("rawMaterialTripFrequencyGate")?.classList.remove("is-open");
      return;
    }
    ui.scKind = kind;
    ensure();
    const branches = state.industry[branchesKey];
    for (let i = 0; i < items.length; i++) {
      const b = branches[i];
      if (rawMaterialSupplyChainBranchNeedsIntroGate(b)) {
        openSupplyChainIntroGate(kind, i);
        return;
      }
      if (rawMaterialOriginBranchNeedsCategoryGate(b)) {
        openOriginQuestionGate(kind, i);
        return;
      }
      if (rawMaterialOriginBranchNeedsMap(b)) {
        ui.scBranchMap = { kind, branchIndex: i };
        ui.scOriginIndex = null;
        syncParticipantMapGateOverlay();
        setParticipantMapHintAfterIndustryGate();
        return;
      }
      if (rawMaterialSupplyChainBranchNeedsDiagramGate(b)) {
        openSupplyChainDiagramGate(kind, i);
        return;
      }
      if (rawMaterialSupplyChainBranchNeedsTransportRoutes(b)) {
        ui.scBranchMap = null;
        ui.scOriginIndex = null;
        startSupplyChainTransportRouteDrawing(kind, i);
        syncParticipantMapGateOverlay();
        setParticipantMapHintAfterIndustryGate();
        return;
      }
      if (rawMaterialSupplyChainBranchNeedsTripFrequencyGate(b)) {
        openTripFrequencyGate(kind, i);
        return;
      }
    }
  } finally {
    syncParticipantLeftPanel();
  }
}

function resumeParticipantSupplyChainFlow() {
  try {
    if (participantRawMaterialBranchWorkIncomplete()) {
      resumeSupplyChainBranchFlowForKind(BRANCH_KIND_RAW);
      return;
    }
    if (participantNeedsProductsGate()) {
      openParticipantProductsGate();
      return;
    }
    if (participantProductBranchWorkIncomplete()) {
      resumeSupplyChainBranchFlowForKind(BRANCH_KIND_PRODUCT);
      return;
    }
    ui.scIntroIndex = null;
    ui.scBranchMap = null;
    ui.scOriginIndex = null;
    ui.scDiagramIndex = null;
    ui.scTripFreqIndex = null;
    resetSupplyChainRouteDraft();
    document.getElementById("rawMaterialSupplyChainDiagramGate")?.classList.remove("is-open");
    document.getElementById("rawMaterialTripFrequencyGate")?.classList.remove("is-open");
    document.getElementById("participantProductsGate")?.classList.remove("is-open");
  } finally {
    syncParticipantLeftPanel();
  }
}

function resumeRawMaterialBranchFlow() {
  resumeSupplyChainBranchFlowForKind(BRANCH_KIND_RAW);
}

function advanceAfterRawMaterialOriginPlaced(completedIndex) {
  void completedIndex;
  resumeParticipantSupplyChainFlow();
  if (!participantAnySupplyChainBranchWorkIncomplete()) {
    ui.scBranchMap = null;
    ui.scOriginIndex = null;
    ui.scDiagramIndex = null;
    syncParticipantMapGateOverlay();
    setParticipantMapHintAfterIndustryGate();
    void flushSaveToServer();
  }
}

function supplyChainDiagramAddB(dIn) {
  const d = normalizeSupplyChainDiagram(dIn);
  const nodes = [...d.modalChangeNodes];
  let legs = [...d.transportLegs];
  if (nodes.length === 0) {
    nodes.push({ modalChangeKey: "", otherDetail: "" });
    legs = [
      { modeKey: "", otherDetail: "" },
      { modeKey: "", otherDetail: "" }
    ];
  } else {
    nodes.push({ modalChangeKey: "", otherDetail: "" });
    legs.splice(legs.length - 1, 0, { modeKey: "", otherDetail: "" });
  }
  return normalizeSupplyChainDiagram({ ...d, modalChangeNodes: nodes, transportLegs: legs });
}

function supplyChainDiagramRemoveB(dIn, i) {
  const d = normalizeSupplyChainDiagram(dIn);
  const nodes = [...d.modalChangeNodes];
  const legs = [...d.transportLegs];
  if (i < 0 || i >= nodes.length) return d;
  if (nodes.length === 1) {
    return normalizeSupplyChainDiagram({
      ...d,
      modalChangeNodes: [],
      transportLegs: [{ modeKey: "", otherDetail: "" }]
    });
  }
  nodes.splice(i, 1);
  legs.splice(i + 1, 1);
  return normalizeSupplyChainDiagram({ ...d, modalChangeNodes: nodes, transportLegs: legs });
}

function supplyChainLocationSelectHtml(selectedKey) {
  const opts = [`<option value="">Select…</option>`]
    .concat(
      SUPPLY_CHAIN_LOCATION_OPTIONS.map((o) => {
        const sel = o.key === selectedKey ? " selected" : "";
        return `<option value="${escapeHtml(o.key)}"${sel}>${escapeHtml(o.label)}</option>`;
      })
    )
    .join("");
  return opts;
}

function rawMaterialOriginSelectHtml(selectedKey) {
  return [`<option value="">Select…</option>`]
    .concat(
      RAW_MATERIAL_ORIGIN_OPTIONS.map((o) => {
        const sel = o.key === selectedKey ? " selected" : "";
        return `<option value="${escapeHtml(o.key)}"${sel}>${escapeHtml(o.label)}</option>`;
      })
    )
    .join("");
}

/** Read sidebar origin controls (participant left diagram only). */
function readOriginFieldsFromDiagramMount(mount) {
  const sel = mount?.querySelector('[data-sc="origin-category"]');
  if (!sel) return null;
  const key = String(sel.value ?? "").trim();
  const other = String(mount.querySelector('[data-sc="origin-other"]')?.value ?? "").trim();
  return { originCategoryKey: key, originOtherDetail: key === "other" ? other : "" };
}

/**
 * Apply origin edits from the left diagram. On change: clear map coords and routes (same as origin gate).
 * Returns { row, setMap, error }.
 */
function applyOriginFromLeftDiagramToRow(row, nextDiagram, o) {
  const key = o.originCategoryKey;
  const otherNorm = key === "other" ? String(o.originOtherDetail ?? "").trim() : "";
  if (!key) return { row, setMap: false, error: "Please select an originating location type." };
  if (key === "other" && !otherNorm) return { row, setMap: false, error: 'Please specify for "Others".' };
  const originChanged =
    row.originCategoryKey !== key ||
    (key === "other" && String(row.originOtherDetail ?? "").trim() !== otherNorm);
  if (!originChanged) return { row, setMap: false };
  return {
    row: {
      ...row,
      originCategoryKey: key,
      originOtherDetail: otherNorm,
      originMapSkipped: false,
      originX: null,
      originY: null,
      tripFrequencyCount: null,
      tripFrequencyPeriod: "",
      supplyChainTransportRoutes: normalizeSupplyChainTransportRoutesForBranch([], nextDiagram)
    },
    setMap: true
  };
}

function supplyChainStoppingPointSelectHtml(selectedKey) {
  const opts = [`<option value="">Select…</option>`]
    .concat(
      SUPPLY_CHAIN_LOCATION_OPTIONS.map((o) => {
        const sel = o.key === selectedKey ? " selected" : "";
        return `<option value="${escapeHtml(o.key)}"${sel}>${escapeHtml(o.label)}</option>`;
      })
    )
    .join("");
  return opts;
}

function supplyChainTransportPickerHtml(selectedKey, disabled = false) {
  return SUPPLY_CHAIN_TRANSPORT_MODE_OPTIONS.map((o) => {
    const icon = TRANSPORT_MODE_ICONS[o.key];
    const sel = o.key === selectedKey ? " is-selected" : "";
    const dis = disabled ? " disabled" : "";
    const inner = icon
      ? `<img src="${escapeHtml(icon)}" alt="" aria-hidden="true"><span>${escapeHtml(o.label)}</span>`
      : `<span>${escapeHtml(o.label)}</span>`;
    return `<button type="button" class="scTransportPicker__btn${sel}" data-mode="${escapeHtml(o.key)}"${dis}>${inner}</button>`;
  }).join("");
}

function formatOriginLabelForSupplyChainABranch(b) {
  if (!b?.originCategoryKey || b.originCategoryKey === RAW_MATERIAL_ORIGIN_SKIPPED_KEY) return "—";
  const opt = RAW_MATERIAL_ORIGIN_OPTIONS.find((o) => o.key === b.originCategoryKey);
  if (b.originCategoryKey === "other") {
    const d = String(b.originOtherDetail ?? "").trim();
    return d ? `Others (${escapeHtml(d)})` : "Others";
  }
  return opt ? escapeHtml(opt.label) : escapeHtml(String(b.originCategoryKey));
}

function originIconSrcForSupplyChain(b) {
  const k = b?.originCategoryKey;
  if (!k || k === RAW_MATERIAL_ORIGIN_SKIPPED_KEY) return ORIGIN_TYPE_ICONS.storage_facility;
  return ORIGIN_TYPE_ICONS[k] ?? ORIGIN_TYPE_ICONS.storage_facility;
}

/** Static img src for destination row in supply-chain diagram (matches map destination icon set). */
function destinationIconSrcForSupplyChainDiagram(dIn) {
  const d = normalizeSupplyChainDiagram(dIn ?? {});
  const k = normalizeSupplyLocationKeyForDiagram(d.destinationCategoryKey);
  if (!k) return ORIGIN_TYPE_ICONS.storage_facility;
  if (k === "other") return ORIGIN_TYPE_ICONS.distribution_center;
  return ORIGIN_TYPE_ICONS[k] ?? ORIGIN_TYPE_ICONS.storage_facility;
}

function collectSupplyChainDiagramFromMount(
  materialIndex,
  mountId = "rawMaterialSupplyChainDiagramMount",
  branchKind = BRANCH_KIND_RAW
) {
  const mount = document.getElementById(mountId);
  if (!mount) return null;
  const { ensure, branchesKey } = industryBranchArrays(branchKind);
  ensure();
  const br = state.industry[branchesKey][materialIndex];
  if (!br) return null;
  const destCatEl = mount.querySelector('[data-sc="destination-category"]');
  const destOtherEl = mount.querySelector('[data-sc="destination-other"]');
  const destCat = destCatEl
    ? String(destCatEl.value ?? "").trim()
    : normalizeSupplyLocationKeyForDiagram(br.supplyChainDiagram?.destinationCategoryKey);
  const destOther = destOtherEl
    ? String(destOtherEl.value ?? "").trim()
    : String(br.supplyChainDiagram?.destinationOtherDetail ?? "");
  const modalSels = [...mount.querySelectorAll("[data-sc-modal-node]")].sort(
    (a, b) =>
      Number(a.getAttribute("data-sc-modal-node")) - Number(b.getAttribute("data-sc-modal-node"))
  );
  const nodes = modalSels.map((sel) => {
    const ni = Number(sel.getAttribute("data-sc-modal-node"));
    const mk = String(sel.value ?? "").trim();
    const ot = mount.querySelector(`[data-sc-modal-other="${ni}"]`);
    return {
      modalChangeKey: normalizeModalChangeKey(mk),
      otherDetail: String(ot?.value ?? "")
    };
  });
  const legCount = nodes.length + 1;
  const legs = [];
  for (let li = 0; li < legCount; li++) {
    const picker = mount.querySelector(`[data-sc-transport-leg="${li}"]`);
    const mode = picker?.querySelector(".scTransportPicker__btn.is-selected")?.dataset.mode ?? "";
    const ot = mount.querySelector(`[data-sc-transport-other="${li}"]`);
    legs.push({
      modeKey: normalizeTransportModeKey(mode),
      otherDetail: String(ot?.value ?? "")
    });
  }
  const normalized = normalizeSupplyChainDiagram({
    destinationCategoryKey: destCat,
    destinationOtherDetail: destOther,
    modalChangeNodes: nodes,
    transportLegs: legs
  });
  return applySupplyChainDiagramConstraints(normalized);
}

/** Green segment from horizontal center of A dot to horizontal center of C dot (relative to `.supplyChainDiagram__flow`). */
function positionSupplyChainDiagramTrack(mountEl) {
  if (!mountEl) return;
  const flow = mountEl.querySelector(".supplyChainDiagram__flow");
  const track = mountEl.querySelector(".supplyChainDiagram__track");
  const dotA = mountEl.querySelector(".supplyChainDiagram__row--a .supplyChainDiagram__nodeDot");
  const dotC = mountEl.querySelector(".supplyChainDiagram__row--c .supplyChainDiagram__nodeDot");
  if (!flow || !track) return;
  if (!dotA || !dotC) {
    track.style.display = "none";
    return;
  }
  const fr = flow.getBoundingClientRect();
  const ar = dotA.getBoundingClientRect();
  const cr = dotC.getBoundingClientRect();
  const yA = ar.top + ar.height / 2 - fr.top;
  const yC = cr.top + cr.height / 2 - fr.top;
  const topPx = Math.min(yA, yC);
  const heightPx = Math.max(1, Math.abs(yC - yA));
  const cxA = ar.left + ar.width / 2 - fr.left;
  const cxC = cr.left + cr.width / 2 - fr.left;
  const cx = (cxA + cxC) / 2;
  track.style.display = "block";
  track.style.left = `${cx - 2}px`;
  track.style.top = `${topPx}px`;
  track.style.height = `${heightPx}px`;
}

function bindSupplyChainDiagramTrackLayout(mountEl) {
  if (!mountEl || mountEl.dataset.supplyChainTrackLayoutBound === "1") return;
  mountEl.dataset.supplyChainTrackLayoutBound = "1";
  const ro = new ResizeObserver(() => {
    positionSupplyChainDiagramTrack(mountEl);
  });
  ro.observe(mountEl);
  window.addEventListener(
    "resize",
    () => {
      positionSupplyChainDiagramTrack(mountEl);
    },
    { passive: true }
  );
}

function renderSupplyChainDiagramMount(materialIndex, options = {}) {
  const mountId = options.mountId ?? "rawMaterialSupplyChainDiagramMount";
  const readOnly = options.readOnly === true;
  const branchKind = options.branchKind ?? BRANCH_KIND_RAW;
  const { ensure, branchesKey, items } = industryBranchArrays(branchKind);
  const mount = document.getElementById(mountId);
  if (!mount) return;
  ensure();
  const br = state.industry[branchesKey][materialIndex];
  if (!br) return;
  let d = applySupplyChainDiagramConstraints(br.supplyChainDiagram ?? defaultSupplyChainDiagram());
  if (!readOnly) {
    br.supplyChainDiagram = d;
  }
  mount.classList.toggle("supplyChainDiagramMount--sidebarReadonly", readOnly);
  const nodes = d.modalChangeNodes;
  const legs = d.transportLegs;
  const matLabel =
    String(items[materialIndex] ?? "").trim() ||
    (branchKind === BRANCH_KIND_PRODUCT ? "this product" : "this material");
  const showTripFreqLeft =
    (mountId === "participantLeftDiagramMount" && SURVEY_MODE === "participant") ||
    (mountId === "conductorLeftDiagramMount" && SURVEY_MODE === "conductor");
  const showTripFreqLeftResolved =
    showTripFreqLeft &&
    br.originCategoryKey !== RAW_MATERIAL_ORIGIN_SKIPPED_KEY &&
    isSupplyChainTransportRoutesComplete(br) &&
    isTripFrequencyComplete(br);
  const originSummary = formatOriginLabelForSupplyChainABranch(br);
  const originIcon = originIconSrcForSupplyChain(br);
  const destIconSrc = destinationIconSrcForSupplyChainDiagram(d);
  const destSummaryText = formatSupplyChainDestinationDesc(d);
  const showRedrawButtons = mountId === "participantLeftDiagramMount" && SURVEY_MODE === "participant" && !readOnly;
  const allowLeftOriginEdit =
    mountId === "participantLeftDiagramMount" &&
    SURVEY_MODE === "participant" &&
    !readOnly &&
    br.originCategoryKey !== RAW_MATERIAL_ORIGIN_SKIPPED_KEY;

  const parts = [];
  parts.push(`<div class="supplyChainDiagram__flow">`);
  parts.push(`<div class="supplyChainDiagram__track" aria-hidden="true"></div>`);
  parts.push(`<div class="supplyChainDiagram__flowInner">`);

  // A — read-only in modal / while drawing route on map; editable on participant left when branch not skipped
  parts.push(`<div class="supplyChainDiagram__row supplyChainDiagram__row--a">`);
  parts.push(`<div class="supplyChainDiagram__nodeDot" aria-hidden="true">A</div>`);
  parts.push(`<div class="supplyChainDiagram__fields">`);
  if (allowLeftOriginEdit) {
    parts.push(`<div class="supplyChainDiagram__fieldLabel supplyChainDiagram__fieldLabel--withIcon">`);
    parts.push(`<img class="supplyChainDiagram__icon" src="${originIcon}" alt="" aria-hidden="true"/>`);
    parts.push(`<span>Origin</span>`);
    parts.push(`</div>`);
    const showOriginOther = br.originCategoryKey === "other";
    parts.push(
      `<select class="goodsGate__input supplyChainDiagram__select" data-sc="origin-category" aria-label="Origin type">`
    );
    parts.push(rawMaterialOriginSelectHtml(br.originCategoryKey));
    parts.push(`</select>`);
    parts.push(
      `<div class="goodsOtherWrap supplyChainDiagram__otherWrap${showOriginOther ? "" : " is-hidden"}" data-sc="origin-other-wrap">`
    );
    parts.push(
      `<input type="text" class="goodsGate__input" data-sc="origin-other" maxlength="300" placeholder="Specify origin" value="${escapeHtml(br.originOtherDetail)}" />`
    );
    parts.push(`</div>`);
    parts.push(
      `<div class="supplyChainDiagram__originMapRow"><button type="button" class="ghostBtn supplyChainDiagram__originMapBtn" data-sc="origin-place-map">Place on map</button></div>`
    );
    parts.push(
      `<p class="supplyChainDiagram__hint">Changing the type clears drawn transportation routes. Use Place on map to set or move the origin pin.</p>`
    );
  } else {
    parts.push(`<div class="supplyChainDiagram__fieldLabel">Origin</div>`);
    parts.push(
      `<div class="supplyChainDiagram__readonly"><img class="supplyChainDiagram__icon" src="${originIcon}" alt=""/> <span>${originSummary}</span></div>`
    );
    parts.push(
      readOnly && mountId === "conductorLeftDiagramMount"
        ? `<p class="supplyChainDiagram__hint">Participant’s recorded originating location (read-only).</p>`
        : readOnly
          ? `<p class="supplyChainDiagram__hint">Reference while you draw each transportation leg on the map.</p>`
          : `<p class="supplyChainDiagram__hint">Same as your answer for where ${escapeHtml(matLabel)} originates.</p>`
    );
  }
  parts.push(`</div></div>`);

  // Transport legs and B nodes: leg[0] … leg[n] for n nodes
  for (let ni = 0; ni <= nodes.length; ni++) {
    const leg = legs[ni] ?? { modeKey: "", otherDetail: "" };
    const legIdx = ni;
    const showTOther = leg.modeKey === "other";
    const tlab = `Transportation ${ni + 1}`;
    const tInputRo = readOnly ? " readonly" : "";
    parts.push(`<div class="supplyChainDiagram__row supplyChainDiagram__row--transport">`);
    parts.push(`<div class="supplyChainDiagram__trackPad" aria-hidden="true"></div>`);
    parts.push(`<div class="supplyChainDiagram__fields">`);
    if (showRedrawButtons) {
      const canR = canRedrawSupplyChainTransportLeg(br, legIdx);
      const redrawTitle = canR
        ? "Clear this leg and all later legs on the map, then redraw starting here"
        : "Complete earlier transportation routes on the map first (and origin for Transportation 1)";
      parts.push(`<div class="supplyChainDiagram__transportLabelBar">`);
      parts.push(`<span class="supplyChainDiagram__fieldLabel supplyChainDiagram__fieldLabel--bar">${escapeHtml(tlab)}</span>`);
      parts.push(
        `<button type="button" class="ghostBtn supplyChainDiagram__redrawRouteBtn" data-sc-redraw-route-leg="${legIdx}"${
          canR ? "" : " disabled"
        } title="${escapeHtml(redrawTitle)}">Redraw Route</button>`
      );
      parts.push(`</div>`);
    } else {
      parts.push(`<div class="supplyChainDiagram__fieldLabel">${escapeHtml(tlab)}</div>`);
    }
    parts.push(`<div class="scTransportPicker" data-sc-transport-leg="${legIdx}">`);
    parts.push(supplyChainTransportPickerHtml(leg.modeKey, readOnly));
    parts.push(`</div>`);
    parts.push(
      `<div class="goodsOtherWrap supplyChainDiagram__otherWrap${showTOther ? "" : " is-hidden"}" data-sc-transport-other-wrap="${legIdx}">`
    );
    parts.push(
      `<input type="text" class="goodsGate__input" data-sc-transport-other="${legIdx}" maxlength="300" placeholder="Specify mode" value="${escapeHtml(leg.otherDetail)}"${tInputRo} />`
    );
    parts.push(`</div>`);
    if (!readOnly) {
      parts.push(
        `<div class="supplyChainDiagram__inlineActions"><button type="button" class="supplyChainDiagram__iconBtn" data-sc-add-after-leg="${legIdx}" title="Add stopping point">+</button></div>`
      );
    }
    parts.push(`</div></div>`);

    if (ni < nodes.length) {
      const node = nodes[ni] ?? { modalChangeKey: "", otherDetail: "" };
      const showMOther = node.modalChangeKey === "other";
      const bLabel = nodes.length <= 1 ? "B" : `B${ni + 1}`;
      const modalDisabled = readOnly ? " disabled" : "";
      const mInputRo = readOnly ? " readonly" : "";
      parts.push(`<div class="supplyChainDiagram__row supplyChainDiagram__row--b">`);
      parts.push(`<div class="supplyChainDiagram__nodeDot supplyChainDiagram__nodeDot--b" aria-hidden="true">${escapeHtml(bLabel)}</div>`);
      parts.push(`<div class="supplyChainDiagram__fields">`);
      const stoppingIconSrc = (node.modalChangeKey && node.modalChangeKey !== "other")
        ? (ORIGIN_TYPE_ICONS[node.modalChangeKey] ?? ORIGIN_TYPE_ICONS.storage_facility)
        : ORIGIN_TYPE_ICONS.storage_facility;
      parts.push(`<div class="supplyChainDiagram__fieldLabel supplyChainDiagram__fieldLabel--withIcon">`);
      parts.push(`<img class="supplyChainDiagram__icon" src="${escapeHtml(stoppingIconSrc)}" alt="" aria-hidden="true"/>`);
      parts.push(`<span>Stopping Point</span>`);
      parts.push(`</div>`);
      parts.push(
        `<select class="goodsGate__input supplyChainDiagram__select" data-sc-modal-node="${ni}" aria-label="Stopping point ${ni + 1}"${modalDisabled}>`
      );
      parts.push(supplyChainStoppingPointSelectHtml(node.modalChangeKey));
      parts.push(`</select>`);
      parts.push(
        `<div class="goodsOtherWrap supplyChainDiagram__otherWrap${showMOther ? "" : " is-hidden"}" data-sc-modal-other-wrap="${ni}">`
      );
      parts.push(
        `<input type="text" class="goodsGate__input" data-sc-modal-other="${ni}" maxlength="300" placeholder="Specify" value="${escapeHtml(node.otherDetail)}"${mInputRo} />`
      );
      parts.push(`</div>`);
      if (!readOnly) {
        parts.push(
          `<div class="supplyChainDiagram__inlineActions"><button type="button" class="supplyChainDiagram__iconBtn supplyChainDiagram__iconBtn--minus" data-sc-remove-b="${ni}" title="Remove this step">−</button></div>`
        );
      }
      parts.push(`</div></div>`);
    }
  }

  // C — destination (icon matches map markers; modal + left panel use same markup)
  const dk = d.destinationCategoryKey;
  const showDOther = dk === "other";
  const destDisabled = readOnly ? " disabled" : "";
  const dInputRo = readOnly ? " readonly" : "";
  parts.push(`<div class="supplyChainDiagram__row supplyChainDiagram__row--c">`);
  parts.push(`<div class="supplyChainDiagram__nodeDot" aria-hidden="true">C</div>`);
  parts.push(`<div class="supplyChainDiagram__fields">`);
  if (readOnly) {
    parts.push(`<div class="supplyChainDiagram__fieldLabel">Destination</div>`);
    parts.push(
      `<div class="supplyChainDiagram__readonly"><img class="supplyChainDiagram__icon" src="${escapeHtml(
        destIconSrc
      )}" alt="" aria-hidden="true"/> <span>${escapeHtml(destSummaryText)}</span></div>`
    );
  } else {
    parts.push(`<div class="supplyChainDiagram__fieldLabel supplyChainDiagram__fieldLabel--withIcon">`);
    parts.push(
      `<img class="supplyChainDiagram__icon supplyChainDiagram__icon--destination" src="${escapeHtml(destIconSrc)}" alt="" aria-hidden="true"/>`
    );
    parts.push(`<span>Destination</span>`);
    parts.push(`</div>`);
    parts.push(
      `<select class="goodsGate__input supplyChainDiagram__select" data-sc="destination-category" aria-label="Destination"${destDisabled}>`
    );
    parts.push(supplyChainLocationSelectHtml(dk));
    parts.push(`</select>`);
    parts.push(
      `<div class="goodsOtherWrap supplyChainDiagram__otherWrap${showDOther ? "" : " is-hidden"}" data-sc-dest-other-wrap>`
    );
    parts.push(
      `<input type="text" class="goodsGate__input" data-sc="destination-other" maxlength="300" placeholder="Specify destination" value="${escapeHtml(d.destinationOtherDetail)}"${dInputRo} />`
    );
    parts.push(`</div>`);
  }
  parts.push(`</div></div>`);

  if (showTripFreqLeftResolved) {
    const n = br.tripFrequencyCount;
    const per = normalizeTripFrequencyPeriod(br.tripFrequencyPeriod);
    const countVal = n != null && Number.isFinite(Number(n)) ? String(Math.floor(Number(n))) : "";
    const tfDisabled = readOnly ? " disabled" : "";
    parts.push(`<div class="supplyChainDiagram__row supplyChainDiagram__row--tripFreq">`);
    parts.push(`<div class="supplyChainDiagram__trackPad" aria-hidden="true"></div>`);
    parts.push(`<div class="supplyChainDiagram__fields">`);
    parts.push(`<div class="supplyChainDiagram__fieldLabel">Trip frequency</div>`);
    parts.push(`<div class="supplyChainDiagram__tripFreqRow">`);
    parts.push(
      `<input type="number" class="goodsGate__input supplyChainDiagram__tripFreqInput" data-trip-freq="count" min="1" step="1" placeholder="N" value="${escapeHtml(countVal)}"${tfDisabled} />`
    );
    parts.push(`<span class="supplyChainDiagram__tripFreqMid">trip(s) per</span>`);
    parts.push(
      `<select class="goodsGate__input supplyChainDiagram__select" data-trip-freq="period" aria-label="Time period"${tfDisabled}>`
    );
    parts.push(`<option value=""${per === "" ? " selected" : ""}>—</option>`);
    parts.push(`<option value="day"${per === "day" ? " selected" : ""}>Day</option>`);
    parts.push(`<option value="week"${per === "week" ? " selected" : ""}>Week</option>`);
    parts.push(`<option value="month"${per === "month" ? " selected" : ""}>Month</option>`);
    parts.push(`</select>`);
    parts.push(`</div></div></div>`);
  }

  parts.push(`</div></div>`);

  mount.innerHTML = parts.join("");

  if (!readOnly) {
    const applyDiagram = (nextD) => {
      ensure();
      let row = state.industry[branchesKey][materialIndex];
      if (!row) return;
      const nextDiagram = applySupplyChainDiagramConstraints(nextD);
      const mid = options.mountId ?? "rawMaterialSupplyChainDiagramMount";
      const mountEl = document.getElementById(mid);
      if (mid === "participantLeftDiagramMount" && mountEl) {
        const o = readOriginFieldsFromDiagramMount(mountEl);
        if (o) {
          const res = applyOriginFromLeftDiagramToRow(row, nextDiagram, o);
          if (res.error) {
            window.alert(res.error);
            renderSupplyChainDiagramMount(materialIndex, options);
            return;
          }
          row = res.row;
          if (res.setMap) {
            ui.scKind = branchKind;
            ui.scBranchMap = { kind: branchKind, branchIndex: materialIndex };
            ui.scOriginIndex = null;
            setParticipantMapHintAfterIndustryGate();
            syncParticipantMapGateOverlay();
          }
        }
      }
      const nextRoutes = normalizeSupplyChainTransportRoutesForBranch(row.supplyChainTransportRoutes, nextDiagram);
      const merged = { ...row, supplyChainDiagram: nextDiagram, supplyChainTransportRoutes: nextRoutes };
      const routesOk = isSupplyChainTransportRoutesComplete(merged);
      let tripFrequencyCount = row.tripFrequencyCount;
      let tripFrequencyPeriod = normalizeTripFrequencyPeriod(row.tripFrequencyPeriod);
      if (!routesOk) {
        tripFrequencyCount = null;
        tripFrequencyPeriod = "";
      }
      state.industry[branchesKey][materialIndex] = {
        ...merged,
        tripFrequencyCount,
        tripFrequencyPeriod
      };
      renderSupplyChainDiagramMount(materialIndex, options);
    };

    /** Merge current form fields from the DOM into state before changing structure. */
    function snapshotDiagramFromDomOrState() {
      const mid = options.mountId ?? "rawMaterialSupplyChainDiagramMount";
      const fromDom = collectSupplyChainDiagramFromMount(materialIndex, mid, branchKind);
      if (fromDom) return fromDom;
      const row = state.industry[branchesKey][materialIndex];
      return row?.supplyChainDiagram
        ? applySupplyChainDiagramConstraints(row.supplyChainDiagram)
        : applySupplyChainDiagramConstraints(defaultSupplyChainDiagram());
    }

    mount.querySelectorAll(".scTransportPicker__btn").forEach((btn) => {
      btn.addEventListener("click", () => {
        const picker = btn.closest("[data-sc-transport-leg]");
        if (!picker) return;
        picker.querySelectorAll(".scTransportPicker__btn").forEach((b) => b.classList.remove("is-selected"));
        btn.classList.add("is-selected");
        const legIdx = Number(picker.getAttribute("data-sc-transport-leg"));
        const otherWrap = mount.querySelector(`[data-sc-transport-other-wrap="${legIdx}"]`);
        if (otherWrap) otherWrap.classList.toggle("is-hidden", btn.dataset.mode !== "other");
        const next = snapshotDiagramFromDomOrState();
        applyDiagram(next);
      });
    });
    mount.querySelectorAll("[data-sc-modal-node]").forEach((sel) => {
      sel.addEventListener("change", () => {
        const next = snapshotDiagramFromDomOrState();
        applyDiagram(next);
      });
    });
    const destSel = mount.querySelector('[data-sc="destination-category"]');
    destSel?.addEventListener("change", () => {
      const next = snapshotDiagramFromDomOrState();
      applyDiagram(next);
    });
    mount.querySelector('[data-sc="destination-other"]')?.addEventListener("change", () => {
      const next = snapshotDiagramFromDomOrState();
      applyDiagram(next);
    });

    mount.querySelector('[data-sc="origin-category"]')?.addEventListener("change", () => {
      const next = snapshotDiagramFromDomOrState();
      applyDiagram(next);
    });
    mount.querySelector('[data-sc="origin-other"]')?.addEventListener("change", () => {
      const next = snapshotDiagramFromDomOrState();
      applyDiagram(next);
    });
    mount.querySelector('[data-sc="origin-place-map"]')?.addEventListener("click", () => {
      if ((options.mountId ?? "rawMaterialSupplyChainDiagramMount") !== "participantLeftDiagramMount") return;
      const o = readOriginFieldsFromDiagramMount(mount);
      if (!o?.originCategoryKey) {
        window.alert("Please select an originating location type.");
        return;
      }
      if (o.originCategoryKey === "other" && !String(mount.querySelector('[data-sc="origin-other"]')?.value ?? "").trim()) {
        window.alert('Please specify for "Others".');
        return;
      }
      applyDiagram(snapshotDiagramFromDomOrState());
      clearPendingLocation();
      ui.scKind = branchKind;
      ui.scBranchMap = { kind: branchKind, branchIndex: materialIndex };
      ui.scOriginIndex = null;
      setParticipantMapHintAfterIndustryGate();
      syncParticipantMapGateOverlay();
      rebuildFromState();
      uiUpdateStats();
    });

    mount.querySelectorAll("[data-sc-add-after-leg]").forEach((btn) => {
      btn.addEventListener("click", () => {
        applyDiagram(supplyChainDiagramAddB(snapshotDiagramFromDomOrState()));
      });
    });
    mount.querySelectorAll("[data-sc-remove-b]").forEach((btn) => {
      btn.addEventListener("click", () => {
        const bi = Number(btn.getAttribute("data-sc-remove-b"));
        applyDiagram(supplyChainDiagramRemoveB(snapshotDiagramFromDomOrState(), bi));
      });
    });

    mount.querySelectorAll("[data-sc-redraw-route-leg]").forEach((btn) => {
      btn.addEventListener("click", () => {
        if (btn.disabled) return;
        const li = Number(btn.getAttribute("data-sc-redraw-route-leg"));
        if (Number.isNaN(li)) return;
        participantRedrawSupplyChainRouteFromLeg(branchKind, materialIndex, li);
      });
    });

    const commitTripFreqFromLeft = () => {
      if (mountId !== "participantLeftDiagramMount") return;
      const c = mount.querySelector('[data-trip-freq="count"]');
      const p = mount.querySelector('[data-trip-freq="period"]');
      ensure();
      const row = state.industry[branchesKey][materialIndex];
      if (!row) return;
      const pv = normalizeTripFrequencyPeriod(p?.value);
      const rawN = String(c?.value ?? "").trim();
      const num = rawN === "" ? null : Number(rawN);
      const tripFrequencyCount =
        num != null && Number.isFinite(num) && num >= 1 && Math.floor(num) === num ? Math.floor(num) : null;
      const tripFrequencyPeriod = tripFrequencyCount != null ? pv : "";
      state.industry[branchesKey][materialIndex] = {
        ...row,
        tripFrequencyCount,
        tripFrequencyPeriod
      };
      void flushSaveToServer();
      rebuildFromState();
      uiUpdateStats();
      ensure();
      const rowAfter = state.industry[branchesKey][materialIndex];
      if (rowAfter && rawMaterialSupplyChainBranchNeedsTripFrequencyGate(rowAfter)) {
        openTripFrequencyGate(branchKind, materialIndex);
      }
    };
    mount.querySelector('[data-trip-freq="count"]')?.addEventListener("change", commitTripFreqFromLeft);
    mount.querySelector('[data-trip-freq="period"]')?.addEventListener("change", commitTripFreqFromLeft);
  }

  bindSupplyChainDiagramTrackLayout(mount);
  requestAnimationFrame(() => {
    requestAnimationFrame(() => positionSupplyChainDiagramTrack(mount));
  });
}

/**
 * Participant left: raw-material pills, product pills (after listed), and shared detail diagram.
 */
function syncParticipantLeftPanel() {
  if (SURVEY_MODE !== "participant") return;
  const emptyEl = document.getElementById("participantLeftEmpty");
  const shellEl = document.getElementById("participantLeftShell");
  const pillsEl = document.getElementById("participantRawMaterialPills");
  const productPillsEl = document.getElementById("participantProductPills");
  const detailEl = document.getElementById("participantLeftDetail");
  const detailName = document.getElementById("participantLeftDetailName");
  const placeholderEl = document.getElementById("participantLeftDetailPlaceholder");
  const mountEl = document.getElementById("participantLeftDiagramMount");
  const saveRow = document.getElementById("participantDiagramSaveRow");
  if (!emptyEl || !shellEl) return;

  const mats = state.industry.rawMaterials ?? [];
  const prods = state.industry.products ?? [];
  const hasMaterials = participantRawMaterialsIsComplete();
  const hasProducts = participantProductsIsComplete();

  if (!hasMaterials) {
    emptyEl.classList.remove("is-hidden");
    emptyEl.setAttribute("aria-hidden", "false");
    shellEl.classList.add("is-hidden");
    if (mountEl) mountEl.innerHTML = "";
    if (saveRow) saveRow.classList.add("is-hidden");
    if (placeholderEl) {
      placeholderEl.classList.add("is-hidden");
      placeholderEl.textContent = "";
    }
    return;
  }

  emptyEl.classList.add("is-hidden");
  emptyEl.setAttribute("aria-hidden", "true");
  shellEl.classList.remove("is-hidden");

  const dr = ui.scRouteDrawing;
  if (dr && dr.branchIndex != null) {
    ui.participantLeftPanelKind = dr.branchKind;
    ui.participantLeftPanelIndex = dr.branchIndex;
  }

  if (ui.participantLeftPanelKind === BRANCH_KIND_RAW && mats.length > 0) {
    if (ui.participantLeftPanelIndex < 0 || ui.participantLeftPanelIndex >= mats.length) {
      ui.participantLeftPanelIndex = 0;
    }
  }
  if (ui.participantLeftPanelKind === BRANCH_KIND_PRODUCT && hasProducts && prods.length > 0) {
    if (ui.participantLeftPanelIndex < 0 || ui.participantLeftPanelIndex >= prods.length) {
      ui.participantLeftPanelIndex = 0;
    }
  }

  let pillsHtml = "";
  for (let i = 0; i < mats.length; i++) {
    const label = String(mats[i] ?? "").trim() || `Material ${i + 1}`;
    const active =
      ui.participantLeftPanelKind === BRANCH_KIND_RAW && ui.participantLeftPanelIndex === i ? " is-active" : "";
    pillsHtml += `<button type="button" class="participantPill${active}" data-participant-rm-index="${i}" role="tab" aria-selected="${
      ui.participantLeftPanelKind === BRANCH_KIND_RAW && ui.participantLeftPanelIndex === i ? "true" : "false"
    }">${escapeHtml(label)}</button>`;
  }
  if (pillsEl) pillsEl.innerHTML = pillsHtml;

  if (productPillsEl) {
    if (!hasProducts) {
      productPillsEl.innerHTML = `<span class="participantPill participantPill--muted">Your products (supply chain steps) appear here after you list them.</span>`;
    } else {
      ensureProductBranchesAligned();
      let ph = "";
      for (let i = 0; i < prods.length; i++) {
        const label = String(prods[i] ?? "").trim() || `Product ${i + 1}`;
        const active =
          ui.participantLeftPanelKind === BRANCH_KIND_PRODUCT && ui.participantLeftPanelIndex === i ? " is-active" : "";
        ph += `<button type="button" class="participantPill${active}" data-participant-product-index="${i}" role="tab" aria-selected="${
          ui.participantLeftPanelKind === BRANCH_KIND_PRODUCT && ui.participantLeftPanelIndex === i ? "true" : "false"
        }">${escapeHtml(label)}</button>`;
      }
      productPillsEl.innerHTML = ph;
    }
  }

  const kind = ui.participantLeftPanelKind;
  const items = kind === BRANCH_KIND_PRODUCT ? prods : mats;
  const sel = ui.participantLeftPanelIndex;

  if (kind === BRANCH_KIND_PRODUCT && !hasProducts) {
    if (detailEl) detailEl.classList.add("is-hidden");
    if (mountEl) mountEl.innerHTML = "";
    if (saveRow) saveRow.classList.add("is-hidden");
    return;
  }

  if (sel == null || sel < 0 || sel >= items.length) {
    if (detailEl) detailEl.classList.add("is-hidden");
    if (mountEl) mountEl.innerHTML = "";
    if (saveRow) saveRow.classList.add("is-hidden");
    if (placeholderEl) placeholderEl.classList.add("is-hidden");
    return;
  }

  if (detailEl) detailEl.classList.remove("is-hidden");
  const itemLabel =
    String(items[sel] ?? "").trim() ||
    (kind === BRANCH_KIND_PRODUCT ? `Product ${sel + 1}` : `Material ${sel + 1}`);
  if (detailName) detailName.textContent = itemLabel;

  const br = branchRow(kind, sel);
  if (!br) {
    if (mountEl) mountEl.innerHTML = "";
    if (saveRow) saveRow.classList.add("is-hidden");
    return;
  }

  if (
    rawMaterialSupplyChainBranchNeedsIntroGate(br) ||
    rawMaterialOriginBranchNeedsCategoryGate(br) ||
    rawMaterialOriginBranchNeedsMap(br)
  ) {
    if (mountEl) mountEl.innerHTML = "";
    if (placeholderEl) {
      placeholderEl.classList.remove("is-hidden");
      placeholderEl.textContent =
        kind === BRANCH_KIND_PRODUCT
          ? "Answer the prompts (intro, origin, and map if needed) for this product before the supply chain diagram appears here."
          : "Answer the prompts (intro, origin, and map if needed) for this material before the supply chain diagram appears here.";
    }
    if (saveRow) saveRow.classList.add("is-hidden");
    return;
  }

  if (br.originCategoryKey === RAW_MATERIAL_ORIGIN_SKIPPED_KEY) {
    if (mountEl) mountEl.innerHTML = "";
    if (placeholderEl) {
      placeholderEl.classList.remove("is-hidden");
      placeholderEl.textContent =
        kind === BRANCH_KIND_PRODUCT
          ? "This product was skipped — no supply chain diagram."
          : "This raw material was skipped — no supply chain diagram.";
    }
    if (saveRow) saveRow.classList.add("is-hidden");
    return;
  }

  if (rawMaterialSupplyChainBranchNeedsDiagramGate(br)) {
    if (mountEl) mountEl.innerHTML = "";
    if (placeholderEl) {
      placeholderEl.classList.remove("is-hidden");
      placeholderEl.textContent =
        "Use the popup to complete the supply chain, then click Confirm & Save. The diagram will show here afterward for review and editing (including after map routes).";
    }
    if (saveRow) saveRow.classList.add("is-hidden");
    return;
  }

  if (placeholderEl) {
    placeholderEl.classList.add("is-hidden");
    placeholderEl.textContent = "";
  }

  const drawingHere = Boolean(dr && dr.branchKind === kind && dr.branchIndex === sel);
  const readOnly = drawingHere;

  renderSupplyChainDiagramMount(sel, {
    readOnly,
    mountId: "participantLeftDiagramMount",
    branchKind: kind
  });

  if (saveRow) {
    saveRow.classList.toggle("is-hidden", readOnly);
  }

  requestAnimationFrame(() => {
    positionSupplyChainDiagramTrack(document.getElementById("participantLeftDiagramMount"));
  });
}

/**
 * Conductor main site: mirror participant left panel (profile, pills, diagram) read-only.
 */
function syncConductorParticipantLeftPanel() {
  if (SURVEY_MODE !== "conductor") return;
  const profileCard = document.getElementById("conductorProfileCard");
  const emptyEl = document.getElementById("conductorLeftEmpty");
  const shellEl = document.getElementById("conductorLeftShell");
  const pillsEl = document.getElementById("conductorRawMaterialPills");
  const productPillsEl = document.getElementById("conductorProductPills");
  const detailEl = document.getElementById("conductorLeftDetail");
  const detailName = document.getElementById("conductorLeftDetailName");
  const placeholderEl = document.getElementById("conductorLeftDetailPlaceholder");
  const mountEl = document.getElementById("conductorLeftDiagramMount");
  if (!emptyEl || !shellEl) return;

  if (!viewingParticipantId) {
    if (profileCard) profileCard.classList.add("is-hidden");
    emptyEl.classList.remove("is-hidden");
    emptyEl.setAttribute("aria-hidden", "false");
    emptyEl.textContent =
      "Select a participant to view their raw materials, products, and supply-chain details (read-only).";
    shellEl.classList.add("is-hidden");
    if (mountEl) mountEl.innerHTML = "";
    if (placeholderEl) {
      placeholderEl.classList.add("is-hidden");
      placeholderEl.textContent = "";
    }
    return;
  }

  const mats = state.industry.rawMaterials ?? [];
  const prods = state.industry.products ?? [];
  const hasMaterials = participantRawMaterialsIsComplete();
  const hasProducts = participantProductsIsComplete();

  if (!hasMaterials) {
    emptyEl.classList.remove("is-hidden");
    emptyEl.setAttribute("aria-hidden", "false");
    emptyEl.textContent =
      "This participant has not completed the raw materials step yet — nothing to show in the supply-chain panel.";
    shellEl.classList.add("is-hidden");
    if (mountEl) mountEl.innerHTML = "";
    if (placeholderEl) {
      placeholderEl.classList.add("is-hidden");
      placeholderEl.textContent = "";
    }
    return;
  }

  emptyEl.classList.add("is-hidden");
  emptyEl.setAttribute("aria-hidden", "true");
  shellEl.classList.remove("is-hidden");

  if (ui.participantLeftPanelKind === BRANCH_KIND_RAW && mats.length > 0) {
    if (ui.participantLeftPanelIndex < 0 || ui.participantLeftPanelIndex >= mats.length) {
      ui.participantLeftPanelIndex = 0;
    }
  }
  if (ui.participantLeftPanelKind === BRANCH_KIND_PRODUCT && hasProducts && prods.length > 0) {
    if (ui.participantLeftPanelIndex < 0 || ui.participantLeftPanelIndex >= prods.length) {
      ui.participantLeftPanelIndex = 0;
    }
  }

  let pillsHtml = "";
  for (let i = 0; i < mats.length; i++) {
    const label = String(mats[i] ?? "").trim() || `Material ${i + 1}`;
    const active =
      ui.participantLeftPanelKind === BRANCH_KIND_RAW && ui.participantLeftPanelIndex === i ? " is-active" : "";
    pillsHtml += `<button type="button" class="participantPill${active}" data-conductor-rm-index="${i}" role="tab" aria-selected="${
      ui.participantLeftPanelKind === BRANCH_KIND_RAW && ui.participantLeftPanelIndex === i ? "true" : "false"
    }">${escapeHtml(label)}</button>`;
  }
  if (pillsEl) pillsEl.innerHTML = pillsHtml;

  if (productPillsEl) {
    if (!hasProducts) {
      productPillsEl.innerHTML = `<span class="participantPill participantPill--muted">Products appear here after the participant lists them.</span>`;
    } else {
      ensureProductBranchesAligned();
      let ph = "";
      for (let i = 0; i < prods.length; i++) {
        const label = String(prods[i] ?? "").trim() || `Product ${i + 1}`;
        const active =
          ui.participantLeftPanelKind === BRANCH_KIND_PRODUCT && ui.participantLeftPanelIndex === i
            ? " is-active"
            : "";
        ph += `<button type="button" class="participantPill${active}" data-conductor-product-index="${i}" role="tab" aria-selected="${
          ui.participantLeftPanelKind === BRANCH_KIND_PRODUCT && ui.participantLeftPanelIndex === i ? "true" : "false"
        }">${escapeHtml(label)}</button>`;
      }
      productPillsEl.innerHTML = ph;
    }
  }

  const kind = ui.participantLeftPanelKind;
  const items = kind === BRANCH_KIND_PRODUCT ? prods : mats;
  const sel = ui.participantLeftPanelIndex;

  if (kind === BRANCH_KIND_PRODUCT && !hasProducts) {
    if (detailEl) detailEl.classList.add("is-hidden");
    if (mountEl) mountEl.innerHTML = "";
    return;
  }

  if (sel == null || sel < 0 || sel >= items.length) {
    if (detailEl) detailEl.classList.add("is-hidden");
    if (mountEl) mountEl.innerHTML = "";
    if (placeholderEl) placeholderEl.classList.add("is-hidden");
    return;
  }

  if (detailEl) detailEl.classList.remove("is-hidden");
  const itemLabel =
    String(items[sel] ?? "").trim() ||
    (kind === BRANCH_KIND_PRODUCT ? `Product ${sel + 1}` : `Material ${sel + 1}`);
  if (detailName) detailName.textContent = itemLabel;

  const br = branchRow(kind, sel);
  if (!br) {
    if (mountEl) mountEl.innerHTML = "";
    return;
  }

  if (
    rawMaterialSupplyChainBranchNeedsIntroGate(br) ||
    rawMaterialOriginBranchNeedsCategoryGate(br) ||
    rawMaterialOriginBranchNeedsMap(br)
  ) {
    if (mountEl) mountEl.innerHTML = "";
    if (placeholderEl) {
      placeholderEl.classList.remove("is-hidden");
      placeholderEl.textContent =
        "The participant has not finished intro, origin, and map steps for this item — no diagram yet.";
    }
    return;
  }

  if (br.originCategoryKey === RAW_MATERIAL_ORIGIN_SKIPPED_KEY) {
    if (mountEl) mountEl.innerHTML = "";
    if (placeholderEl) {
      placeholderEl.classList.remove("is-hidden");
      placeholderEl.textContent =
        kind === BRANCH_KIND_PRODUCT
          ? "This product was skipped — no supply chain diagram."
          : "This raw material was skipped — no supply chain diagram.";
    }
    return;
  }

  if (rawMaterialSupplyChainBranchNeedsDiagramGate(br)) {
    if (mountEl) mountEl.innerHTML = "";
    if (placeholderEl) {
      placeholderEl.classList.remove("is-hidden");
      placeholderEl.textContent =
        "The participant has not confirmed the supply chain in the popup for this item — no diagram here yet.";
    }
    return;
  }

  if (placeholderEl) {
    placeholderEl.classList.add("is-hidden");
    placeholderEl.textContent = "";
  }

  renderSupplyChainDiagramMount(sel, {
    readOnly: true,
    mountId: "conductorLeftDiagramMount",
    branchKind: kind
  });

  requestAnimationFrame(() => {
    positionSupplyChainDiagramTrack(document.getElementById("conductorLeftDiagramMount"));
  });
}

function openSupplyChainDiagramGate(kind, index) {
  ui.scKind = kind;
  const gate = document.getElementById("rawMaterialSupplyChainDiagramGate");
  if (!gate) return;
  document.getElementById("participantProductsGate")?.classList.remove("is-open");
  document.getElementById("rawMaterialSupplyChainIntroGate")?.classList.remove("is-open");
  document.getElementById("rawMaterialOriginGate")?.classList.remove("is-open");
  document.getElementById("rawMaterialTripFrequencyGate")?.classList.remove("is-open");
  ui.scTripFreqIndex = null;
  ui.scBranchMap = null;
  ui.scOriginIndex = null;
  ui.scIntroIndex = null;
  const { items, ensure, branchesKey } = industryBranchArrays(kind);
  ensure();
  const br = state.industry[branchesKey][index];
  if (!br || br.originCategoryKey === RAW_MATERIAL_ORIGIN_SKIPPED_KEY) return;
  br.supplyChainDiagram = applySupplyChainDiagramConstraints(br.supplyChainDiagram ?? defaultSupplyChainDiagram());
  ui.scDiagramIndex = index;
  const badge = document.getElementById("rawMaterialSupplyChainDiagramBadge");
  const titleEl = document.getElementById("rawMaterialSupplyChainDiagramTitle");
  const matName =
    String(items[index] ?? "").trim() || (kind === BRANCH_KIND_PRODUCT ? "this product" : "this material");
  if (badge) badge.textContent = matName;
  const kindPrefixEl = document.getElementById("supplyChainDiagramKindPrefix");
  if (kindPrefixEl) {
    kindPrefixEl.textContent = kind === BRANCH_KIND_PRODUCT ? "Product" : "Raw material";
  }
  if (titleEl) {
    titleEl.textContent = `Please Provide the Supply Chain Information of ${matName}`;
  }
  if (SURVEY_MODE === "participant") {
    ui.participantLeftPanelKind = kind;
    ui.participantLeftPanelIndex = index;
  }
  gate.classList.add("is-open");
  renderSupplyChainDiagramMount(index, { branchKind: kind });
  syncParticipantMapGateOverlay();
  if (SURVEY_MODE === "participant") {
    syncParticipantLeftPanel();
  }
  requestAnimationFrame(() => {
    positionSupplyChainDiagramTrack(document.getElementById("rawMaterialSupplyChainDiagramMount"));
    document.getElementById("rawMaterialSupplyChainDiagramSaveBtn")?.focus();
  });
}

function openRawMaterialSupplyChainDiagramGate(materialIndex) {
  openSupplyChainDiagramGate(BRANCH_KIND_RAW, materialIndex);
}

function openTripFrequencyGate(kind, index) {
  ui.scKind = kind;
  const gate = document.getElementById("rawMaterialTripFrequencyGate");
  if (!gate) return;
  document.getElementById("participantProductsGate")?.classList.remove("is-open");
  document.getElementById("rawMaterialSupplyChainIntroGate")?.classList.remove("is-open");
  document.getElementById("rawMaterialOriginGate")?.classList.remove("is-open");
  document.getElementById("rawMaterialSupplyChainDiagramGate")?.classList.remove("is-open");
  ui.scBranchMap = null;
  ui.scOriginIndex = null;
  ui.scIntroIndex = null;
  ui.scDiagramIndex = null;
  resetSupplyChainRouteDraft();
  const { items, ensure, branchesKey } = industryBranchArrays(kind);
  ensure();
  const br = state.industry[branchesKey][index];
  if (!br || !rawMaterialSupplyChainBranchNeedsTripFrequencyGate(br)) return;
  ui.scTripFreqIndex = index;
  const matName =
    String(items[index] ?? "").trim() || (kind === BRANCH_KIND_PRODUCT ? "this product" : "this material");
  const titleEl = document.getElementById("rawMaterialTripFrequencyGateTitle");
  if (titleEl) {
    titleEl.textContent = `What is the frequency of this trip for ${matName}?`;
  }
  const countInput = document.getElementById("rawMaterialTripFrequencyCount");
  const periodSel = document.getElementById("rawMaterialTripFrequencyPeriod");
  const n = br.tripFrequencyCount;
  if (countInput) {
    countInput.value = n != null && Number.isFinite(Number(n)) ? String(Math.floor(Number(n))) : "";
  }
  if (periodSel) {
    periodSel.value = normalizeTripFrequencyPeriod(br.tripFrequencyPeriod);
  }
  if (SURVEY_MODE === "participant") {
    ui.participantLeftPanelKind = kind;
    ui.participantLeftPanelIndex = index;
  }
  gate.classList.add("is-open");
  syncParticipantMapGateOverlay();
  syncParticipantLeftPanel();
  requestAnimationFrame(() => document.getElementById("rawMaterialTripFrequencyCount")?.focus());
}

function openRawMaterialTripFrequencyGate(materialIndex) {
  openTripFrequencyGate(BRANCH_KIND_RAW, materialIndex);
}

function openSupplyChainIntroGate(kind, index) {
  ui.scKind = kind;
  const gate = document.getElementById("rawMaterialSupplyChainIntroGate");
  const titleEl = document.getElementById("rawMaterialSupplyChainIntroTitle");
  if (!gate) return;
  document.getElementById("participantProductsGate")?.classList.remove("is-open");
  document.getElementById("rawMaterialSupplyChainDiagramGate")?.classList.remove("is-open");
  document.getElementById("rawMaterialOriginGate")?.classList.remove("is-open");
  document.getElementById("rawMaterialTripFrequencyGate")?.classList.remove("is-open");
  ui.scTripFreqIndex = null;
  ui.scDiagramIndex = null;
  ui.scBranchMap = null;
  ui.scOriginIndex = null;
  const { items } = industryBranchArrays(kind);
  const matName =
    String(items[index] ?? "").trim() || (kind === BRANCH_KIND_PRODUCT ? "this product" : "this material");
  if (titleEl) {
    titleEl.textContent = `Now Please provide some information about the supply chain of ${matName}`;
  }
  ui.scIntroIndex = index;
  gate.classList.add("is-open");
  syncParticipantMapGateOverlay();
  requestAnimationFrame(() => document.getElementById("rawMaterialSupplyChainIntroContinueBtn")?.focus());
}

function openRawMaterialSupplyChainIntroGate(materialIndex) {
  openSupplyChainIntroGate(BRANCH_KIND_RAW, materialIndex);
}

function openOriginQuestionGate(kind, index) {
  ui.scKind = kind;
  const gate = document.getElementById("rawMaterialOriginGate");
  const titleEl = document.getElementById("rawMaterialOriginGateTitle");
  if (!gate) return;
  document.getElementById("participantProductsGate")?.classList.remove("is-open");
  document.getElementById("rawMaterialSupplyChainIntroGate")?.classList.remove("is-open");
  document.getElementById("rawMaterialSupplyChainDiagramGate")?.classList.remove("is-open");
  document.getElementById("rawMaterialTripFrequencyGate")?.classList.remove("is-open");
  ui.scTripFreqIndex = null;
  ui.scIntroIndex = null;
  ui.scDiagramIndex = null;
  const { items, ensure, branchesKey } = industryBranchArrays(kind);
  const matName =
    String(items[index] ?? "").trim() || (kind === BRANCH_KIND_PRODUCT ? "this product" : "this material");
  if (titleEl) {
    titleEl.textContent = `Where does ${matName} originate?`;
  }
  ui.scOriginIndex = index;
  ui.scBranchMap = null;
  ensure();
  const br = state.industry[branchesKey][index] ?? {};

  document.querySelectorAll('input[name="rawMaterialOriginChoice"]').forEach((r) => {
    r.checked = br.originCategoryKey === r.value;
  });
  const otherWrap = document.getElementById("rawMaterialOriginOtherWrap");
  const showOther = br.originCategoryKey === "other";
  otherWrap?.classList.toggle("is-hidden", !showOther);
  const oi = document.getElementById("rawMaterialOriginOtherInput");
  if (oi) oi.value = String(br.originOtherDetail ?? "");

  gate.classList.add("is-open");
  syncParticipantMapGateOverlay();
  requestAnimationFrame(() => {
    const checked = document.querySelector('input[name="rawMaterialOriginChoice"]:checked');
    if (checked) checked.focus();
    else document.querySelector('input[name="rawMaterialOriginChoice"]')?.focus();
  });
}

function openRawMaterialOriginQuestionGate(materialIndex) {
  openOriginQuestionGate(BRANCH_KIND_RAW, materialIndex);
}

function skipRawMaterialOriginMap() {
  if (readOnly || SURVEY_MODE !== "participant") return;
  const kind = ui.scBranchMap?.kind;
  const idx = ui.scBranchMap?.branchIndex;
  if (kind == null || idx == null || idx < 0) return;
  clearPendingLocation();
  const { ensure, branchesKey } = industryBranchArrays(kind);
  ensure();
  const row = state.industry[branchesKey][idx] ?? {};
  if (row.originCategoryKey === RAW_MATERIAL_ORIGIN_SKIPPED_KEY) return;
  state.industry[branchesKey][idx] = {
    ...row,
    originMapSkipped: true,
    originX: null,
    originY: null
  };
  ui.scBranchMap = null;
  void flushSaveToServer();
  rebuildFromState();
  uiUpdateStats();
  setParticipantMapHintAfterIndustryGate();
  advanceAfterRawMaterialOriginPlaced(idx);
}

function initParticipantRawMaterialOriginMapSkipOnce() {
  if (SURVEY_MODE !== "participant" || document.body.dataset.participantRawMaterialMapSkipBound === "1") {
    return;
  }
  const btn = document.getElementById("rawMaterialOriginMapSkipBtn");
  if (!btn) return;
  document.body.dataset.participantRawMaterialMapSkipBound = "1";
  btn.addEventListener("click", () => skipRawMaterialOriginMap());
}

function initParticipantLeftPanelOnce() {
  if (SURVEY_MODE !== "participant" || document.body.dataset.participantLeftPanelBound === "1") return;
  const pills = document.getElementById("participantRawMaterialPills");
  const productPills = document.getElementById("participantProductPills");
  if (!pills && !productPills) return;
  document.body.dataset.participantLeftPanelBound = "1";
  pills?.addEventListener("click", (e) => {
    const btn = e.target.closest("[data-participant-rm-index]");
    if (!btn) return;
    const i = Number(btn.getAttribute("data-participant-rm-index"));
    if (Number.isNaN(i)) return;
    ui.participantLeftPanelKind = BRANCH_KIND_RAW;
    ui.participantLeftPanelIndex = i;
    syncParticipantLeftPanel();
  });
  productPills?.addEventListener("click", (e) => {
    const btn = e.target.closest("[data-participant-product-index]");
    if (!btn) return;
    const i = Number(btn.getAttribute("data-participant-product-index"));
    if (Number.isNaN(i)) return;
    ui.participantLeftPanelKind = BRANCH_KIND_PRODUCT;
    ui.participantLeftPanelIndex = i;
    syncParticipantLeftPanel();
  });
}

function initConductorLeftPanelOnce() {
  if (SURVEY_MODE !== "conductor" || document.body.dataset.conductorLeftPanelBound === "1") return;
  const pills = document.getElementById("conductorRawMaterialPills");
  const productPills = document.getElementById("conductorProductPills");
  if (!pills && !productPills) return;
  document.body.dataset.conductorLeftPanelBound = "1";
  pills?.addEventListener("click", (e) => {
    const btn = e.target.closest("[data-conductor-rm-index]");
    if (!btn) return;
    const i = Number(btn.getAttribute("data-conductor-rm-index"));
    if (Number.isNaN(i)) return;
    ui.participantLeftPanelKind = BRANCH_KIND_RAW;
    ui.participantLeftPanelIndex = i;
    syncConductorParticipantLeftPanel();
  });
  productPills?.addEventListener("click", (e) => {
    const btn = e.target.closest("[data-conductor-product-index]");
    if (!btn) return;
    const i = Number(btn.getAttribute("data-conductor-product-index"));
    if (Number.isNaN(i)) return;
    ui.participantLeftPanelKind = BRANCH_KIND_PRODUCT;
    ui.participantLeftPanelIndex = i;
    syncConductorParticipantLeftPanel();
  });
}

function initParticipantRawMaterialSupplyChainDiagramOnce() {
  if (SURVEY_MODE !== "participant" || document.body.dataset.participantRawMaterialDiagramBound === "1") {
    return;
  }
  const gate = document.getElementById("rawMaterialSupplyChainDiagramGate");
  const modalBtn = document.getElementById("rawMaterialSupplyChainDiagramSaveBtn");
  const leftBtn = document.getElementById("participantLeftDiagramSaveBtn");
  if (!modalBtn && !leftBtn) return;
  document.body.dataset.participantRawMaterialDiagramBound = "1";

  const commitSupplyChainDiagramSave = () => {
    if (readOnly) return;
    const kind = ui.scKind ?? BRANCH_KIND_RAW;
    let idx = ui.scDiagramIndex;
    if (idx == null || idx < 0) {
      if (ui.participantLeftPanelKind === kind) idx = ui.participantLeftPanelIndex;
    }
    if (idx == null || idx < 0) return;
    const diagramGateOpen = gate?.classList.contains("is-open");
    const mountId =
      diagramGateOpen
        ? "rawMaterialSupplyChainDiagramMount"
        : SURVEY_MODE === "participant"
          ? "participantLeftDiagramMount"
          : "rawMaterialSupplyChainDiagramMount";
    let collected = collectSupplyChainDiagramFromMount(idx, mountId, kind);
    if (!collected) return;
    collected = applySupplyChainDiagramConstraints(collected);
    if (!isSupplyChainDiagramComplete(collected)) {
      window.alert('Please complete all fields in the diagram, including any "Others" detail.');
      return;
    }
    const { ensure, branchesKey } = industryBranchArrays(kind);
    ensure();
    const mount = document.getElementById(mountId);
    let prevRow = state.industry[branchesKey][idx];
    if (mountId === "participantLeftDiagramMount" && mount) {
      const o = readOriginFieldsFromDiagramMount(mount);
      if (o) {
        const res = applyOriginFromLeftDiagramToRow(prevRow, collected, o);
        if (res.error) {
          window.alert(res.error);
          return;
        }
        prevRow = res.row;
      }
    }
    let mergedRow = {
      ...prevRow,
      supplyChainDiagram: collected,
      supplyChainTransportRoutes: normalizeSupplyChainTransportRoutesForBranch(
        prevRow?.supplyChainTransportRoutes,
        collected
      )
    };
    if (!isSupplyChainTransportRoutesComplete(mergedRow)) {
      mergedRow = {
        ...mergedRow,
        tripFrequencyCount: null,
        tripFrequencyPeriod: ""
      };
    }
    state.industry[branchesKey][idx] = mergedRow;
    gate?.classList.remove("is-open");
    ui.scDiagramIndex = null;
    void flushSaveToServer();
    rebuildFromState();
    uiUpdateStats();
    syncParticipantMapGateOverlay();
    syncParticipantLeftPanel();
    resumeParticipantSupplyChainFlow();
    if (!participantAnySupplyChainBranchWorkIncomplete()) {
      setParticipantMapHintAfterIndustryGate();
      void flushSaveToServer();
    }
    if (map) {
      requestAnimationFrame(() => map.invalidateSize());
      setTimeout(() => map.invalidateSize(), 200);
    }
  };

  modalBtn?.addEventListener("click", commitSupplyChainDiagramSave);
  leftBtn?.addEventListener("click", commitSupplyChainDiagramSave);
}

function initParticipantRawMaterialTripFrequencyOnce() {
  if (SURVEY_MODE !== "participant" || document.body.dataset.participantRawMaterialTripFreqBound === "1") {
    return;
  }
  const gate = document.getElementById("rawMaterialTripFrequencyGate");
  const btn = document.getElementById("rawMaterialTripFrequencyContinueBtn");
  const countInput = document.getElementById("rawMaterialTripFrequencyCount");
  if (!gate || !btn) return;
  document.body.dataset.participantRawMaterialTripFreqBound = "1";
  btn.addEventListener("click", () => {
    const idx = ui.scTripFreqIndex;
    if (idx == null || idx < 0) return;
    const n = Number(document.getElementById("rawMaterialTripFrequencyCount")?.value ?? "");
    const per = normalizeTripFrequencyPeriod(document.getElementById("rawMaterialTripFrequencyPeriod")?.value);
    if (!Number.isFinite(n) || n < 1 || Math.floor(n) !== n) {
      window.alert("Please enter a whole number of trips (at least 1).");
      return;
    }
    if (!per) {
      window.alert("Please choose Day, Week, or Month.");
      return;
    }
    const kind = ui.scKind ?? BRANCH_KIND_RAW;
    const { ensure, branchesKey } = industryBranchArrays(kind);
    ensure();
    const prev = state.industry[branchesKey][idx];
    state.industry[branchesKey][idx] = {
      ...prev,
      tripFrequencyCount: Math.floor(n),
      tripFrequencyPeriod: per
    };
    gate.classList.remove("is-open");
    ui.scTripFreqIndex = null;
    void flushSaveToServer();
    rebuildFromState();
    uiUpdateStats();
    syncParticipantMapGateOverlay();
    resumeParticipantSupplyChainFlow();
    if (!participantAnySupplyChainBranchWorkIncomplete()) {
      setParticipantMapHintAfterIndustryGate();
      void flushSaveToServer();
    }
    if (map) {
      requestAnimationFrame(() => map.invalidateSize());
      setTimeout(() => map.invalidateSize(), 200);
    }
  });
  countInput?.addEventListener("keydown", (e) => {
    if (e.key === "Enter") btn.click();
  });
}

function initParticipantRawMaterialSupplyChainIntroOnce() {
  if (SURVEY_MODE !== "participant" || document.body.dataset.participantRawMaterialSupplyIntroBound === "1") {
    return;
  }
  const gate = document.getElementById("rawMaterialSupplyChainIntroGate");
  const btn = document.getElementById("rawMaterialSupplyChainIntroContinueBtn");
  if (!gate || !btn) return;
  document.body.dataset.participantRawMaterialSupplyIntroBound = "1";
  btn.addEventListener("click", () => {
    const idx = ui.scIntroIndex;
    if (idx == null || idx < 0) return;
    const kind = ui.scKind ?? BRANCH_KIND_RAW;
    const { ensure, branchesKey } = industryBranchArrays(kind);
    ensure();
    state.industry[branchesKey][idx] = {
      ...state.industry[branchesKey][idx],
      supplyChainIntroAcknowledged: true
    };
    gate.classList.remove("is-open");
    ui.scIntroIndex = null;
    void flushSaveToServer();
    syncParticipantMapGateOverlay();
    openOriginQuestionGate(kind, idx);
    if (map) {
      requestAnimationFrame(() => map.invalidateSize());
      setTimeout(() => map.invalidateSize(), 200);
    }
  });
}

function initParticipantRawMaterialOriginGateOnce() {
  if (SURVEY_MODE !== "participant" || document.body.dataset.participantRawMaterialOriginGateBound === "1") {
    return;
  }
  const gate = document.getElementById("rawMaterialOriginGate");
  const btn = document.getElementById("rawMaterialOriginContinueBtn");
  const otherWrap = document.getElementById("rawMaterialOriginOtherWrap");
  const otherInput = document.getElementById("rawMaterialOriginOtherInput");
  if (!gate) return;
  document.body.dataset.participantRawMaterialOriginGateBound = "1";

  document.querySelectorAll('input[name="rawMaterialOriginChoice"]').forEach((radio) => {
    radio.addEventListener("change", () => {
      const checked = document.querySelector('input[name="rawMaterialOriginChoice"]:checked');
      const show = checked?.value === "other";
      otherWrap?.classList.toggle("is-hidden", !show);
      if (show) otherInput?.focus();
    });
  });

  otherInput?.addEventListener("keydown", (e) => {
    if (e.key === "Enter") btn?.click();
  });

  btn?.addEventListener("click", () => {
    const idx = ui.scOriginIndex;
    if (idx == null || idx < 0) return;
    const kind = ui.scKind ?? BRANCH_KIND_RAW;
    const { ensure, branchesKey } = industryBranchArrays(kind);
    const sel = document.querySelector('input[name="rawMaterialOriginChoice"]:checked');
    if (!sel) {
      window.alert("Please select an option.");
      return;
    }
    if (sel.value === RAW_MATERIAL_ORIGIN_SKIPPED_KEY) {
      ensure();
      state.industry[branchesKey][idx] = {
        ...state.industry[branchesKey][idx],
        originCategoryKey: RAW_MATERIAL_ORIGIN_SKIPPED_KEY,
        originOtherDetail: "",
        originX: null,
        originY: null,
        originMapSkipped: false
      };
      gate.classList.remove("is-open");
      syncParticipantMapGateOverlay();
      ui.scBranchMap = null;
      ui.scOriginIndex = null;
      void flushSaveToServer();
      rebuildFromState();
      uiUpdateStats();
      setParticipantMapHintAfterIndustryGate();
      advanceAfterRawMaterialOriginPlaced(idx);
      if (map) {
        requestAnimationFrame(() => map.invalidateSize());
        setTimeout(() => map.invalidateSize(), 200);
      }
      return;
    }
    let detail = "";
    if (sel.value === "other") {
      detail = String(otherInput?.value ?? "").trim();
      if (!detail) {
        window.alert("Please specify for “Others”.");
        return;
      }
    }
    ensure();
    state.industry[branchesKey][idx] = {
      ...state.industry[branchesKey][idx],
      originCategoryKey: sel.value,
      originOtherDetail: sel.value === "other" ? detail : "",
      originMapSkipped: false,
      originX: null,
      originY: null
    };
    gate.classList.remove("is-open");
    syncParticipantMapGateOverlay();
    ui.scBranchMap = { kind, branchIndex: idx };
    ui.scOriginIndex = null;
    void flushSaveToServer();
    rebuildFromState();
    uiUpdateStats();
    setParticipantMapHintAfterIndustryGate();
    if (map) {
      requestAnimationFrame(() => map.invalidateSize());
      setTimeout(() => map.invalidateSize(), 200);
    }
  });
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
  syncParticipantLeftPanel();
  initParticipantIndustryGateOnce();
  initParticipantProfileStripOnce();
  initParticipantRoleGateOnce();
  initParticipantGoodsGateOnce();
  initParticipantRawMaterialsGateOnce();
  initParticipantProductsGateOnce();
  initParticipantLeftPanelOnce();
  initParticipantRawMaterialSupplyChainIntroOnce();
  initParticipantRawMaterialSupplyChainDiagramOnce();
  initParticipantRawMaterialTripFrequencyOnce();
  initParticipantRawMaterialOriginGateOnce();
  initParticipantRawMaterialOriginMapSkipOnce();

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
    const input = document.getElementById("industryCompanyInput");
    if (input) input.value = "";
    openParticipantIndustryGateForEdit();
    return;
  }

  if (participantNeedsRoleGate()) {
    openParticipantRoleGate();
  } else if (participantNeedsGoodsGate()) {
    openParticipantGoodsGate();
  } else if (participantNeedsRawMaterialsGate()) {
    openParticipantRawMaterialsGate();
  } else if (participantRawMaterialBranchWorkIncomplete()) {
    resumeRawMaterialBranchFlow();
  } else {
    resumeParticipantSupplyChainFlow();
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
    state.industry = {
      companyName: "",
      roleKey: "",
      roleOtherDetail: "",
      goodsCategoryKeys: [],
      goodsOtherDetail: "",
      rawMaterials: [],
      rawMaterialBranches: []
    };
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
      "Use the left panel to review the participant’s profile, raw materials, products, and supply-chain diagram (read-only). Open “Show participant filter” to select someone. The map shows all layers including IBX rail context.";
  }
  setupConductorCreateLink();
  initConductorLeftPanelOnce();
  syncConductorParticipantLeftPanel();
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


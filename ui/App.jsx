const { useState, useEffect, useRef, useCallback } = React;

const API = "";

// ---------------------------------------------------------------------------
// Auth-aware API helper
// ---------------------------------------------------------------------------

function getToken() {
  return localStorage.getItem("pa_token");
}

function clearToken() {
  localStorage.removeItem("pa_token");
  localStorage.removeItem("pa_user");
}

async function api(method, path, body) {
  const opts = { method, headers: { "Content-Type": "application/json" } };
  const token = getToken();
  if (token) {
    opts.headers["Authorization"] = `Bearer ${token}`;
  }
  if (body) opts.body = JSON.stringify(body);
  const r = await fetch(API + path, opts);
  if (r.status === 401) {
    clearToken();
    window.location.reload();
    throw new Error("Session expired. Please log in again.");
  }
  if (!r.ok) {
    const text = await r.text();
    throw new Error(text || `HTTP ${r.status}`);
  }
  const ct = r.headers.get("content-type") || "";
  if (ct.includes("application/json")) return r.json();
  return r.text();
}

// ---------------------------------------------------------------------------
// Shared UI components
// ---------------------------------------------------------------------------

const TIER_LABELS = { 1: "Critical", 2: "Standard", 3: "Exploratory" };

function TierBadge({ tier }) {
  const colors = {
    1: "bg-red-50 text-red-700 border border-red-200",
    2: "bg-amber-50 text-amber-700 border border-amber-200",
    3: "bg-blue-50 text-blue-700 border border-blue-200",
  };
  return (
    <span className={`text-xs font-medium px-1.5 py-0.5 rounded ${colors[tier] || "bg-slate-100 text-slate-500 border border-slate-300"}`}>
      {TIER_LABELS[tier] || `Tier ${tier}`}
    </span>
  );
}

// ---------------------------------------------------------------------------
// Default logo — D-mark (Abstract "D" with data-flow lines)
// ---------------------------------------------------------------------------
function DefaultLogo({ size = 28 }) {
  return (
    <svg width={size} height={size} viewBox="0 0 32 32" fill="none">
      <path d="M10 6v20" stroke="#3b82f6" strokeWidth="3" strokeLinecap="round"/>
      <path d="M10 6c12 0 16 6 16 10s-4 10-16 10" stroke="#3b82f6" strokeWidth="3" strokeLinecap="round" fill="none"/>
      <path d="M13 12h6" stroke="#60a5fa" strokeWidth="1.5" strokeLinecap="round"/>
      <path d="M13 16h8" stroke="#60a5fa" strokeWidth="1.5" strokeLinecap="round"/>
      <path d="M13 20h6" stroke="#93c5fd" strokeWidth="1.5" strokeLinecap="round"/>
    </svg>
  );
}

// SVG nav icons (16x16, Lucide-style stroke icons)
function NavIcon({ id }) {
  const p = { xmlns: "http://www.w3.org/2000/svg", width: 16, height: 16, viewBox: "0 0 24 24", fill: "none", stroke: "currentColor", strokeWidth: 1.75, strokeLinecap: "round", strokeLinejoin: "round" };
  const icons = {
    command: <svg {...p}><polyline points="4 17 10 11 4 5"/><line x1="12" y1="19" x2="20" y2="19"/></svg>,
    pipelines: <svg {...p}><line x1="6" y1="3" x2="6" y2="15"/><circle cx="18" cy="6" r="3"/><circle cx="6" cy="18" r="3"/><path d="M18 9a9 9 0 0 1-9 9"/></svg>,
    activity: <svg {...p}><polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/></svg>,
    freshness: <svg {...p}><circle cx="12" cy="12" r="10"/><polyline points="12 6 12 12 16 14"/></svg>,
    quality: <svg {...p}><path d="M12 22s8-4 8-10V5l-8-3-8 3v7c0 6 8 10 8 10z"/></svg>,
    approvals: <svg {...p}><polyline points="9 11 12 14 22 4"/><path d="M21 12v7a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V5a2 2 0 0 1 2-2h11"/></svg>,
    dag: <svg {...p}><circle cx="18" cy="5" r="3"/><circle cx="6" cy="12" r="3"/><circle cx="18" cy="19" r="3"/><line x1="8.59" y1="13.51" x2="15.42" y2="17.49"/><line x1="15.41" y1="6.51" x2="8.59" y2="10.49"/></svg>,
    connectors: <svg {...p}><path d="M12 2v6m0 8v6M4.93 4.93l4.24 4.24m5.66 5.66 4.24 4.24M2 12h6m8 0h6M4.93 19.07l4.24-4.24m5.66-5.66 4.24-4.24"/></svg>,
    alerts: <svg {...p}><path d="M18 8A6 6 0 0 0 6 8c0 7-3 9-3 9h18s-3-2-3-9"/><path d="M13.73 21a2 2 0 0 1-3.46 0"/></svg>,
    metrics: <svg {...p}><line x1="18" y1="20" x2="18" y2="10"/><line x1="12" y1="20" x2="12" y2="4"/><line x1="6" y1="20" x2="6" y2="14"/></svg>,
    settings: <svg {...p}><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 0 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 0 1-2.83-2.83l.06-.06A1.65 1.65 0 0 0 4.68 15a1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 0 1 2.83-2.83l.06.06A1.65 1.65 0 0 0 9 4.68a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 0 1 2.83 2.83l-.06.06A1.65 1.65 0 0 0 19.4 9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z"/></svg>,
    agent: <svg {...p}><path d="M12 2 2 7l10 5 10-5-10-5z"/><path d="m2 17 10 5 10-5"/><path d="m2 12 10 5 10-5"/></svg>,
    docs: <svg {...p}><path d="M2 3h6a4 4 0 0 1 4 4v14a3 3 0 0 0-3-3H2z"/><path d="M22 3h-6a4 4 0 0 0-4 4v14a3 3 0 0 1 3-3h7z"/></svg>,
  };
  return icons[id] || <svg {...p}><circle cx="12" cy="12" r="1"/></svg>;
}

function StatusDot({ status }) {
  const map = {
    active: "bg-green-400 shadow-green", ok: "bg-green-400", complete: "bg-green-400",
    fresh: "bg-green-400", passed: "bg-green-400", promote: "bg-green-400",
    paused: "bg-gray-500", pending: "bg-gray-500", untested: "bg-gray-500",
    warning: "bg-amber-400 shadow-amber", promote_with_warning: "bg-amber-400",
    failed: "bg-red-400 shadow-red", halted: "bg-red-400", critical: "bg-red-400",
    exhausted: "bg-red-400",
    draft: "bg-purple-400", approved: "bg-blue-400", applied: "bg-blue-400",
  };
  return <span className={`inline-block w-2 h-2 rounded-full ${map[status] || "bg-slate-400"}`} />;
}

function Pill({ label, color = "blue" }) {
  const colors = {
    blue: "bg-blue-50 text-blue-700 border border-blue-200",
    green: "bg-green-50 text-green-700 border border-green-200",
    amber: "bg-amber-50 text-amber-700 border border-amber-200",
    red: "bg-red-50 text-red-700 border border-red-200",
    purple: "bg-purple-50 text-purple-700 border border-purple-200",
    gray: "bg-slate-100 text-slate-500 border border-slate-200",
  };
  return (
    <span className={`text-xs font-medium px-2 py-0.5 rounded-full ${colors[color] || colors.gray}`}>
      {label}
    </span>
  );
}

function ProgressBar({ pct, color = "blue" }) {
  const barColors = {
    green: "bg-green-500",
    amber: "bg-amber-500",
    red: "bg-red-500",
    blue: "bg-blue-500",
  };
  return (
    <div className="h-1.5 bg-slate-200 rounded-full overflow-hidden">
      <div
        className={`h-full ${barColors[color] || barColors.blue} rounded-full transition-all`}
        style={{ width: `${Math.min(100, Math.max(0, pct))}%` }}
      />
    </div>
  );
}

function RunRow({ r }) {
  const [showQuality, setShowQuality] = useState(false);
  const duration = r.started_at && r.completed_at
    ? Math.round((new Date(r.completed_at) - new Date(r.started_at)) / 1000)
    : null;
  const fmtDuration = duration != null
    ? duration >= 60 ? `${Math.floor(duration / 60)}m ${duration % 60}s` : `${duration}s`
    : null;
  const fmtBytes = (b) => {
    if (!b) return null;
    if (b > 1048576) return `${(b / 1048576).toFixed(1)} MB`;
    if (b > 1024) return `${(b / 1024).toFixed(1)} KB`;
    return `${b} B`;
  };
  const checks = r.quality_results
    ? (Array.isArray(r.quality_results.checks) ? r.quality_results.checks : [])
    : [];
  return (
    <div className="border border-slate-200 rounded-lg px-3 py-2">
      <div className="flex items-center gap-2 text-xs flex-wrap">
        <StatusDot status={r.status} />
        <span className="font-mono text-slate-400">{r.started_at?.slice(0, 16)}</span>
        {fmtDuration && <span className="text-slate-400">{fmtDuration}</span>}
        <Pill label={r.run_mode || "scheduled"} color="blue" />
        {r.triggered_by_pipeline_id && (
          <span className="text-[10px] text-slate-400 italic">from {r.triggered_by_pipeline_id?.slice(0, 8)}</span>
        )}
        <span className="text-slate-500">{r.rows_extracted?.toLocaleString()} extracted</span>
        {r.rows_loaded > 0 && <span className="text-slate-500">{r.rows_loaded?.toLocaleString()} loaded</span>}
        {fmtBytes(r.staging_size_bytes) && <span className="text-slate-400">{fmtBytes(r.staging_size_bytes)}</span>}
        <Pill
          label={r.gate_decision || r.status}
          color={r.gate_decision === "halt" ? "red" : r.gate_decision === "promote_with_warning" ? "amber" : "green"}
        />
      </div>
      {(r.watermark_before || r.watermark_after) && (
        <div className="text-xs text-slate-400 mt-1 font-mono">
          watermark: {r.watermark_before || "null"} &rarr; {r.watermark_after || "null"}
        </div>
      )}
      {checks.length > 0 && (
        <div className="mt-1">
          <button
            onClick={() => setShowQuality(!showQuality)}
            className="text-xs text-blue-500 hover:text-blue-700"
          >
            {showQuality ? "Hide" : "Show"} quality checks ({checks.length})
          </button>
          {showQuality && (
            <div className="mt-1 bg-slate-50 rounded p-2 space-y-0.5">
              {checks.map((c, i) => (
                <div key={i} className="flex items-center gap-2 text-xs">
                  <span className={`w-2 h-2 rounded-full ${
                    c.status === "pass" ? "bg-green-400" :
                    c.status === "warn" ? "bg-amber-400" : "bg-red-400"
                  }`} />
                  <span className="font-medium text-slate-600">{c.name}</span>
                  {c.detail && <span className="text-slate-400 truncate">{c.detail}</span>}
                </div>
              ))}
            </div>
          )}
        </div>
      )}
      {r.error && <div className="text-xs text-red-500 mt-1 truncate">{r.error}</div>}
    </div>
  );
}

function ErrorBudgetCard({ eb }) {
  const utilizationPct = eb.utilization_pct != null
    ? eb.utilization_pct
    : (eb.budget_remaining != null
      ? Math.max(0, Math.min(100, (1 - (eb.success_rate || 0)) / (1 - (eb.budget_threshold || 0.9)) * 100))
      : 0);
  const color = !eb ? "gray" : utilizationPct > 80 ? "red" : utilizationPct > 50 ? "amber" : "green";
  return (
    <div className="bg-slate-50 border border-slate-300 rounded-lg px-4 py-3">
      <div className="flex items-center justify-between mb-2">
        <span className="text-xs font-semibold text-slate-500">Error Budget</span>
        <Pill
          label={eb.escalated ? "EXHAUSTED" : `${utilizationPct.toFixed(1)}% used`}
          color={color}
        />
      </div>
      <ProgressBar pct={utilizationPct} color={color} />
      <div className="text-xs text-slate-400 mt-1">
        {eb.successful_runs}/{eb.total_runs} runs successful ({eb.window_days}d window) — threshold {((eb.budget_threshold || 0.9) * 100).toFixed(0)}%
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// StepDAGSection — shows pipeline steps with transform SQL details
// ---------------------------------------------------------------------------

function TransformStepCard({ step, tf, nameById, typeColors, onTransformUpdate }) {
  const [isExpanded, setExpanded] = useState(false);
  const [editing, setEditing] = useState(false);
  const [editSql, setEditSql] = useState("");
  const [editDesc, setEditDesc] = useState("");
  const [editMat, setEditMat] = useState("");
  const [saving, setSaving] = useState(false);
  const [approving, setApproving] = useState(false);
  const [validating, setValidating] = useState(false);
  const [validationResult, setValidationResult] = useState(null);
  const [msg, setMsg] = useState(null);

  const colorCls = typeColors[step.step_type] || typeColors.custom;
  const matColor = (m) =>
    m === "view" ? "bg-indigo-100 text-indigo-700" :
    m === "incremental" ? "bg-orange-100 text-orange-700" :
    m === "ephemeral" ? "bg-slate-100 text-slate-600" :
    "bg-emerald-100 text-emerald-700";

  function startEdit() {
    setEditSql(tf.sql);
    setEditDesc(tf.description || "");
    setEditMat(tf.materialization);
    setEditing(true);
    setValidationResult(null);
    setMsg(null);
  }

  function cancelEdit() {
    setEditing(false);
    setValidationResult(null);
    setMsg(null);
  }

  async function handleValidate() {
    setValidating(true);
    setValidationResult(null);
    try {
      const res = await api("POST", `/api/transforms/${tf.transform_id}/validate`);
      setValidationResult(res);
    } catch (e) {
      setValidationResult({ valid: false, error: e.message });
    }
    setValidating(false);
  }

  async function handleSave() {
    setSaving(true);
    setMsg(null);
    try {
      const body = { sql: editSql, approved: false };
      if (editDesc !== (tf.description || "")) body.description = editDesc;
      if (editMat !== tf.materialization) body.materialization = editMat;
      const res = await api("PATCH", `/api/transforms/${tf.transform_id}`, body);
      // Re-fetch updated transform
      const updated = await api("GET", `/api/transforms/${tf.transform_id}`);
      onTransformUpdate(step.step_id, updated);
      setEditing(false);
      setMsg({ type: "ok", text: `Saved v${res.version} — pending approval` });
    } catch (e) {
      setMsg({ type: "err", text: e.message });
    }
    setSaving(false);
  }

  async function handleApprove() {
    setApproving(true);
    setMsg(null);
    try {
      const res = await api("PATCH", `/api/transforms/${tf.transform_id}`, { approved: true });
      const updated = await api("GET", `/api/transforms/${tf.transform_id}`);
      onTransformUpdate(step.step_id, updated);
      setMsg({ type: "ok", text: `Approved v${res.version}` });
    } catch (e) {
      setMsg({ type: "err", text: e.message });
    }
    setApproving(false);
  }

  return (
    <div className="border border-slate-300 rounded-lg bg-white w-full">
      <div
        className="flex items-center gap-2 px-3 py-2 cursor-pointer"
        onClick={() => { if (!editing) setExpanded(!isExpanded); }}
      >
        <span className={"text-[10px] font-bold px-1.5 py-0.5 rounded " + colorCls}>
          {step.step_type}
        </span>
        <span className="text-xs font-medium text-slate-700">
          {step.step_name || step.step_id.slice(0, 8)}
        </span>
        {step.depends_on && step.depends_on.length > 0 && (
          <span className="text-[10px] text-slate-400">
            depends on: {step.depends_on.map((id) => nameById[id] || id.slice(0, 8)).join(", ")}
          </span>
        )}
        <span className="ml-auto text-[10px] text-slate-400">
          {isExpanded ? "collapse" : "expand"}
        </span>
      </div>

      {isExpanded && (
        <div className="border-t border-slate-200 px-3 py-2 space-y-2">
          {/* Header row: materialization, target, refs, status, actions */}
          <div className="flex items-center gap-3 flex-wrap">
            <span className={"text-[10px] font-bold px-1.5 py-0.5 rounded " + matColor(tf.materialization)}>
              {tf.materialization}
            </span>
            <span className="text-[10px] text-slate-500">
              {tf.target_schema}.{tf.target_table}
            </span>
            {tf.refs && tf.refs.length > 0 && (
              <span className="text-[10px] text-slate-400">
                refs: {tf.refs.join(", ")}
              </span>
            )}
            {tf.approved ? (
              <span className="text-[10px] text-green-600 font-medium">approved</span>
            ) : (
              <span className="text-[10px] text-amber-600 font-medium">pending approval</span>
            )}
            <span className="text-[10px] text-slate-400">v{tf.version}</span>
            <div className="ml-auto flex gap-1">
              {!editing && (
                <button
                  onClick={(e) => { e.stopPropagation(); startEdit(); }}
                  className="text-[10px] px-2 py-0.5 rounded bg-slate-200 text-slate-600 hover:bg-slate-300"
                >Edit SQL</button>
              )}
              {!editing && !tf.approved && (
                <button
                  onClick={(e) => { e.stopPropagation(); handleApprove(); }}
                  disabled={approving}
                  className="text-[10px] px-2 py-0.5 rounded bg-green-100 text-green-700 hover:bg-green-200 disabled:opacity-50"
                >{approving ? "Approving..." : "Approve"}</button>
              )}
            </div>
          </div>

          {/* Status message */}
          {msg && (
            <div className={"text-[10px] px-2 py-1 rounded " + (msg.type === "ok" ? "bg-green-50 text-green-700" : "bg-red-50 text-red-700")}>
              {msg.text}
            </div>
          )}

          {/* Description */}
          {!editing && tf.description && (
            <div className="text-xs text-slate-600">{tf.description}</div>
          )}

          {/* Edit mode */}
          {editing ? (
            <div className="space-y-2">
              <div className="flex gap-2 items-center">
                <label className="text-[10px] text-slate-500">Description</label>
                <input
                  className="flex-1 text-xs border border-slate-300 rounded px-2 py-1 bg-white"
                  value={editDesc}
                  onChange={(e) => setEditDesc(e.target.value)}
                />
                <label className="text-[10px] text-slate-500">Materialization</label>
                <select
                  className="text-xs border border-slate-300 rounded px-2 py-1 bg-white"
                  value={editMat}
                  onChange={(e) => setEditMat(e.target.value)}
                >
                  <option value="table">table</option>
                  <option value="view">view</option>
                  <option value="incremental">incremental</option>
                  <option value="ephemeral">ephemeral</option>
                </select>
              </div>
              <textarea
                className="w-full text-xs font-mono bg-slate-900 text-green-300 rounded p-3 border-0 outline-none resize-y"
                style={{ minHeight: "160px" }}
                value={editSql}
                onChange={(e) => setEditSql(e.target.value)}
                spellCheck={false}
              />
              {validationResult && (
                <div className={"text-[10px] px-2 py-1 rounded " + (validationResult.valid ? "bg-green-50 text-green-700" : "bg-red-50 text-red-700")}>
                  {validationResult.valid ? "Validation passed" : `Validation failed: ${validationResult.error || JSON.stringify(validationResult.errors || [])}`}
                </div>
              )}
              <div className="flex gap-2 justify-end">
                <button
                  onClick={handleValidate}
                  disabled={validating}
                  className="text-[10px] px-3 py-1 rounded bg-blue-100 text-blue-700 hover:bg-blue-200 disabled:opacity-50"
                >{validating ? "Validating..." : "Validate"}</button>
                <button
                  onClick={cancelEdit}
                  className="text-[10px] px-3 py-1 rounded bg-slate-200 text-slate-600 hover:bg-slate-300"
                >Cancel</button>
                <button
                  onClick={handleSave}
                  disabled={saving || editSql === tf.sql && editDesc === (tf.description || "") && editMat === tf.materialization}
                  className="text-[10px] px-3 py-1 rounded bg-purple-600 text-white hover:bg-purple-700 disabled:opacity-50"
                >{saving ? "Saving..." : "Save & Submit for Approval"}</button>
              </div>
            </div>
          ) : (
            <pre className="text-xs bg-slate-900 text-green-300 rounded p-3 overflow-x-auto whitespace-pre-wrap">{tf.sql}</pre>
          )}

          {/* Variables */}
          {tf.variables && Object.keys(tf.variables).length > 0 && (
            <div className="text-[10px] text-slate-400">
              variables: {Object.entries(tf.variables).map(([k, v]) => `${k}=${v}`).join(", ")}
            </div>
          )}
        </div>
      )}

      {!isExpanded && (
        <div className="border-t border-slate-200 px-3 py-1.5">
          <div className="flex items-center gap-2">
            <span className={"text-[10px] font-bold px-1.5 py-0.5 rounded " + matColor(tf.materialization)}>
              {tf.materialization}
            </span>
            <span className="text-[10px] text-slate-500 truncate flex-1">
              {tf.description || tf.sql.slice(0, 80) + (tf.sql.length > 80 ? "..." : "")}
            </span>
            {tf.approved ? (
              <span className="text-[10px] text-green-600">approved</span>
            ) : (
              <span className="text-[10px] text-amber-600">pending</span>
            )}
          </div>
        </div>
      )}
    </div>
  );
}

function StepDAGSection({ steps, pipelineId }) {
  const [transforms, setTransforms] = useState({});

  useEffect(() => {
    if (!steps || steps.length === 0) return;
    const transformSteps = steps.filter(
      (s) => s.step_type === "transform" && s.config && s.config.transform_id
    );
    if (transformSteps.length === 0) return;
    Promise.all(
      transformSteps.map((s) =>
        api("GET", `/api/transforms/${s.config.transform_id}`)
          .then((t) => ({ stepId: s.step_id, transform: t }))
          .catch(() => null)
      )
    ).then((results) => {
      const map = {};
      results.filter(Boolean).forEach((r) => { map[r.stepId] = r.transform; });
      setTransforms(map);
    });
  }, [steps]);

  function handleTransformUpdate(stepId, updated) {
    setTransforms((prev) => ({ ...prev, [stepId]: updated }));
  }

  if (!steps || steps.length === 0) return null;

  // Build step name lookup
  const nameById = {};
  steps.forEach((s) => { nameById[s.step_id] = s.step_name || s.step_id.slice(0, 8); });

  // Group steps into layers by topological depth
  const depthOf = {};
  function calcDepth(s) {
    if (depthOf[s.step_id] !== undefined) return depthOf[s.step_id];
    if (!s.depends_on || s.depends_on.length === 0) { depthOf[s.step_id] = 0; return 0; }
    const maxParent = Math.max(...s.depends_on.map((id) => {
      const parent = steps.find((p) => p.step_id === id);
      return parent ? calcDepth(parent) : 0;
    }));
    depthOf[s.step_id] = maxParent + 1;
    return depthOf[s.step_id];
  }
  steps.forEach((s) => calcDepth(s));

  const layers = {};
  steps.forEach((s) => {
    const d = depthOf[s.step_id] || 0;
    if (!layers[d]) layers[d] = [];
    layers[d].push(s);
  });
  const sortedDepths = Object.keys(layers).map(Number).sort((a, b) => a - b);

  const typeColors = {
    extract: "bg-blue-100 text-blue-700",
    transform: "bg-purple-100 text-purple-700",
    quality_gate: "bg-amber-100 text-amber-700",
    promote: "bg-green-100 text-green-700",
    cleanup: "bg-red-100 text-red-700",
    hook: "bg-teal-100 text-teal-700",
    sensor: "bg-cyan-100 text-cyan-700",
    custom: "bg-slate-100 text-slate-600",
  };

  return (
    <div className="bg-slate-50 border border-slate-200 rounded-lg px-4 py-3">
      <div className="text-xs font-semibold text-slate-500 mb-3">Pipeline Steps</div>
      <div className="space-y-3">
        {sortedDepths.map((depth) => (
          <div key={depth}>
            {sortedDepths.length > 1 && (
              <div className="text-[10px] text-slate-400 mb-1">Layer {depth + 1}</div>
            )}
            <div className="flex flex-wrap gap-2">
              {layers[depth].map((step) => {
                const tf = transforms[step.step_id];
                const colorCls = typeColors[step.step_type] || typeColors.custom;
                if (tf) {
                  return (
                    <TransformStepCard
                      key={step.step_id}
                      step={step}
                      tf={tf}
                      nameById={nameById}
                      typeColors={typeColors}
                      onTransformUpdate={handleTransformUpdate}
                    />
                  );
                }
                return (
                  <div key={step.step_id} className="border border-slate-300 rounded-lg bg-white">
                    <div className="flex items-center gap-2 px-3 py-2">
                      <span className={"text-[10px] font-bold px-1.5 py-0.5 rounded " + colorCls}>
                        {step.step_type}
                      </span>
                      <span className="text-xs font-medium text-slate-700">
                        {step.step_name || step.step_id.slice(0, 8)}
                      </span>
                      {step.depends_on && step.depends_on.length > 0 && (
                        <span className="text-[10px] text-slate-400">
                          depends on: {step.depends_on.map((id) => nameById[id] || id.slice(0, 8)).join(", ")}
                        </span>
                      )}
                      {step.config && Object.keys(step.config).length > 0 && (
                        <span className="text-[10px] text-slate-400 ml-auto">
                          {Object.entries(step.config).map(([k, v]) => `${k}: ${v}`).join(", ")}
                        </span>
                      )}
                    </div>
                  </div>
                );
              })}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// ConnectorCodeSection — shows source/target connector code for ingestion DAGs
// ---------------------------------------------------------------------------

function ConnectorCodeSection({ sourceConnectorId, targetConnectorId }) {
  const [connectors, setConnectors] = useState({});
  const [expanded, setExpanded] = useState({});

  useEffect(() => {
    const ids = [
      sourceConnectorId && { key: "source", id: sourceConnectorId },
      targetConnectorId && { key: "target", id: targetConnectorId },
    ].filter(Boolean);
    if (ids.length === 0) return;
    Promise.all(
      ids.map((item) =>
        api("GET", `/api/connectors/${item.id}`)
          .then((c) => ({ key: item.key, connector: c }))
          .catch(() => null)
      )
    ).then((results) => {
      const map = {};
      results.filter(Boolean).forEach((r) => { map[r.key] = r.connector; });
      setConnectors(map);
    });
  }, [sourceConnectorId, targetConnectorId]);

  if (!connectors.source && !connectors.target) return null;

  const toggle = (key) => setExpanded((prev) => ({ ...prev, [key]: !prev[key] }));

  function ConnectorPanel({ label, conn, connKey }) {
    if (!conn) return null;
    const isExp = expanded[connKey];
    const roleColor = connKey === "source" ? "bg-blue-100 text-blue-700" : "bg-green-100 text-green-700";
    return (
      <div className="border border-slate-300 rounded-lg bg-white">
        <div className="flex items-center gap-2 px-3 py-2 cursor-pointer" onClick={() => toggle(connKey)}>
          <span className={"text-[10px] font-bold px-1.5 py-0.5 rounded " + roleColor}>{label}</span>
          <span className="text-xs font-medium text-slate-700">{conn.name}</span>
          <Pill label={conn.connector_type} color="purple" />
          <Pill label={conn.db_type} color="gray" />
          {conn.interface_version && <span className="text-[10px] text-slate-400">v{conn.interface_version}</span>}
          <span className="ml-auto text-[10px] text-slate-400">{isExp ? "collapse" : "expand"}</span>
        </div>
        {isExp && (
          <div className="border-t border-slate-200 px-3 py-2 space-y-2">
            {conn.dependencies && conn.dependencies.length > 0 && (
              <div className="text-[10px] text-slate-400">
                dependencies: {conn.dependencies.join(", ")}
              </div>
            )}
            <pre className="text-xs bg-slate-900 text-green-300 rounded p-3 overflow-x-auto whitespace-pre-wrap max-h-96 overflow-y-auto">{conn.code}</pre>
          </div>
        )}
        {!isExp && (
          <div className="border-t border-slate-200 px-3 py-1.5">
            <div className="text-[10px] text-slate-500 truncate">
              {(conn.code || "").split("\n").find((l) => l.trim().startsWith("class ")) || `${(conn.code || "").split("\n").length} lines`}
            </div>
          </div>
        )}
      </div>
    );
  }

  return (
    <div className="bg-slate-50 border border-slate-200 rounded-lg px-4 py-3">
      <div className="text-xs font-semibold text-slate-500 mb-3">Connector Code</div>
      <div className="space-y-2">
        <ConnectorPanel label="Source" conn={connectors.source} connKey="source" />
        <ConnectorPanel label="Target" conn={connectors.target} connKey="target" />
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Login component
// ---------------------------------------------------------------------------

function Login({ onLogin }) {
  const [username, setUsername] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);

  async function handleSubmit(e) {
    e.preventDefault();
    setError("");
    setLoading(true);
    try {
      const data = await api("POST", "/api/auth/login", { username, password });
      localStorage.setItem("pa_token", data.token);
      localStorage.setItem("pa_user", JSON.stringify({ user_id: data.user_id, username: data.username, role: data.role }));
      onLogin(data);
    } catch (err) {
      setError("Invalid credentials. Please try again.");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="min-h-screen flex items-center justify-center bg-slate-50">
      <div className="w-96 bg-white border border-slate-200 rounded-xl p-8 shadow-lg">
        <div className="text-center mb-8">
          <div className="flex justify-center mb-3"><DefaultLogo size={48} /></div>
          <div className="text-2xl font-semibold text-slate-800 font-ui tracking-wide">DAPOS</div>
          <div className="text-sm text-slate-400 mt-1">Sign in to continue</div>
        </div>
        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label className="block text-xs text-slate-400 mb-1.5">Username</label>
            <input
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              className="w-full px-4 py-2.5 bg-slate-50 border border-slate-300 rounded-lg text-sm text-slate-700 outline-none focus:border-blue-500"
              autoFocus
            />
          </div>
          <div>
            <label className="block text-xs text-slate-400 mb-1.5">Password</label>
            <input
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              className="w-full px-4 py-2.5 bg-slate-50 border border-slate-300 rounded-lg text-sm text-slate-700 outline-none focus:border-blue-500"
            />
          </div>
          {error && <div className="text-xs text-red-600 bg-red-50 border border-red-200/50 rounded-lg px-3 py-2">{error}</div>}
          <button
            type="submit"
            disabled={loading || !username || !password}
            className="w-full py-2.5 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700 disabled:opacity-50 transition-colors"
          >
            {loading ? "Signing in..." : "Sign In"}
          </button>
        </form>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Onboarding Guide — spotlight style, no overlay
// ---------------------------------------------------------------------------

const GUIDE_HINTS = {
  command: {
    title: "Welcome to DAPOS",
    text: "Describe what you need and the AI builds, runs, and monitors your pipelines. This is your Command tab \u2014 ask anything in plain language. Try: \"list my pipelines\" or \"why is orders failing\"",
  },
  pipelines: {
    title: "Pipelines",
    text: "All your data pipelines with status, schedule, and quick actions. Click any pipeline to see config, trigger runs, or edit settings.",
  },
  activity: {
    title: "Activity",
    text: "Every pipeline run with a 13-step execution timeline. Expand a run to see extract \u2192 load \u2192 quality gate \u2192 promote detail.",
  },
  freshness: {
    title: "Freshness",
    text: "Data staleness monitoring with time-series charts. Each pipeline is measured against its SLA \u2014 green means fresh, red means stale.",
  },
  quality: {
    title: "Quality",
    text: "7-check quality gate trends: count reconciliation, schema consistency, PK uniqueness, null rates, volume z-score, sample verification, freshness.",
  },
  alerts: {
    title: "Alerts",
    text: "Pipeline failures, SLA breaches, and anomaly alerts. Dispatches to Slack, email, or PagerDuty based on tier.",
  },
  dag: {
    title: "Lineage",
    text: "Visual dependency graph showing how pipelines connect. See upstream/downstream relationships and data contracts.",
  },
  connectors: {
    title: "Connectors",
    text: "Source and target connectors \u2014 8 built-in, unlimited via AI generation. Ask the agent to generate any connector you need.",
  },
  costs: {
    title: "Costs",
    text: "Every AI call tracked with token counts and latency. See exactly what the agent costs per operation.",
  },
  docs: {
    title: "Docs",
    text: "Full documentation: quickstart, architecture, API reference, concepts, and more \u2014 all available in-app.",
  },
};

const GUIDE_ORDER = ["command", "pipelines", "activity", "freshness", "quality", "alerts", "dag", "connectors", "settings", "docs"];

// ---------------------------------------------------------------------------
// Sidebar
// ---------------------------------------------------------------------------

const NAV = [
  { id: "command", label: "Command" },
  { id: "pipelines", label: "Pipelines" },
  { id: "activity", label: "Activity" },
  { id: "freshness", label: "Freshness" },
  { id: "quality", label: "Quality" },
  { id: "approvals", label: "Approvals" },
  { id: "dag", label: "Lineage" },
  { id: "connectors", label: "Connectors" },
  { id: "alerts", label: "Alerts" },
  { id: "metrics", label: "Metrics" },
  { id: "settings", label: "Settings" },
  { id: "agent", label: "Agent" },
  { id: "docs", label: "Docs" },
];

function Sidebar({ view, setView, tierFilter, setTierFilter, searchQuery, setSearchQuery, user, onLogout, guideStep, onGuideNav, branding }) {
  const guideId = guideStep !== null ? GUIDE_ORDER[guideStep] : null;
  const appName = branding?.app_name || "DAPOS";
  const logoUrl = branding?.logo_url || "";

  return (
    <div className="w-56 min-h-screen bg-slate-950 flex flex-col shrink-0">
      <div className="px-4 py-4 border-b border-slate-800">
        <div className="flex items-center gap-2.5">
          <div className="shrink-0">
            {logoUrl ? (
              <img src={logoUrl} alt={appName} className="w-7 h-7 rounded object-contain" />
            ) : (
              <DefaultLogo size={28} />
            )}
          </div>
          <div>
            <div className="text-sm font-semibold text-white font-ui tracking-wide">{appName}</div>
            <div className="text-[10px] text-slate-500">Agentic Data Platform</div>
          </div>
        </div>
      </div>
      <nav className="flex-1 px-2 py-3 space-y-0.5">
        {NAV.map((n) => {
          const isGuideTarget = guideId === n.id;
          return (
            <button
              key={n.id}
              data-nav-id={n.id}
              onClick={() => {
                setView(n.id);
                if (guideStep !== null && GUIDE_ORDER.includes(n.id)) {
                  onGuideNav(n.id);
                }
              }}
              className={`w-full flex items-center gap-2.5 px-3 py-2 rounded-lg text-sm transition-colors ${
                view === n.id
                  ? "bg-slate-800 text-white font-medium"
                  : "text-slate-400 hover:bg-slate-900 hover:text-slate-200"
              } ${isGuideTarget ? "ring-2 ring-blue-400 ring-offset-1 ring-offset-slate-950" : ""}`}
            >
              <span className="w-4 flex items-center justify-center opacity-70"><NavIcon id={n.id} /></span>
              {n.label}
              {isGuideTarget && (
                <span className="ml-auto w-2 h-2 rounded-full bg-blue-400 animate-pulse" />
              )}
            </button>
          );
        })}
      </nav>
      <div className="px-3 py-2 border-t border-slate-800">
        <div className="relative">
          <input
            type="text"
            value={searchQuery}
            onChange={(e) => setSearchQuery(e.target.value)}
            placeholder="Search pipelines..."
            className="w-full text-xs px-2.5 py-1.5 border border-slate-700 rounded-lg bg-slate-900 text-slate-300 focus:outline-none focus:border-blue-500 focus:bg-slate-800 font-mono placeholder-slate-600"
          />
          {searchQuery && (
            <button
              onClick={() => setSearchQuery("")}
              className="absolute right-2 top-1/2 -translate-y-1/2 text-slate-500 hover:text-slate-300 text-xs"
            >&times;</button>
          )}
        </div>
      </div>
      <div className="px-3 py-2 border-t border-slate-800">
        <div className="text-xs text-slate-500 mb-2 px-1">Priority filter</div>
        <div className="flex gap-1">
          {[{ key: "All", label: "All" }, { key: "T1", label: "Critical" }, { key: "T2", label: "Standard" }, { key: "T3", label: "Exploratory" }].map((t) => (
            <button
              key={t.key}
              onClick={() => setTierFilter(t.key)}
              className={`flex-1 text-xs py-1 rounded ${
                tierFilter === t.key
                  ? "bg-blue-600 text-white"
                  : "bg-slate-800 text-slate-400 hover:bg-slate-700 hover:text-slate-200"
              }`}
            >
              {t.label}
            </button>
          ))}
        </div>
      </div>
      {user && (
        <div className="px-3 py-3 border-t border-slate-800">
          <div className="flex items-center justify-between">
            <div>
              <div className="text-xs text-slate-300 font-medium">{user.username}</div>
              <div className="text-xs text-slate-600">{user.role}</div>
            </div>
            <button onClick={onLogout} className="text-xs text-slate-500 hover:text-slate-300 px-2 py-1 rounded hover:bg-slate-800">
              Logout
            </button>
          </div>
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Guide Tooltip — rendered as a fixed-position portal in the App, not inside sidebar
// ---------------------------------------------------------------------------

function GuideTooltip({ guideStep, setView, onGuideNav, onGuideFinish }) {
  const guideId = GUIDE_ORDER[guideStep];
  const hint = GUIDE_HINTS[guideId];
  const isLastStep = guideStep === GUIDE_ORDER.length - 1;
  const [pos, setPos] = useState({ top: 100, left: 240 });

  useEffect(() => {
    const el = document.querySelector(`[data-nav-id="${guideId}"]`);
    if (el) {
      const rect = el.getBoundingClientRect();
      setPos({ top: rect.top, left: rect.right + 12 });
    }
  }, [guideId]);

  if (!hint) return null;

  return (
    <div className="fixed z-50" style={{ top: pos.top, left: pos.left, width: 300 }}>
      <div className="bg-white border border-slate-200 rounded-xl shadow-lg p-4">
        {/* Arrow pointing left */}
        <div className="absolute -left-2 top-3 w-0 h-0" style={{ borderTop: "6px solid transparent", borderBottom: "6px solid transparent", borderRight: "8px solid #e2e8f0" }} />
        <div className="absolute top-3 w-0 h-0" style={{ left: "-5.5px", borderTop: "5px solid transparent", borderBottom: "5px solid transparent", borderRight: "7px solid white", marginTop: "1px" }} />

        <div className="text-xs text-blue-600 font-semibold uppercase tracking-wider mb-1">{hint.title}</div>
        <p className="text-sm text-slate-600 leading-relaxed">{hint.text}</p>

        <div className="flex items-center justify-between mt-3 pt-3 border-t border-slate-100">
          <span className="text-xs text-slate-300">{guideStep + 1} / {GUIDE_ORDER.length}</span>
          <div className="flex gap-2">
            <button
              onClick={onGuideFinish}
              className="text-xs text-slate-400 hover:text-slate-600 px-2 py-1"
            >
              End tour
            </button>
            <button
              onClick={() => {
                if (isLastStep) {
                  onGuideFinish();
                } else {
                  const nextId = GUIDE_ORDER[guideStep + 1];
                  setView(nextId);
                  onGuideNav(nextId);
                }
              }}
              className="text-xs text-white bg-blue-600 hover:bg-blue-700 px-3 py-1 rounded-md font-medium"
            >
              {isLastStep ? "Done" : "Next"}
            </button>
          </div>
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// 1. Command View (agent-routed)
// ---------------------------------------------------------------------------

const CHAT_GREETING = { role: "agent", text: "Hello! I'm DAPOS. I can help you connect to databases, discover schemas, set up data pipelines, analyze quality, and much more.\n\nTry asking me to discover tables in a database, profile a table, or create a pipeline. What would you like to do?" };

function CommandView() {
  const [messages, setMessages] = useState(() => {
    try {
      const saved = sessionStorage.getItem("pa_chat");
      if (saved) return JSON.parse(saved);
    } catch {}
    return [CHAT_GREETING];
  });
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [sessionId] = useState(() => sessionStorage.getItem("pa_session") || "session-" + Date.now());
  const endRef = useRef();

  // Persist chat and session to sessionStorage
  useEffect(() => {
    try { sessionStorage.setItem("pa_chat", JSON.stringify(messages)); } catch {}
  }, [messages]);
  useEffect(() => { sessionStorage.setItem("pa_session", sessionId); }, [sessionId]);

  useEffect(() => {
    endRef.current?.scrollIntoView({ behavior: "smooth" });
  }, [messages]);

  const chips = [
    "What connectors are available?",
    "List my pipelines",
    "Show freshness report",
    "Show recent alerts",
    "Check pipeline status",
    "Discover tables in my SQLite at ./data/demo/sample.db",
  ];

  async function send(text) {
    if (!text.trim()) return;
    const msg = text.trim();
    setInput("");
    setMessages((m) => [...m, { role: "user", text: msg }]);
    setLoading(true);
    try {
      const result = await api("POST", "/api/command", { text: msg, session_id: sessionId });
      const reply = typeof result.response === "string" ? result.response : JSON.stringify(result.response, null, 2);
      setMessages((m) => [...m, { role: "agent", text: reply }]);
    } catch (e) {
      setMessages((m) => [...m, { role: "agent", text: `Error: ${e.message}` }]);
    } finally {
      setLoading(false);
    }
  }

  return (
    <div className="flex flex-col h-full">
      <div className="px-6 py-4 border-b border-slate-200">
        <h1 className="text-lg font-semibold text-slate-800">Command</h1>
        <p className="text-sm text-slate-400">Chat with DAPOS</p>
      </div>
      <div className="flex-1 overflow-y-auto px-6 py-4 space-y-4">
        {messages.map((m, i) => (
          <div key={i} className={`flex gap-3 ${m.role === "user" ? "flex-row-reverse" : ""}`}>
            {m.role === "agent" && (
              <div className="w-8 h-8 rounded-full bg-blue-600 flex items-center justify-center text-white text-xs font-mono flex-shrink-0">
                PA
              </div>
            )}
            <div
              className={`max-w-lg px-4 py-3 rounded-xl text-sm whitespace-pre-wrap ${
                m.role === "agent"
                  ? "bg-slate-100 border border-slate-300 text-slate-700"
                  : "bg-blue-600 text-white"
              }`}
            >
              {m.text}
            </div>
          </div>
        ))}
        {loading && (
          <div className="flex gap-3">
            <div className="w-8 h-8 rounded-full bg-blue-600 flex items-center justify-center text-white text-xs font-mono">
              PA
            </div>
            <div className="px-4 py-3 rounded-xl bg-slate-100 border border-slate-300 text-slate-400 text-sm animate-pulse">
              Thinking...
            </div>
          </div>
        )}
        <div ref={endRef} />
      </div>
      <div className="px-6 py-3 border-t border-slate-200">
        <div className="flex flex-wrap gap-2 mb-3">
          {chips.map((c) => (
            <button
              key={c}
              onClick={() => send(c)}
              className="text-xs px-3 py-1.5 bg-slate-100 hover:bg-slate-200 text-slate-500 hover:text-slate-700 rounded-full border border-slate-300 transition-colors"
            >
              {c}
            </button>
          ))}
        </div>
        <div className="flex gap-2">
          <input
            value={input}
            onChange={(e) => setInput(e.target.value)}
            onKeyDown={(e) => e.key === "Enter" && send(input)}
            placeholder="Ask anything about your pipelines..."
            className="flex-1 px-4 py-2.5 bg-slate-100 border border-slate-300 rounded-lg text-sm text-slate-700 outline-none focus:border-blue-500 placeholder-slate-400"
          />
          <button
            onClick={() => send(input)}
            className="px-4 py-2.5 bg-blue-600 text-white rounded-lg text-sm font-medium hover:bg-blue-700"
          >
            Send
          </button>
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// 2. Pipelines View
// ---------------------------------------------------------------------------


function PipelinesView({ tierFilter, searchQuery }) {
  const [pipelines, setPipelines] = useState([]);
  const [expanded, setExpanded] = useState(null);
  const [detail, setDetail] = useState(null);
  const [runs, setRuns] = useState([]);
  const [editForm, setEditForm] = useState(null);
  const [saving, setSaving] = useState(false);
  const [yamlView, setYamlView] = useState(null);
  const [timeline, setTimeline] = useState([]);

  useEffect(() => {
    const tierParam = tierFilter !== "All" ? `&tier=${tierFilter[1]}` : "";
    api("GET", `/api/pipelines?${tierParam}`).then(setPipelines).catch(console.error);
  }, [tierFilter]);

  const filteredPipelines = searchQuery
    ? pipelines.filter((p) => p.pipeline_name?.toLowerCase().includes(searchQuery))
    : pipelines;

  async function expand(p) {
    if (expanded === p.pipeline_id) {
      setExpanded(null);
      setEditForm(null);
      setYamlView(null);
      setTimeline([]);
      return;
    }
    setExpanded(p.pipeline_id);
    setEditForm(null);
    setYamlView(null);
    setTimeline([]);
    const [d, r] = await Promise.all([
      api("GET", `/api/pipelines/${p.pipeline_id}`),
      api("GET", `/api/pipelines/${p.pipeline_id}/runs?limit=5`),
    ]);
    setDetail(d);
    setRuns(r);
  }

  function startEditing() {
    if (!detail) return;
    const qc = detail.quality_config || {};
    setEditForm({
      schedule_cron: detail.schedule_cron || "",
      retry_max_attempts: detail.retry_max_attempts ?? 3,
      retry_backoff_seconds: detail.retry_backoff_seconds ?? 60,
      timeout_seconds: detail.timeout_seconds ?? 3600,
      refresh_type: detail.refresh_type || "full",
      replication_method: detail.replication_method || "watermark",
      incremental_column: detail.incremental_column || "",
      load_type: detail.load_type || "append",
      merge_keys: (detail.merge_keys || []).join(", "),
      last_watermark: detail.last_watermark || "",
      reset_watermark: false,
      count_tolerance: qc.count_tolerance ?? 0.001,
      volume_z_score_warn: qc.volume_z_score_warn ?? 2.0,
      volume_z_score_fail: qc.volume_z_score_fail ?? 3.0,
      null_rate_stddev_threshold: qc.null_rate_stddev_threshold ?? 2.0,
      freshness_warn_multiplier: qc.freshness_warn_multiplier ?? 2.0,
      freshness_fail_multiplier: qc.freshness_fail_multiplier ?? 5.0,
      promote_on_warn: qc.promote_on_warn ?? true,
      halt_on_first_fail: qc.halt_on_first_fail ?? true,
      tier: detail.tier || 2,
      owner: detail.owner || "",
      freshness_column: detail.freshness_column || "",
      tags_json: JSON.stringify(detail.tags || {}, null, 2),
      auto_approve_additive_schema: detail.auto_approve_additive_schema || false,
      on_new_column: (detail.schema_change_policy || {}).on_new_column || "auto_add",
      on_dropped_column: (detail.schema_change_policy || {}).on_dropped_column || "propose",
      on_type_change: (detail.schema_change_policy || {}).on_type_change || "propose",
      on_nullable_change: (detail.schema_change_policy || {}).on_nullable_change || "auto_accept",
      propagate_to_downstream: (detail.schema_change_policy || {}).propagate_to_downstream ?? false,
      hooks_json: JSON.stringify(detail.post_promotion_hooks || [], null, 2),
      reason: "",
    });
  }

  async function saveSettings() {
    if (!editForm || !detail) return;
    setSaving(true);
    try {
      const body = {};
      // Schedule
      if (editForm.schedule_cron !== (detail.schedule_cron || "")) body.schedule_cron = editForm.schedule_cron;
      if (editForm.retry_max_attempts !== (detail.retry_max_attempts ?? 3)) body.retry_max_attempts = parseInt(editForm.retry_max_attempts);
      if (editForm.retry_backoff_seconds !== (detail.retry_backoff_seconds ?? 60)) body.retry_backoff_seconds = parseInt(editForm.retry_backoff_seconds);
      if (editForm.timeout_seconds !== (detail.timeout_seconds ?? 3600)) body.timeout_seconds = parseInt(editForm.timeout_seconds);
      // Strategy
      if (editForm.refresh_type !== (detail.refresh_type || "full")) body.refresh_type = editForm.refresh_type;
      if (editForm.replication_method !== (detail.replication_method || "watermark")) body.replication_method = editForm.replication_method;
      if (editForm.incremental_column !== (detail.incremental_column || "")) body.incremental_column = editForm.incremental_column;
      if (editForm.load_type !== (detail.load_type || "append")) body.load_type = editForm.load_type;
      const newMergeKeys = editForm.merge_keys.split(",").map((s) => s.trim()).filter(Boolean);
      if (JSON.stringify(newMergeKeys) !== JSON.stringify(detail.merge_keys || [])) body.merge_keys = newMergeKeys;
      if (editForm.reset_watermark) body.reset_watermark = true;
      // Quality
      const qc = detail.quality_config || {};
      const qualityUpdates = {};
      if (parseFloat(editForm.count_tolerance) !== (qc.count_tolerance ?? 0.001)) qualityUpdates.count_tolerance = parseFloat(editForm.count_tolerance);
      if (parseFloat(editForm.volume_z_score_warn) !== (qc.volume_z_score_warn ?? 2.0)) qualityUpdates.volume_z_score_warn = parseFloat(editForm.volume_z_score_warn);
      if (parseFloat(editForm.volume_z_score_fail) !== (qc.volume_z_score_fail ?? 3.0)) qualityUpdates.volume_z_score_fail = parseFloat(editForm.volume_z_score_fail);
      if (parseFloat(editForm.null_rate_stddev_threshold) !== (qc.null_rate_stddev_threshold ?? 2.0)) qualityUpdates.null_rate_stddev_threshold = parseFloat(editForm.null_rate_stddev_threshold);
      if (parseFloat(editForm.freshness_warn_multiplier) !== (qc.freshness_warn_multiplier ?? 2.0)) qualityUpdates.freshness_warn_multiplier = parseFloat(editForm.freshness_warn_multiplier);
      if (parseFloat(editForm.freshness_fail_multiplier) !== (qc.freshness_fail_multiplier ?? 5.0)) qualityUpdates.freshness_fail_multiplier = parseFloat(editForm.freshness_fail_multiplier);
      if (editForm.promote_on_warn !== (qc.promote_on_warn ?? true)) qualityUpdates.promote_on_warn = editForm.promote_on_warn;
      if (editForm.halt_on_first_fail !== (qc.halt_on_first_fail ?? true)) qualityUpdates.halt_on_first_fail = editForm.halt_on_first_fail;
      if (Object.keys(qualityUpdates).length > 0) body.quality_config = qualityUpdates;
      // Observability
      if (parseInt(editForm.tier) !== (detail.tier || 2)) body.tier = parseInt(editForm.tier);
      if (editForm.owner !== (detail.owner || "")) body.owner = editForm.owner;
      if (editForm.freshness_column !== (detail.freshness_column || "")) body.freshness_column = editForm.freshness_column;
      try { const newTags = JSON.parse(editForm.tags_json); if (JSON.stringify(newTags) !== JSON.stringify(detail.tags || {})) body.tags = newTags; } catch {}
      if (editForm.auto_approve_additive_schema !== (detail.auto_approve_additive_schema || false)) body.auto_approve_additive_schema = editForm.auto_approve_additive_schema;
      // Schema change policy
      const scp = detail.schema_change_policy || {};
      const scpChanged = editForm.on_new_column !== (scp.on_new_column || "auto_add")
        || editForm.on_dropped_column !== (scp.on_dropped_column || "propose")
        || editForm.on_type_change !== (scp.on_type_change || "propose")
        || editForm.on_nullable_change !== (scp.on_nullable_change || "auto_accept")
        || editForm.propagate_to_downstream !== (scp.propagate_to_downstream ?? false);
      if (scpChanged) {
        body.schema_change_policy = {
          on_new_column: editForm.on_new_column,
          on_dropped_column: editForm.on_dropped_column,
          on_type_change: editForm.on_type_change,
          on_nullable_change: editForm.on_nullable_change,
          propagate_to_downstream: editForm.propagate_to_downstream,
        };
      }
      // Post-promotion hooks
      try {
        const newHooks = JSON.parse(editForm.hooks_json);
        if (JSON.stringify(newHooks) !== JSON.stringify(detail.post_promotion_hooks || [])) {
          body.post_promotion_hooks = newHooks;
        }
      } catch {}
      if (editForm.reason) body.reason = editForm.reason;

      if (Object.keys(body).length === 0 || (Object.keys(body).length === 1 && body.reason)) {
        window.alert("No changes detected.");
        setSaving(false);
        return;
      }

      const updated = await api("PATCH", `/api/pipelines/${detail.pipeline_id}`, body);
      setDetail(updated);
      setEditForm(null);
      setPipelines((ps) => ps.map((pp) => pp.pipeline_id === updated.pipeline_id ? { ...pp, ...updated } : pp));
    } catch (e) {
      window.alert("Save failed: " + (e.message || e));
    }
    setSaving(false);
  }

  async function loadYaml() {
    if (yamlView) { setYamlView(null); return; }
    if (!detail) return;
    try {
      const resp = await fetch(`/api/pipelines/${detail.pipeline_id}/export`, {
        headers: { Authorization: `Bearer ${localStorage.getItem("token")}` },
      });
      const text = await resp.text();
      setYamlView(text);
    } catch (e) { console.error(e); }
  }

  async function loadTimeline() {
    if (timeline.length > 0) { setTimeline([]); return; }
    if (!detail) return;
    try {
      const data = await api("GET", `/api/pipelines/${detail.pipeline_id}/timeline?limit=20`);
      setTimeline(Array.isArray(data) ? data : (data.events || []));
    } catch (e) { console.error(e); }
  }

  async function trigger(id) {
    await api("POST", `/api/pipelines/${id}/trigger`);
    window.alert("Run triggered!");
  }

  async function pause(id) {
    await api("POST", `/api/pipelines/${id}/pause`);
    setPipelines((ps) => ps.map((p) => (p.pipeline_id === id ? { ...p, status: "paused" } : p)));
  }

  async function resume(id) {
    await api("POST", `/api/pipelines/${id}/resume`);
    setPipelines((ps) => ps.map((p) => (p.pipeline_id === id ? { ...p, status: "active" } : p)));
  }

  const [depPicker, setDepPicker] = useState(null); // { pipelineId, search, selected }

  async function addDep(pipelineId) {
    setDepPicker({ pipelineId, search: "", selected: null });
  }

  async function confirmDep() {
    if (!depPicker || !depPicker.selected) return;
    try {
      await api("POST", `/api/pipelines/${depPicker.pipelineId}/dependencies`, { depends_on_id: depPicker.selected.pipeline_id });
      const d = await api("GET", `/api/pipelines/${depPicker.pipelineId}`);
      setDetail(d);
      setDepPicker(null);
    } catch (e) { window.alert("Failed: " + (e.message || e)); }
  }

  async function removeDep(pipelineId, depId) {
    if (!window.confirm("Remove this dependency?")) return;
    try {
      await api("DELETE", `/api/pipelines/${pipelineId}/dependencies/${depId}`);
      const d = await api("GET", `/api/pipelines/${pipelineId}`);
      setDetail(d);
    } catch (e) { window.alert("Failed: " + (e.message || e)); }
  }

  return (
    <div className="px-6 py-4">
      <h1 className="text-lg font-semibold mb-4 text-slate-800">Pipelines</h1>
      <div className="space-y-2">
        {filteredPipelines.map((p) => (
          <div key={p.pipeline_id} className="bg-white border border-slate-200 rounded-xl overflow-hidden">
            <div
              className="flex items-center gap-3 px-4 py-3 cursor-pointer hover:bg-slate-50 transition-colors"
              onClick={() => expand(p)}
            >
              <StatusDot status={p.status} />
              <TierBadge tier={p.tier} />
              <span className="font-medium text-sm flex-1 font-mono text-slate-700">{p.pipeline_name}</span>
              <span className="text-xs text-slate-400">
                {p.source} -&gt; {p.target}
              </span>
              <Pill label={p.refresh_type} color="blue" />
              <Pill label={p.load_type} color="purple" />
              <span className="text-xs text-slate-400 font-mono">{p.schedule_cron}</span>
              {p.owner && <span className="text-xs text-slate-400">{p.owner}</span>}
            </div>
            {expanded === p.pipeline_id && detail && (
              <div className="border-t border-slate-200 px-4 py-4 bg-slate-50/50 space-y-4">
                <div className="grid grid-cols-3 gap-4 text-sm">
                  <div>
                    <span className="text-slate-400">Incremental col</span>
                    <br />
                    <span className="font-mono text-slate-600">{detail.incremental_column || "--"}</span>
                  </div>
                  <div>
                    <span className="text-slate-400">Merge keys</span>
                    <br />
                    <span className="font-mono text-slate-600">{detail.merge_keys?.join(", ") || "--"}</span>
                  </div>
                  <div>
                    <span className="text-slate-400">Version</span>
                    <br />
                    <span className="font-mono text-slate-600">v{detail.version}</span>
                  </div>
                </div>

                {detail.error_budget && <ErrorBudgetCard eb={detail.error_budget} />}

                {detail.agent_reasoning?.refresh_type_reason && (
                  <div className="bg-green-50 border border-green-200 rounded-lg px-4 py-3">
                    <div className="text-xs font-semibold text-green-600 mb-1.5">Agent Reasoning</div>
                    {Object.entries(detail.agent_reasoning)
                      .filter(([k]) => k.endsWith("_reason"))
                      .map(([k, v]) => (
                        <div key={k} className="text-xs text-green-700 mb-1">
                          <span className="font-medium">{k.replace("_reason", "")}:</span> {v}
                        </div>
                      ))}
                  </div>
                )}

                {detail.recent_changes?.length > 0 && (
                  <div className="bg-amber-50 border border-amber-200 rounded-lg px-4 py-3">
                    <div className="text-xs font-semibold text-amber-700 mb-2">Changelog</div>
                    <div className="space-y-1">
                      {detail.recent_changes.map((c, i) => (
                        <div key={i} className="flex items-center gap-2 text-xs">
                          <span className="font-mono text-amber-500">{c.created_at?.slice(0, 16)}</span>
                          <Pill label={c.change_type} color={
                            c.change_type === "created" ? "green" :
                            c.change_type === "triggered" ? "blue" :
                            c.change_type === "paused" ? "amber" :
                            c.change_type === "resumed" ? "green" :
                            "purple"
                          } />
                          <span className="text-slate-600">{c.changed_by || "system"}</span>
                          <span className="text-slate-400">{c.source}</span>
                          {c.reason && <span className="text-slate-400 italic truncate max-w-xs">— {c.reason}</span>}
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                <div>
                  <div className="text-xs font-semibold text-slate-500 mb-2">Recent Runs</div>
                  <div className="space-y-1.5">
                    {runs.map((r) => <RunRow key={r.run_id} r={r} />)}
                    {runs.length === 0 && <div className="text-xs text-slate-300">No runs yet</div>}
                  </div>
                </div>

                {/* ---- Dependencies (Build 11) ---- */}
                {detail.dependencies && (
                  <div className="bg-slate-50 border border-slate-200 rounded-lg px-4 py-3 space-y-2">
                    <div className="text-xs font-semibold text-slate-500 mb-1">Dependencies</div>
                    <div className="grid grid-cols-2 gap-4">
                      <div>
                        <div className="text-[10px] text-slate-400 mb-1 uppercase tracking-wider">Upstream (depends on)</div>
                        {detail.dependencies.upstream?.length > 0 ? detail.dependencies.upstream.map((d) => (
                          <div key={d.dependency_id} className="flex items-center gap-2 text-xs border border-slate-200 rounded px-2 py-1 mb-1 bg-white">
                            <span className="font-mono text-slate-600">{d.depends_on_name || d.depends_on_id}</span>
                            <Pill label={d.dependency_type} color="blue" />
                            {d.notes && <span className="text-slate-400 italic text-[10px]">{d.notes}</span>}
                            <button onClick={() => removeDep(detail.pipeline_id, d.dependency_id)} className="text-red-400 hover:text-red-600 ml-auto text-[10px]">Remove</button>
                          </div>
                        )) : <div className="text-xs text-slate-300">No upstream dependencies</div>}
                        {depPicker && depPicker.pipelineId === detail.pipeline_id ? (
                          <div className="mt-1 border border-blue-300 rounded bg-white p-2 space-y-1">
                            <div className="flex items-center gap-2">
                              <input
                                autoFocus
                                className="flex-1 text-xs border border-slate-300 rounded px-2 py-1"
                                placeholder="Search pipelines..."
                                value={depPicker.search}
                                onChange={(e) => setDepPicker({ ...depPicker, search: e.target.value })}
                              />
                              <button onClick={() => setDepPicker(null)} className="text-[10px] text-slate-400 hover:text-slate-600">Cancel</button>
                            </div>
                            {depPicker.selected && (
                              <div className="flex items-center gap-2 bg-blue-50 border border-blue-200 rounded px-2 py-1.5">
                                <span className="text-xs font-medium text-blue-800">{depPicker.selected.pipeline_name}</span>
                                <span className="text-[10px] text-blue-500 font-mono">{depPicker.selected.pipeline_id.slice(0, 8)}</span>
                                <button onClick={() => setDepPicker({ ...depPicker, selected: null })} className="text-[10px] text-blue-400 hover:text-blue-600 ml-auto">Change</button>
                                <button onClick={confirmDep} className="text-[10px] px-2 py-0.5 rounded bg-blue-600 text-white hover:bg-blue-700">Save Dependency</button>
                              </div>
                            )}
                            {!depPicker.selected && (
                              <div className="max-h-40 overflow-y-auto space-y-0.5">
                                {pipelines
                                  .filter((p) => {
                                    if (p.pipeline_id === detail.pipeline_id) return false;
                                    const existing = (detail.dependencies.upstream || []).map((d) => d.depends_on_id);
                                    if (existing.includes(p.pipeline_id)) return false;
                                    if (!depPicker.search) return true;
                                    const q = depPicker.search.toLowerCase();
                                    return (p.pipeline_name || "").toLowerCase().includes(q)
                                      || (p.source_table || "").toLowerCase().includes(q)
                                      || (p.pipeline_id || "").toLowerCase().includes(q);
                                  })
                                  .map((p) => (
                                    <button
                                      key={p.pipeline_id}
                                      onClick={() => setDepPicker({ ...depPicker, selected: p })}
                                      className="w-full text-left text-xs px-2 py-1.5 rounded hover:bg-blue-50 flex items-center gap-2 border border-transparent hover:border-blue-200"
                                    >
                                      <span className="font-medium text-slate-700">{p.pipeline_name}</span>
                                      <span className="text-[10px] text-slate-400 font-mono">{p.pipeline_id.slice(0, 8)}</span>
                                      {p.status && <Pill label={p.status} color={p.status === "active" ? "green" : "slate"} />}
                                    </button>
                                  ))
                                }
                                {pipelines.filter((p) => {
                                  if (p.pipeline_id === detail.pipeline_id) return false;
                                  const existing = (detail.dependencies.upstream || []).map((d) => d.depends_on_id);
                                  if (existing.includes(p.pipeline_id)) return false;
                                  if (!depPicker.search) return true;
                                  const q = depPicker.search.toLowerCase();
                                  return (p.pipeline_name || "").toLowerCase().includes(q) || (p.pipeline_id || "").toLowerCase().includes(q);
                                }).length === 0 && (
                                  <div className="text-[10px] text-slate-400 px-2 py-1">No matching pipelines</div>
                                )}
                              </div>
                            )}
                          </div>
                        ) : (
                          <button onClick={() => addDep(detail.pipeline_id)} className="text-[10px] text-blue-500 hover:text-blue-700 mt-1">+ Add dependency</button>
                        )}
                      </div>
                      <div>
                        <div className="text-[10px] text-slate-400 mb-1 uppercase tracking-wider">Downstream</div>
                        <div className="text-xs text-slate-500">{detail.dependencies.downstream_count || 0} pipeline(s) depend on this</div>
                      </div>
                    </div>
                  </div>
                )}

                {/* ---- Metadata (Build 11) ---- */}
                {detail.metadata?.length > 0 && (
                  <div className="bg-slate-50 border border-slate-200 rounded-lg px-4 py-3">
                    <div className="text-xs font-semibold text-slate-500 mb-2">Pipeline Metadata</div>
                    <div className="grid grid-cols-3 gap-2">
                      {detail.metadata.map((m) => (
                        <div key={m.namespace + "/" + m.key} className="border border-slate-200 rounded px-2 py-1 bg-white">
                          <div className="text-[10px] text-slate-400">{m.namespace}/{m.key}</div>
                          <div className="text-xs font-mono text-slate-600 truncate">{JSON.stringify(m.value?.value ?? m.value)}</div>
                          <div className="text-[10px] text-slate-300">{(m.updated_at || "").slice(0, 16)}</div>
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                {/* ---- Step DAG / Transforms (Build 29) ---- */}
                {!editForm && (detail.steps || []).length > 0 && (
                  <StepDAGSection steps={detail.steps} pipelineId={detail.pipeline_id} />
                )}

                {/* ---- Connector Code (Build 29b) ---- */}
                {!editForm && (detail.source_connector_id || detail.target_connector_id) && (
                  <ConnectorCodeSection
                    sourceConnectorId={detail.source_connector_id}
                    targetConnectorId={detail.target_connector_id}
                  />
                )}

                {/* ---- Post-Promotion Hooks (Build 13) ---- */}
                {!editForm && (detail.post_promotion_hooks || []).length > 0 && (
                  <div className="bg-slate-50 border border-slate-200 rounded-lg px-4 py-3">
                    <div className="text-xs font-semibold text-slate-500 mb-2">Post-Promotion Hooks</div>
                    <div className="space-y-2">
                      {detail.post_promotion_hooks.map((h) => {
                        const result = (detail.hook_results || {})[h.metadata_key || h.name];
                        return (
                          <div key={h.hook_id} className="flex items-start gap-2 text-xs">
                            <Pill label={h.enabled ? "on" : "off"} color={h.enabled ? "green" : "slate"} />
                            <div className="flex-1 min-w-0">
                              <div className="font-semibold text-slate-700">{h.name || "unnamed"}</div>
                              <div className="font-mono text-[10px] text-slate-400 truncate" title={h.sql}>{h.sql}</div>
                              {h.description && <div className="text-[10px] text-slate-400">{h.description}</div>}
                              {result && (
                                <div className={`text-[10px] mt-0.5 ${result.status === "success" ? "text-green-600" : "text-red-500"}`}>
                                  Last: {result.status} ({result.duration_ms}ms)
                                  {result.result && Object.keys(result.result).length > 0 && (
                                    <span className="ml-1 font-mono">{JSON.stringify(result.result)}</span>
                                  )}
                                  {result.error && <span className="ml-1">{result.error}</span>}
                                </div>
                              )}
                            </div>
                            {h.fail_pipeline_on_error && <Pill label="fail on err" color="red" />}
                          </div>
                        );
                      })}
                    </div>
                  </div>
                )}

                {/* ---- Schema Policy Summary (Build 12) ---- */}
                {detail.schema_change_policy && !editForm && (
                  <div className="bg-slate-50 border border-slate-200 rounded-lg px-4 py-2">
                    <div className="flex items-center gap-2">
                      <span className="text-xs font-semibold text-slate-500">Schema Policy</span>
                      {detail.schema_change_policy_is_custom && <Pill label="custom" color="purple" />}
                    </div>
                    <div className="text-xs text-slate-500 mt-1">
                      new: <span className="font-mono">{detail.schema_change_policy.on_new_column}</span>,{" "}
                      drop: <span className="font-mono">{detail.schema_change_policy.on_dropped_column}</span>,{" "}
                      type: <span className="font-mono">{detail.schema_change_policy.on_type_change}</span>,{" "}
                      nullable: <span className="font-mono">{detail.schema_change_policy.on_nullable_change}</span>
                      {detail.schema_change_policy.propagate_to_downstream && <span className="ml-2 text-[10px] text-green-600">(propagates)</span>}
                    </div>
                  </div>
                )}

                {/* ---- Edit Settings Panel ---- */}
                {editForm && (
                  <div className="border-2 border-blue-300 rounded-xl p-4 bg-blue-50/30 space-y-4">
                    <div className="text-xs font-semibold text-blue-600 mb-2">Edit Settings</div>

                    {/* Schedule */}
                    <div>
                      <div className="text-xs font-semibold text-slate-500 mb-1.5">Schedule</div>
                      <div className="grid grid-cols-4 gap-2">
                        <label className="text-xs text-slate-500">
                          Cron
                          <input className="block w-full mt-0.5 px-2 py-1 border border-slate-300 rounded text-xs font-mono bg-white" value={editForm.schedule_cron} onChange={(e) => setEditForm({...editForm, schedule_cron: e.target.value})} />
                        </label>
                        <label className="text-xs text-slate-500">
                          Retry attempts
                          <input type="number" className="block w-full mt-0.5 px-2 py-1 border border-slate-300 rounded text-xs bg-white" value={editForm.retry_max_attempts} onChange={(e) => setEditForm({...editForm, retry_max_attempts: parseInt(e.target.value) || 0})} />
                        </label>
                        <label className="text-xs text-slate-500">
                          Backoff (s)
                          <input type="number" className="block w-full mt-0.5 px-2 py-1 border border-slate-300 rounded text-xs bg-white" value={editForm.retry_backoff_seconds} onChange={(e) => setEditForm({...editForm, retry_backoff_seconds: parseInt(e.target.value) || 0})} />
                        </label>
                        <label className="text-xs text-slate-500">
                          Timeout (s)
                          <input type="number" className="block w-full mt-0.5 px-2 py-1 border border-slate-300 rounded text-xs bg-white" value={editForm.timeout_seconds} onChange={(e) => setEditForm({...editForm, timeout_seconds: parseInt(e.target.value) || 0})} />
                        </label>
                      </div>
                    </div>

                    {/* Strategy */}
                    <div>
                      <div className="text-xs font-semibold text-slate-500 mb-1.5">Strategy</div>
                      <div className="grid grid-cols-3 gap-2">
                        <label className="text-xs text-slate-500">
                          Refresh type
                          <select className="block w-full mt-0.5 px-2 py-1 border border-slate-300 rounded text-xs bg-white" value={editForm.refresh_type} onChange={(e) => setEditForm({...editForm, refresh_type: e.target.value})}>
                            <option value="full">full</option>
                            <option value="incremental">incremental</option>
                          </select>
                        </label>
                        <label className="text-xs text-slate-500">
                          Load type
                          <select className="block w-full mt-0.5 px-2 py-1 border border-slate-300 rounded text-xs bg-white" value={editForm.load_type} onChange={(e) => setEditForm({...editForm, load_type: e.target.value})}>
                            <option value="append">append</option>
                            <option value="merge">merge</option>
                          </select>
                        </label>
                        <label className="text-xs text-slate-500">
                          Replication
                          <select className="block w-full mt-0.5 px-2 py-1 border border-slate-300 rounded text-xs bg-white" value={editForm.replication_method} onChange={(e) => setEditForm({...editForm, replication_method: e.target.value})}>
                            <option value="watermark">watermark</option>
                            <option value="cdc">cdc</option>
                            <option value="snapshot">snapshot</option>
                          </select>
                        </label>
                      </div>
                      <div className="grid grid-cols-3 gap-2 mt-2">
                        <label className="text-xs text-slate-500">
                          Incremental column
                          <input className="block w-full mt-0.5 px-2 py-1 border border-slate-300 rounded text-xs font-mono bg-white" value={editForm.incremental_column} onChange={(e) => setEditForm({...editForm, incremental_column: e.target.value})} />
                        </label>
                        <label className="text-xs text-slate-500">
                          Merge keys (comma-sep)
                          <input className="block w-full mt-0.5 px-2 py-1 border border-slate-300 rounded text-xs font-mono bg-white" value={editForm.merge_keys} onChange={(e) => setEditForm({...editForm, merge_keys: e.target.value})} />
                        </label>
                        <div className="flex items-end gap-2">
                          <label className="text-xs text-slate-500 flex-1">
                            Watermark
                            <input className="block w-full mt-0.5 px-2 py-1 border border-slate-300 rounded text-xs font-mono bg-white" value={editForm.last_watermark} disabled={editForm.reset_watermark} onChange={(e) => setEditForm({...editForm, last_watermark: e.target.value})} />
                          </label>
                          <button
                            onClick={() => setEditForm({...editForm, reset_watermark: !editForm.reset_watermark})}
                            className={`text-xs px-2 py-1 rounded border mb-0.5 ${editForm.reset_watermark ? "bg-red-100 border-red-300 text-red-600" : "border-slate-300 text-slate-500 hover:bg-slate-100"}`}
                          >
                            {editForm.reset_watermark ? "Will Reset" : "Reset"}
                          </button>
                        </div>
                      </div>
                    </div>

                    {/* Quality */}
                    <div>
                      <div className="text-xs font-semibold text-slate-500 mb-1.5">Quality Thresholds</div>
                      <div className="grid grid-cols-3 gap-2">
                        <label className="text-xs text-slate-500">
                          Count tolerance
                          <input type="number" step="0.001" className="block w-full mt-0.5 px-2 py-1 border border-slate-300 rounded text-xs bg-white" value={editForm.count_tolerance} onChange={(e) => setEditForm({...editForm, count_tolerance: e.target.value})} />
                        </label>
                        <label className="text-xs text-slate-500">
                          Volume Z warn
                          <input type="number" step="0.1" className="block w-full mt-0.5 px-2 py-1 border border-slate-300 rounded text-xs bg-white" value={editForm.volume_z_score_warn} onChange={(e) => setEditForm({...editForm, volume_z_score_warn: e.target.value})} />
                        </label>
                        <label className="text-xs text-slate-500">
                          Volume Z fail
                          <input type="number" step="0.1" className="block w-full mt-0.5 px-2 py-1 border border-slate-300 rounded text-xs bg-white" value={editForm.volume_z_score_fail} onChange={(e) => setEditForm({...editForm, volume_z_score_fail: e.target.value})} />
                        </label>
                        <label className="text-xs text-slate-500">
                          Null rate stddev
                          <input type="number" step="0.1" className="block w-full mt-0.5 px-2 py-1 border border-slate-300 rounded text-xs bg-white" value={editForm.null_rate_stddev_threshold} onChange={(e) => setEditForm({...editForm, null_rate_stddev_threshold: e.target.value})} />
                        </label>
                        <label className="text-xs text-slate-500">
                          Freshness warn ×
                          <input type="number" step="0.1" className="block w-full mt-0.5 px-2 py-1 border border-slate-300 rounded text-xs bg-white" value={editForm.freshness_warn_multiplier} onChange={(e) => setEditForm({...editForm, freshness_warn_multiplier: e.target.value})} />
                        </label>
                        <label className="text-xs text-slate-500">
                          Freshness fail ×
                          <input type="number" step="0.1" className="block w-full mt-0.5 px-2 py-1 border border-slate-300 rounded text-xs bg-white" value={editForm.freshness_fail_multiplier} onChange={(e) => setEditForm({...editForm, freshness_fail_multiplier: e.target.value})} />
                        </label>
                      </div>
                      <div className="flex gap-4 mt-2">
                        <label className="text-xs text-slate-500 flex items-center gap-1.5">
                          <input type="checkbox" checked={editForm.promote_on_warn} onChange={(e) => setEditForm({...editForm, promote_on_warn: e.target.checked})} />
                          Promote on warn
                        </label>
                        <label className="text-xs text-slate-500 flex items-center gap-1.5">
                          <input type="checkbox" checked={editForm.halt_on_first_fail} onChange={(e) => setEditForm({...editForm, halt_on_first_fail: e.target.checked})} />
                          Halt on first fail
                        </label>
                      </div>
                    </div>

                    {/* Observability */}
                    <div>
                      <div className="text-xs font-semibold text-slate-500 mb-1.5">Observability</div>
                      <div className="grid grid-cols-4 gap-2">
                        <label className="text-xs text-slate-500">
                          Tier
                          <select className="block w-full mt-0.5 px-2 py-1 border border-slate-300 rounded text-xs bg-white" value={editForm.tier} onChange={(e) => setEditForm({...editForm, tier: parseInt(e.target.value)})}>
                            <option value={1}>T1 - Critical</option>
                            <option value={2}>T2 - Standard</option>
                            <option value={3}>T3 - Best-effort</option>
                          </select>
                        </label>
                        <label className="text-xs text-slate-500">
                          Owner
                          <input className="block w-full mt-0.5 px-2 py-1 border border-slate-300 rounded text-xs bg-white" value={editForm.owner} onChange={(e) => setEditForm({...editForm, owner: e.target.value})} />
                        </label>
                        <label className="text-xs text-slate-500">
                          Freshness column
                          <input className="block w-full mt-0.5 px-2 py-1 border border-slate-300 rounded text-xs font-mono bg-white" value={editForm.freshness_column} onChange={(e) => setEditForm({...editForm, freshness_column: e.target.value})} />
                        </label>
                        <label className="text-xs text-slate-500 flex items-end gap-1.5 pb-0.5">
                          <input type="checkbox" checked={editForm.auto_approve_additive_schema} onChange={(e) => setEditForm({...editForm, auto_approve_additive_schema: e.target.checked})} />
                          Auto-approve additive
                        </label>
                      </div>
                      <label className="text-xs text-slate-500 block mt-2">
                        Tags (JSON)
                        <textarea className="block w-full mt-0.5 px-2 py-1 border border-slate-300 rounded text-xs font-mono bg-white" rows={2} value={editForm.tags_json} onChange={(e) => setEditForm({...editForm, tags_json: e.target.value})} />
                      </label>
                    </div>

                    {/* Schema Change Policy (Build 12) */}
                    <div>
                      <div className="text-xs font-semibold text-slate-500 mb-1.5">Schema Change Policy</div>
                      <div className="grid grid-cols-4 gap-2">
                        <label className="text-xs text-slate-500">
                          New columns
                          <select className="block w-full mt-0.5 px-2 py-1 border border-slate-300 rounded text-xs bg-white" value={editForm.on_new_column} onChange={(e) => setEditForm({...editForm, on_new_column: e.target.value})}>
                            <option value="auto_add">Auto-add</option>
                            <option value="propose">Propose</option>
                            <option value="ignore">Ignore</option>
                          </select>
                        </label>
                        <label className="text-xs text-slate-500">
                          Dropped columns
                          <select className="block w-full mt-0.5 px-2 py-1 border border-slate-300 rounded text-xs bg-white" value={editForm.on_dropped_column} onChange={(e) => setEditForm({...editForm, on_dropped_column: e.target.value})}>
                            <option value="halt">Halt</option>
                            <option value="propose">Propose</option>
                            <option value="ignore">Ignore</option>
                          </select>
                        </label>
                        <label className="text-xs text-slate-500">
                          Type changes
                          <select className="block w-full mt-0.5 px-2 py-1 border border-slate-300 rounded text-xs bg-white" value={editForm.on_type_change} onChange={(e) => setEditForm({...editForm, on_type_change: e.target.value})}>
                            <option value="auto_widen">Auto-widen (safe)</option>
                            <option value="propose">Propose</option>
                            <option value="halt">Halt</option>
                          </select>
                        </label>
                        <label className="text-xs text-slate-500">
                          Nullable changes
                          <select className="block w-full mt-0.5 px-2 py-1 border border-slate-300 rounded text-xs bg-white" value={editForm.on_nullable_change} onChange={(e) => setEditForm({...editForm, on_nullable_change: e.target.value})}>
                            <option value="auto_accept">Auto-accept</option>
                            <option value="propose">Propose</option>
                            <option value="halt">Halt</option>
                          </select>
                        </label>
                      </div>
                      <label className="text-xs text-slate-500 flex items-center gap-1.5 mt-2">
                        <input type="checkbox" checked={editForm.propagate_to_downstream} onChange={(e) => setEditForm({...editForm, propagate_to_downstream: e.target.checked})} />
                        Propagate changes to downstream pipelines
                      </label>
                      <div className="text-[10px] text-slate-400 mt-1">Defaults based on tier. Override per-pipeline here.</div>
                    </div>

                    {/* Post-Promotion Hooks (Build 13) */}
                    <div>
                      <div className="text-xs font-semibold text-slate-500 mb-1.5">Post-Promotion SQL Hooks</div>
                      <textarea
                        className="block w-full px-2 py-1.5 border border-slate-300 rounded text-xs font-mono bg-white"
                        rows={6}
                        value={editForm.hooks_json}
                        onChange={(e) => setEditForm({...editForm, hooks_json: e.target.value})}
                        placeholder={'[\n  {"name": "row_count", "sql": "SELECT COUNT(*) as cnt FROM ...", "metadata_key": "total_rows"}\n]'}
                      />
                      <div className="text-[10px] text-slate-400 mt-1">
                        JSON array. Each hook: name, sql, metadata_key, description, enabled (true), timeout_seconds (30), fail_pipeline_on_error (false)
                      </div>
                      <div className="text-[10px] text-slate-400 mt-0.5">
                        Template variables: <code className="bg-slate-100 px-0.5 rounded">{"{{watermark_after}}"}</code> <code className="bg-slate-100 px-0.5 rounded">{"{{watermark_before}}"}</code> <code className="bg-slate-100 px-0.5 rounded">{"{{run_id}}"}</code> <code className="bg-slate-100 px-0.5 rounded">{"{{batch_id}}"}</code> <code className="bg-slate-100 px-0.5 rounded">{"{{target_schema}}"}</code> <code className="bg-slate-100 px-0.5 rounded">{"{{target_table}}"}</code> <code className="bg-slate-100 px-0.5 rounded">{"{{rows_extracted}}"}</code> <code className="bg-slate-100 px-0.5 rounded">{"{{pipeline_name}}"}</code> | Upstream: <code className="bg-slate-100 px-0.5 rounded">{"{{upstream_watermark_after}}"}</code> <code className="bg-slate-100 px-0.5 rounded">{"{{upstream_run_id}}"}</code> <code className="bg-slate-100 px-0.5 rounded">{"{{upstream_rows_extracted}}"}</code> <code className="bg-slate-100 px-0.5 rounded">{"{{upstream_pipeline_id}}"}</code>
                      </div>
                    </div>

                    {/* Footer: reason + save */}
                    <div className="flex items-end gap-2 pt-2 border-t border-blue-200">
                      <label className="text-xs text-slate-500 flex-1">
                        Change reason
                        <input className="block w-full mt-0.5 px-2 py-1 border border-slate-300 rounded text-xs bg-white" placeholder="Why are you making this change?" value={editForm.reason} onChange={(e) => setEditForm({...editForm, reason: e.target.value})} />
                      </label>
                      <button
                        onClick={saveSettings}
                        disabled={saving}
                        className="text-xs px-4 py-1.5 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50"
                      >
                        {saving ? "Saving..." : "Save Changes"}
                      </button>
                      <button
                        onClick={() => setEditForm(null)}
                        className="text-xs px-3 py-1.5 border border-slate-300 text-slate-500 rounded-lg hover:bg-slate-100"
                      >
                        Cancel
                      </button>
                    </div>
                  </div>
                )}

                <div className="flex gap-2">
                  <button
                    onClick={() => trigger(p.pipeline_id)}
                    className="text-xs px-3 py-1.5 bg-blue-600 text-white rounded-lg hover:bg-blue-700"
                  >
                    Trigger Run
                  </button>
                  {!editForm && (
                    <button
                      onClick={startEditing}
                      className="text-xs px-3 py-1.5 border border-blue-300 text-blue-600 rounded-lg hover:bg-blue-50"
                    >
                      Edit Settings
                    </button>
                  )}
                  <button
                    onClick={loadYaml}
                    className={`text-xs px-3 py-1.5 border rounded-lg ${yamlView ? "border-amber-300 text-amber-600 bg-amber-50" : "border-slate-300 text-slate-500 hover:bg-slate-100"}`}
                  >
                    {yamlView ? "Hide YAML" : "View YAML"}
                  </button>
                  <button
                    onClick={loadTimeline}
                    className={`text-xs px-3 py-1.5 border rounded-lg ${timeline.length > 0 ? "border-purple-300 text-purple-600 bg-purple-50" : "border-slate-300 text-slate-500 hover:bg-slate-100"}`}
                  >
                    {timeline.length > 0 ? "Hide Timeline" : "Timeline"}
                  </button>
                  {p.status === "active" ? (
                    <button
                      onClick={() => pause(p.pipeline_id)}
                      className="text-xs px-3 py-1.5 border border-slate-300 text-slate-500 rounded-lg hover:bg-slate-100"
                    >
                      Pause
                    </button>
                  ) : (
                    <button
                      onClick={() => resume(p.pipeline_id)}
                      className="text-xs px-3 py-1.5 border border-green-300 text-green-600 rounded-lg hover:bg-green-50"
                    >
                      Resume
                    </button>
                  )}
                </div>

                {/* YAML View */}
                {yamlView && (
                  <div className="bg-slate-900 rounded-xl p-4 overflow-auto max-h-96">
                    <pre className="text-xs text-green-400 font-mono whitespace-pre">{yamlView}</pre>
                  </div>
                )}

                {/* Timeline */}
                {timeline.length > 0 && (
                  <div>
                    <div className="text-xs font-semibold text-slate-500 mb-2">Change Timeline</div>
                    <div className="space-y-1.5">
                      {timeline.filter((e) => e.type === "decision").map((e, i) => (
                        <div key={i} className="border border-slate-200 rounded-lg px-3 py-2 bg-white">
                          <div className="flex items-center gap-2 text-xs">
                            <span className="px-1.5 py-0.5 bg-purple-100 text-purple-700 rounded text-[10px] font-medium">{e.decision_type || "decision"}</span>
                            <span className="text-slate-400 font-mono">{e.timestamp?.slice(0, 16)}</span>
                          </div>
                          {e.detail && (
                            <div className="text-xs text-slate-500 mt-1 font-mono truncate">{typeof e.detail === "string" ? e.detail : JSON.stringify(e.detail)}</div>
                          )}
                          {e.reasoning && (
                            <div className="text-xs text-slate-400 mt-0.5 italic">{e.reasoning}</div>
                          )}
                        </div>
                      ))}
                      {timeline.filter((e) => e.type === "decision").length === 0 && (
                        <div className="text-xs text-slate-300">No change events recorded yet</div>
                      )}
                    </div>
                  </div>
                )}
              </div>
            )}
          </div>
        ))}
        {pipelines.length === 0 && (
          <div className="text-sm text-slate-400 py-8 text-center">
            No pipelines yet. Use the Command view to create your first pipeline.
          </div>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// 3. Activity View
// ---------------------------------------------------------------------------

// Maps execution log step names to which connector role they relate to
const STEP_CONNECTOR_MAP = {
  connectors: "both",
  extract: "source",
  load_staging: "target",
  quality_gate: "target",
  promote: "target",
  cleanup: "target",
  hook: "target",
};

function ExecutionLogWithCode({ log, sourceConnectorId, targetConnectorId }) {
  const [expandedStep, setExpandedStep] = useState(null); // index of expanded step
  const [connectorCache, setConnectorCache] = useState({}); // { connectorId: data }
  const [loading, setLoading] = useState(false);

  const hasConnectors = sourceConnectorId || targetConnectorId;

  async function toggleStep(idx, stepName) {
    if (expandedStep === idx) { setExpandedStep(null); return; }
    const role = STEP_CONNECTOR_MAP[stepName];
    if (!role || !hasConnectors) return;

    setExpandedStep(idx);
    const idsToFetch = [];
    if ((role === "source" || role === "both") && sourceConnectorId && !connectorCache[sourceConnectorId]) {
      idsToFetch.push(sourceConnectorId);
    }
    if ((role === "target" || role === "both") && targetConnectorId && !connectorCache[targetConnectorId]) {
      idsToFetch.push(targetConnectorId);
    }
    if (idsToFetch.length > 0) {
      setLoading(true);
      const results = await Promise.all(
        idsToFetch.map((id) => api("GET", `/api/connectors/${id}`).catch(() => null))
      );
      const updated = { ...connectorCache };
      idsToFetch.forEach((id, i) => { if (results[i]) updated[id] = results[i]; });
      setConnectorCache(updated);
      setLoading(false);
    }
  }

  // Find the relevant method in connector code for a given step
  function findMethod(code, stepName) {
    if (!code) return null;
    const methodMap = {
      extract: "async def extract",
      load_staging: "async def load_staging",
      promote: "async def promote",
      quality_gate: "async def count_rows",
      cleanup: "async def cleanup",
      connectors: "async def test_connection",
    };
    const pattern = methodMap[stepName];
    if (!pattern) return null;
    const lines = code.split("\n");
    const startIdx = lines.findIndex((l) => l.includes(pattern));
    if (startIdx === -1) return null;
    // Find the next method or end of class
    const indent = lines[startIdx].search(/\S/);
    let endIdx = lines.length;
    for (let i = startIdx + 1; i < lines.length; i++) {
      const line = lines[i];
      if (line.trim() === "") continue;
      const lineIndent = line.search(/\S/);
      if (lineIndent <= indent && (line.trim().startsWith("async def ") || line.trim().startsWith("def ") || line.trim().startsWith("class "))) {
        endIdx = i;
        break;
      }
    }
    return lines.slice(startIdx, endIdx).join("\n");
  }

  return (
    <div className="bg-white border border-slate-200 rounded-lg px-3 py-2">
      <div className="text-[10px] uppercase text-slate-400 font-semibold mb-2">Execution Log</div>
      <div className="relative pl-4 border-l-2 border-slate-200 space-y-1.5">
        {log.map((entry, i) => {
          const stepColor = entry.status === "error" ? "bg-red-400" : entry.status === "warn" ? "bg-amber-400" : "bg-green-400";
          const role = STEP_CONNECTOR_MAP[entry.step];
          const isClickable = hasConnectors && role;
          const isExpanded = expandedStep === i;
          return (
            <div key={i}>
              <div
                className={"relative flex items-start gap-2 text-xs" + (isClickable ? " cursor-pointer hover:bg-slate-50 -mx-1 px-1 rounded" : "")}
                onClick={() => isClickable && toggleStep(i, entry.step)}
              >
                <div className={`absolute -left-[21px] top-1 w-2.5 h-2.5 rounded-full ${stepColor} ring-2 ring-white`} />
                <div className="flex-1 flex items-baseline gap-2 min-w-0">
                  <span className={"font-mono font-medium whitespace-nowrap " + (isClickable ? "text-blue-700 underline decoration-dotted" : "text-slate-700")}>{entry.step}</span>
                  {entry.detail && <span className="text-slate-400 truncate">{entry.detail}</span>}
                </div>
                <span className="text-slate-300 font-mono whitespace-nowrap text-[10px]">{entry.elapsed_ms}ms</span>
                {isClickable && <span className="text-[10px] text-slate-300">{isExpanded ? "\u25B2" : "\u25BC"}</span>}
              </div>
              {isExpanded && (
                <div className="ml-2 mt-1 mb-2 space-y-2">
                  {loading && <div className="text-[10px] text-slate-400">Loading connector code...</div>}
                  {(role === "source" || role === "both") && sourceConnectorId && connectorCache[sourceConnectorId] && (() => {
                    const c = connectorCache[sourceConnectorId];
                    const method = findMethod(c.code, entry.step);
                    return (
                      <div className="border border-blue-200 rounded bg-blue-50/50">
                        <div className="flex items-center gap-2 px-2 py-1.5">
                          <span className="text-[10px] font-bold px-1.5 py-0.5 rounded bg-blue-100 text-blue-700">source</span>
                          <span className="text-[10px] font-medium text-slate-600">{c.name}</span>
                          <span className="text-[10px] text-slate-400">{c.db_type}</span>
                        </div>
                        <pre className="text-[11px] bg-slate-900 text-green-300 rounded-b p-2 overflow-x-auto whitespace-pre-wrap max-h-60 overflow-y-auto">{method || c.code}</pre>
                      </div>
                    );
                  })()}
                  {(role === "target" || role === "both") && targetConnectorId && connectorCache[targetConnectorId] && (() => {
                    const c = connectorCache[targetConnectorId];
                    const method = findMethod(c.code, entry.step);
                    return (
                      <div className="border border-green-200 rounded bg-green-50/50">
                        <div className="flex items-center gap-2 px-2 py-1.5">
                          <span className="text-[10px] font-bold px-1.5 py-0.5 rounded bg-green-100 text-green-700">target</span>
                          <span className="text-[10px] font-medium text-slate-600">{c.name}</span>
                          <span className="text-[10px] text-slate-400">{c.db_type}</span>
                        </div>
                        <pre className="text-[11px] bg-slate-900 text-green-300 rounded-b p-2 overflow-x-auto whitespace-pre-wrap max-h-60 overflow-y-auto">{method || c.code}</pre>
                      </div>
                    );
                  })()}
                </div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}

function ActivityRunDetail({ r }) {
  const [expanded, setExpanded] = useState(false);
  const duration = r.started_at && r.completed_at
    ? Math.round((new Date(r.completed_at) - new Date(r.started_at)) / 1000)
    : null;
  const fmtDur = duration != null
    ? duration >= 60 ? `${Math.floor(duration / 60)}m ${duration % 60}s` : `${duration}s`
    : "--";
  const fmtBytes = (b) => {
    if (!b) return null;
    if (b > 1048576) return `${(b / 1048576).toFixed(1)} MB`;
    if (b > 1024) return `${(b / 1024).toFixed(1)} KB`;
    return `${b} B`;
  };
  const checks = r.quality_results && Array.isArray(r.quality_results.checks) ? r.quality_results.checks : [];

  return (
    <div className="border-b border-slate-200">
      <div
        className="flex items-center gap-3 px-4 py-2.5 hover:bg-slate-50 text-sm cursor-pointer"
        onClick={() => setExpanded(!expanded)}
      >
        <StatusDot status={r.status} />
        <TierBadge tier={r.tier} />
        <span className="text-slate-400 font-mono text-xs w-32">{r.started_at?.slice(0, 16)}</span>
        <span className="font-mono font-medium flex-1 text-slate-700">{r.pipeline_name}</span>
        <span className="text-slate-400 text-xs">{r.rows_extracted?.toLocaleString()} rows</span>
        {r.gate_decision && (
          <Pill
            label={r.gate_decision}
            color={r.gate_decision === "halt" ? "red" : r.gate_decision === "promote_with_warning" ? "amber" : "green"}
          />
        )}
        {r.error && <span className="text-xs text-red-600 truncate max-w-[200px]">{r.error}</span>}
        <span className="text-slate-300 text-xs">{expanded ? "\u25B2" : "\u25BC"}</span>
      </div>
      {expanded && (
        <div className="px-4 pb-4 pt-1 bg-slate-50/50 space-y-3">
          {/* Run metadata grid */}
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 text-xs">
            <div>
              <span className="text-slate-400 block">Duration</span>
              <span className="font-mono text-slate-700">{fmtDur}</span>
            </div>
            <div>
              <span className="text-slate-400 block">Mode</span>
              <span className="font-mono text-slate-700">{r.run_mode || "scheduled"}</span>
            </div>
            <div>
              <span className="text-slate-400 block">Extracted</span>
              <span className="font-mono text-slate-700">{r.rows_extracted?.toLocaleString() ?? "--"}</span>
            </div>
            <div>
              <span className="text-slate-400 block">Loaded</span>
              <span className="font-mono text-slate-700">{r.rows_loaded?.toLocaleString() ?? "--"}</span>
            </div>
            <div>
              <span className="text-slate-400 block">Staging Size</span>
              <span className="font-mono text-slate-700">{fmtBytes(r.staging_size_bytes) || "--"}</span>
            </div>
            <div>
              <span className="text-slate-400 block">Retries</span>
              <span className="font-mono text-slate-700">{r.retry_count ?? 0}</span>
            </div>
            <div>
              <span className="text-slate-400 block">Started</span>
              <span className="font-mono text-slate-700">{r.started_at?.replace("T", " ").slice(0, 19) || "--"}</span>
            </div>
            <div>
              <span className="text-slate-400 block">Completed</span>
              <span className="font-mono text-slate-700">{r.completed_at?.replace("T", " ").slice(0, 19) || "--"}</span>
            </div>
          </div>

          {/* Watermarks */}
          {(r.watermark_before || r.watermark_after) && (
            <div className="bg-white border border-slate-200 rounded-lg px-3 py-2">
              <div className="text-[10px] uppercase text-slate-400 font-semibold mb-1">Watermark</div>
              <div className="flex items-center gap-2 text-xs font-mono text-slate-600">
                <span className="bg-slate-100 rounded px-2 py-0.5">{r.watermark_before || "null"}</span>
                <span className="text-slate-400">&rarr;</span>
                <span className="bg-blue-50 text-blue-700 rounded px-2 py-0.5">{r.watermark_after || "null"}</span>
              </div>
            </div>
          )}

          {/* Triggered by */}
          {r.triggered_by_pipeline_id && (
            <div className="text-xs text-slate-500">
              Triggered by pipeline <span className="font-mono text-slate-600">{r.triggered_by_pipeline_id.slice(0, 8)}</span>
              {r.triggered_by_run_id && <span> (run <span className="font-mono text-slate-600">{r.triggered_by_run_id.slice(0, 8)}</span>)</span>}
            </div>
          )}

          {/* Quality gate checks */}
          {checks.length > 0 && (
            <div className="bg-white border border-slate-200 rounded-lg px-3 py-2">
              <div className="flex items-center justify-between mb-2">
                <span className="text-[10px] uppercase text-slate-400 font-semibold">Quality Gate</span>
                <Pill
                  label={r.quality_results.decision || r.gate_decision || "unknown"}
                  color={r.gate_decision === "halt" ? "red" : r.gate_decision === "promote_with_warning" ? "amber" : "green"}
                />
              </div>
              <div className="space-y-1">
                {checks.map((c, i) => (
                  <div key={i} className="flex items-center gap-2 text-xs">
                    <span className={`w-2 h-2 rounded-full flex-shrink-0 ${
                      c.status === "pass" ? "bg-green-400" :
                      c.status === "warn" ? "bg-amber-400" : "bg-red-400"
                    }`} />
                    <span className="font-medium text-slate-600 w-36">{c.name}</span>
                    <span className="text-slate-400 truncate">{c.detail}</span>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Run Insights */}
          {r.insights && r.insights.length > 0 && (
            <div className="bg-white border border-indigo-200 rounded-lg px-3 py-2">
              <div className="flex items-center gap-2 mb-2">
                <span className="text-[10px] uppercase text-indigo-400 font-semibold">Insights</span>
                <span className="text-[10px] bg-indigo-100 text-indigo-600 rounded-full px-1.5">{r.insights.length}</span>
              </div>
              <div className="space-y-2">
                {r.insights.map((ins, i) => (
                  <div key={i} className="flex items-start gap-2 text-xs">
                    <span className={`w-2 h-2 rounded-full flex-shrink-0 mt-1 ${
                      ins.priority === "high" ? "bg-red-400" :
                      ins.priority === "medium" ? "bg-amber-400" : "bg-green-400"
                    }`} />
                    <div className="flex-1">
                      <div className="flex items-center gap-1.5 mb-0.5">
                        <span className="bg-slate-100 text-slate-500 rounded px-1.5 py-0 text-[10px] font-medium">{ins.category}</span>
                        {ins.action_type === "patch_pipeline" && (
                          <button
                            className="bg-indigo-50 text-indigo-600 rounded px-1.5 py-0 text-[10px] font-medium hover:bg-indigo-100 cursor-pointer"
                            onClick={(e) => {
                              e.stopPropagation();
                              if (confirm("Apply this suggestion to the pipeline?")) {
                                api("PATCH", `/api/pipelines/${r.pipeline_id}`, ins.action_payload)
                                  .then(() => alert("Applied!"))
                                  .catch((err) => alert("Error: " + err.message));
                              }
                            }}
                          >Apply</button>
                        )}
                      </div>
                      <span className="text-slate-600">{ins.message}</span>
                    </div>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Execution Log with Connector Code Links */}
          {r.execution_log && r.execution_log.length > 0 && (
            <ExecutionLogWithCode
              log={r.execution_log}
              sourceConnectorId={r.source_connector_id}
              targetConnectorId={r.target_connector_id}
            />
          )}

          {/* Error detail */}
          {r.error && (
            <div className="bg-red-50 border border-red-200 rounded-lg px-3 py-2">
              <div className="text-[10px] uppercase text-red-400 font-semibold mb-1">Error</div>
              <div className="text-xs text-red-700 font-mono whitespace-pre-wrap">{r.error}</div>
            </div>
          )}

          {/* Run ID */}
          <div className="text-[10px] font-mono text-slate-400">Run ID: {r.run_id}</div>
        </div>
      )}
    </div>
  );
}

function ActivityView({ searchQuery }) {
  const [runs, setRuns] = useState([]);
  const [filter, setFilter] = useState("all");
  useEffect(() => {
    api("GET", "/api/pipelines")
      .then(async (pipelines) => {
        const allRuns = await Promise.all(
          pipelines.slice(0, 20).map((p) =>
            api("GET", `/api/pipelines/${p.pipeline_id}/runs?limit=10`)
              .then((rs) => rs.map((r) => ({ ...r, pipeline_name: p.pipeline_name, tier: p.tier, source_connector_id: p.source_connector_id, target_connector_id: p.target_connector_id })))
              .catch(() => [])
          )
        );
        const flat = allRuns.flat().sort((a, b) => (b.started_at || "").localeCompare(a.started_at || ""));
        setRuns(flat.slice(0, 100));
      })
      .catch(console.error);
  }, []);

  const searched = searchQuery ? runs.filter((r) => r.pipeline_name?.toLowerCase().includes(searchQuery)) : runs;
  const filtered = filter === "all" ? searched
    : filter === "failed" ? searched.filter((r) => r.status === "failed" || r.status === "halted")
    : filter === "complete" ? searched.filter((r) => r.status === "complete")
    : searched;

  return (
    <div className="px-6 py-4">
      <div className="flex items-center justify-between mb-4">
        <h1 className="text-lg font-semibold text-slate-800">Activity</h1>
        <div className="flex gap-1">
          {["all", "complete", "failed"].map((f) => (
            <button
              key={f}
              onClick={() => setFilter(f)}
              className={`px-3 py-1 rounded-lg text-xs font-medium transition-colors ${
                filter === f
                  ? "bg-slate-800 text-white"
                  : "bg-slate-100 text-slate-500 hover:bg-slate-200"
              }`}
            >
              {f === "all" ? `All (${runs.length})` : f === "complete" ? "Completed" : "Failed/Halted"}
            </button>
          ))}
        </div>
      </div>
      <div className="bg-white border border-slate-200 rounded-xl overflow-hidden">
        {filtered.map((r) => <ActivityRunDetail key={r.run_id} r={r} />)}
        {filtered.length === 0 && (
          <div className="text-sm text-slate-400 py-8 text-center">No activity yet.</div>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// 4. Freshness View
// ---------------------------------------------------------------------------

function FreshnessChart({ pipelineId, warnMin, critMin }) {
  const [history, setHistory] = useState([]);
  const [hours, setHours] = useState(24);
  useEffect(() => {
    api("GET", `/api/observability/freshness/${pipelineId}/history?hours=${hours}`)
      .then(setHistory).catch(() => setHistory([]));
  }, [pipelineId, hours]);

  if (!history.length) return React.createElement("div", { className: "text-xs text-slate-400 py-4 text-center" }, "No history data yet \u2014 freshness snapshots accumulate over time.");

  const W = 560, H = 140, PAD_L = 48, PAD_R = 12, PAD_T = 8, PAD_B = 24;
  const chartW = W - PAD_L - PAD_R, chartH = H - PAD_T - PAD_B;

  const points = history.map((s) => ({
    t: new Date(s.checked_at).getTime(),
    v: s.staleness_minutes,
    status: s.status,
  }));
  const tMin = points[0].t, tMax = points[points.length - 1].t;
  const tRange = Math.max(tMax - tMin, 1);
  const maxVal = Math.max(critMin * 1.3, ...points.map((p) => p.v)) || critMin * 1.5;

  const x = (t) => PAD_L + ((t - tMin) / tRange) * chartW;
  const y = (v) => PAD_T + chartH - (Math.min(v, maxVal) / maxVal) * chartH;

  const linePath = points.map((pt, i) => `${i === 0 ? "M" : "L"}${x(pt.t).toFixed(1)},${y(pt.v).toFixed(1)}`).join(" ");
  const areaPath = linePath + ` L${x(points[points.length - 1].t).toFixed(1)},${(PAD_T + chartH).toFixed(1)} L${x(points[0].t).toFixed(1)},${(PAD_T + chartH).toFixed(1)} Z`;

  const warnY = y(warnMin), critY = y(critMin);
  const fmtMin = (m) => m >= 1440 ? `${(m/1440).toFixed(0)}d` : m >= 60 ? `${(m/60).toFixed(0)}h` : `${Math.round(m)}m`;

  const ticks = 5;
  const yTicks = Array.from({ length: ticks + 1 }, (_, i) => (maxVal / ticks) * i);

  const timeTicks = [];
  const tickCount = Math.min(6, points.length);
  for (let i = 0; i < tickCount; i++) {
    const idx = Math.round((i / (tickCount - 1)) * (points.length - 1));
    timeTicks.push(points[idx]);
  }

  return React.createElement("div", null,
    React.createElement("div", { className: "flex items-center justify-between mb-1" },
      React.createElement("span", { className: "text-[10px] uppercase text-slate-400 font-semibold" }, "Staleness Over Time"),
      React.createElement("div", { className: "flex gap-1" },
        [6, 24, 72, 168].map((h) =>
          React.createElement("button", {
            key: h, onClick: (e) => { e.stopPropagation(); setHours(h); },
            className: `px-2 py-0.5 rounded text-[10px] font-medium ${hours === h ? "bg-slate-800 text-white" : "bg-slate-100 text-slate-500 hover:bg-slate-200"}`
          }, h <= 24 ? `${h}h` : `${h/24}d`)
        )
      )
    ),
    React.createElement("svg", { viewBox: `0 0 ${W} ${H}`, className: "w-full", style: { maxHeight: 180 } },
      // Grid lines
      yTicks.map((v, i) => React.createElement("line", { key: `g${i}`, x1: PAD_L, x2: W - PAD_R, y1: y(v), y2: y(v), stroke: "#e2e8f0", strokeWidth: 0.5 })),
      // Y-axis labels
      yTicks.map((v, i) => React.createElement("text", { key: `y${i}`, x: PAD_L - 4, y: y(v) + 3, textAnchor: "end", fontSize: 9, fill: "#94a3b8", fontFamily: "JetBrains Mono, monospace" }, fmtMin(v))),
      // Warn threshold
      warnY >= PAD_T && React.createElement("line", { x1: PAD_L, x2: W - PAD_R, y1: warnY, y2: warnY, stroke: "#d97706", strokeWidth: 1, strokeDasharray: "4 3", opacity: 0.6 }),
      warnY >= PAD_T && React.createElement("text", { x: W - PAD_R + 2, y: warnY + 3, fontSize: 8, fill: "#d97706", fontFamily: "JetBrains Mono, monospace" }, "warn"),
      // Critical threshold
      critY >= PAD_T && React.createElement("line", { x1: PAD_L, x2: W - PAD_R, y1: critY, y2: critY, stroke: "#dc2626", strokeWidth: 1, strokeDasharray: "4 3", opacity: 0.6 }),
      critY >= PAD_T && React.createElement("text", { x: W - PAD_R + 2, y: critY + 3, fontSize: 8, fill: "#dc2626", fontFamily: "JetBrains Mono, monospace" }, "crit"),
      // Area fill
      React.createElement("path", { d: areaPath, fill: "url(#freshGrad)", opacity: 0.3 }),
      // Line
      React.createElement("path", { d: linePath, fill: "none", stroke: "#16a34a", strokeWidth: 1.5, strokeLinejoin: "round" }),
      // Dots
      points.map((pt, i) => React.createElement("circle", {
        key: i, cx: x(pt.t), cy: y(pt.v), r: 2.5,
        fill: pt.status === "fresh" ? "#16a34a" : pt.status === "warning" ? "#d97706" : "#dc2626",
        stroke: "white", strokeWidth: 1,
      })),
      // Time labels
      timeTicks.map((pt, i) => {
        const d = new Date(pt.t);
        const label = `${String(d.getHours()).padStart(2,"0")}:${String(d.getMinutes()).padStart(2,"0")}`;
        return React.createElement("text", { key: `t${i}`, x: x(pt.t), y: H - 2, textAnchor: "middle", fontSize: 9, fill: "#94a3b8", fontFamily: "JetBrains Mono, monospace" }, label);
      }),
      // Gradient def
      React.createElement("defs", null,
        React.createElement("linearGradient", { id: "freshGrad", x1: 0, y1: 0, x2: 0, y2: 1 },
          React.createElement("stop", { offset: "0%", stopColor: "#16a34a", stopOpacity: 0.4 }),
          React.createElement("stop", { offset: "100%", stopColor: "#16a34a", stopOpacity: 0.05 })
        )
      )
    )
  );
}

function FreshnessCard({ p }) {
  const [expanded, setExpanded] = useState(false);
  const pct = Math.min(100, (p.staleness_minutes / (p.freshness_sla_minutes * 5)) * 100);
  const color = p.status === "fresh" ? "green" : p.status === "warning" ? "amber" : "red";
  const fmtMin = (m) => {
    if (m == null) return "--";
    if (m >= 1440) return `${(m / 1440).toFixed(1)}d`;
    if (m >= 60) return `${(m / 60).toFixed(1)}h`;
    return `${Math.round(m)}m`;
  };
  return (
    <div
      className={`bg-white border rounded-xl px-4 py-3 cursor-pointer transition-colors hover:bg-slate-50/50 ${
        p.status === "critical" ? "border-red-200 bg-red-50" : "border-slate-200"
      }`}
      onClick={() => setExpanded(!expanded)}
    >
      <div className="flex items-center gap-3 mb-2">
        <StatusDot status={p.status} />
        <span className="font-mono text-sm font-medium flex-1 text-slate-700">{p.pipeline_name}</span>
        <Pill label={p.status} color={color} />
        <span className="text-xs text-slate-400">
          {fmtMin(p.staleness_minutes)} / {fmtMin(p.freshness_sla_minutes)} SLA
        </span>
        <span className="text-slate-300 text-xs">{expanded ? "\u25B2" : "\u25BC"}</span>
      </div>
      <ProgressBar pct={pct} color={color} />
      {expanded && (
        <div className="mt-3 pt-3 border-t border-slate-100 space-y-3" onClick={(e) => e.stopPropagation()}>
          {/* Staleness time-series chart */}
          <FreshnessChart pipelineId={p.pipeline_id} warnMin={p.freshness_sla_minutes} critMin={p.freshness_critical_minutes || p.freshness_sla_minutes * 3} />

          {/* Detail grid */}
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 text-xs">
            <div>
              <span className="text-slate-400 block">Warn Threshold</span>
              <span className="font-mono text-slate-700">{fmtMin(p.freshness_sla_minutes)}</span>
            </div>
            <div>
              <span className="text-slate-400 block">Critical Threshold</span>
              <span className="font-mono text-slate-700">{fmtMin(p.freshness_critical_minutes)}</span>
            </div>
            <div>
              <span className="text-slate-400 block">Freshness Column</span>
              <span className="font-mono text-slate-700">{p.freshness_column || React.createElement("span", { className: "text-slate-300 italic" }, "last run time")}</span>
            </div>
            <div>
              <span className="text-slate-400 block">Schedule</span>
              <span className="font-mono text-slate-700">{p.schedule || "--"}</span>
            </div>
            <div>
              <span className="text-slate-400 block">Last Record</span>
              <span className="font-mono text-slate-700">{p.last_record_time ? p.last_record_time.replace("T", " ").slice(0, 19) : "--"}</span>
            </div>
            <div>
              <span className="text-slate-400 block">Last Successful Run</span>
              <span className="font-mono text-slate-700">{p.last_run_at ? p.last_run_at.replace("T", " ").slice(0, 19) : "--"}</span>
            </div>
            <div>
              <span className="text-slate-400 block">Rows (last run)</span>
              <span className="font-mono text-slate-700">{p.last_run_rows?.toLocaleString() ?? "--"}</span>
            </div>
            <div>
              <span className="text-slate-400 block">Target Table</span>
              <span className="font-mono text-slate-700">{p.target_table || "--"}</span>
            </div>
          </div>
          <div className="text-[10px] font-mono text-slate-400">
            Checked: {p.checked_at?.replace("T", " ").slice(0, 19) || "--"} &middot; Pipeline: {p.pipeline_id?.slice(0, 12)}
          </div>
        </div>
      )}
    </div>
  );
}

function FreshnessView({ tierFilter, searchQuery }) {
  const [data, setData] = useState({});
  useEffect(() => {
    const tierParam = tierFilter !== "All" ? `?tier=${tierFilter[1]}` : "";
    api("GET", `/api/observability/freshness${tierParam}`)
      .then((d) => setData(d.tiers || {}))
      .catch(console.error);
  }, [tierFilter]);

  return (
    <div className="px-6 py-4">
      <h1 className="text-lg font-semibold mb-4 text-slate-800">Freshness</h1>
      {Object.entries(data).map(([tier, pipelines]) => {
        const filtered = searchQuery ? pipelines.filter((p) => p.pipeline_name?.toLowerCase().includes(searchQuery)) : pipelines;
        if (filtered.length === 0) return null;
        return (
          <div key={tier} className="mb-6">
            <div className="flex items-center gap-2 mb-3">
              <TierBadge tier={parseInt(tier)} />
              <span className="text-sm font-medium text-slate-500">{filtered.length} pipeline(s)</span>
            </div>
            <div className="space-y-2">
              {filtered.map((p) => <FreshnessCard key={p.pipeline_id} p={p} />)}
            </div>
          </div>
        );
      })}
      {Object.keys(data).length === 0 && (
        <div className="text-sm text-slate-400 py-8 text-center">No freshness data yet.</div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// 5. Quality View
// ---------------------------------------------------------------------------

function QualityCard({ p }) {
  const [expanded, setExpanded] = useState(false);
  const q = p.quality;
  const passRate = q?.summary?.pass_rate ?? null;
  const gates = q?.gates?.slice(0, 20) || [];
  const checkStats = q?.summary?.check_stats || {};
  const halted = q?.summary?.halted || 0;
  const totalRuns = q?.summary?.total_runs || 0;
  const lastGate = gates[0] || null;

  const gateColor = (d) =>
    d === "promote" ? "bg-green-500" : d === "promote_with_warning" ? "bg-amber-500" : "bg-red-500";

  // Find worst check by fail rate
  const checkEntries = Object.entries(checkStats);
  let worstCheck = null;
  if (checkEntries.length > 0) {
    worstCheck = checkEntries.reduce((worst, [name, stats]) => {
      const total = (stats.pass || 0) + (stats.warn || 0) + (stats.fail || 0);
      const failRate = total > 0 ? (stats.fail || 0) / total : 0;
      if (!worst || failRate > worst.failRate) return { name, failRate, stats, total };
      return worst;
    }, null);
    if (worstCheck && worstCheck.failRate === 0) worstCheck = null;
  }

  return (
    <div
      className="bg-white border border-slate-200 rounded-xl px-4 py-4 cursor-pointer hover:bg-slate-50/50 transition-colors"
      onClick={() => setExpanded(!expanded)}
    >
      <div className="flex items-center gap-2 mb-2">
        <TierBadge tier={p.tier} />
        <span className="font-mono text-sm font-medium flex-1 truncate text-slate-700">{p.pipeline_name}</span>
        <span className="text-slate-300 text-xs">{expanded ? "\u25B2" : "\u25BC"}</span>
      </div>
      {q ? (
        <>
          <div className="flex items-baseline gap-3 mb-1">
            <div
              className="text-3xl font-semibold font-mono"
              style={{ color: passRate > 0.95 ? "#16a34a" : passRate > 0.8 ? "#d97706" : "#dc2626" }}
            >
              {passRate !== null ? `${(passRate * 100).toFixed(1)}%` : "--"}
            </div>
            <div className="text-xs text-slate-400">
              {totalRuns} runs (7d)
              {halted > 0 && <span className="text-red-500 font-medium ml-2">{halted} halted</span>}
            </div>
          </div>
          <div className="flex flex-wrap gap-1 mb-1">
            {gates.map((g, i) => (
              <div key={i} className={`w-3.5 h-3.5 rounded-sm ${gateColor(g.decision)}`} title={`${g.decision} — ${g.evaluated_at?.slice(0, 16)}`} />
            ))}
          </div>
          {worstCheck && (
            <div className="text-[10px] text-red-500 mt-1">
              Weakest: <span className="font-mono font-medium">{worstCheck.name}</span> ({worstCheck.stats.fail} fail / {worstCheck.total})
            </div>
          )}

          {expanded && (
            <div className="mt-3 pt-3 border-t border-slate-100 space-y-3" onClick={(e) => e.stopPropagation()}>
              {/* Per-check breakdown */}
              {checkEntries.length > 0 && (
                <div>
                  <div className="text-[10px] uppercase text-slate-400 font-semibold mb-2">Check Breakdown (7d)</div>
                  <div className="space-y-1.5">
                    {checkEntries.map(([name, stats]) => {
                      const total = (stats.pass || 0) + (stats.warn || 0) + (stats.fail || 0);
                      const passPct = total > 0 ? ((stats.pass || 0) / total) * 100 : 0;
                      const warnPct = total > 0 ? ((stats.warn || 0) / total) * 100 : 0;
                      const failPct = total > 0 ? ((stats.fail || 0) / total) * 100 : 0;
                      return (
                        <div key={name}>
                          <div className="flex items-center justify-between text-xs mb-0.5">
                            <span className="font-mono text-slate-600 truncate flex-1">{name}</span>
                            <span className="text-slate-400 text-[10px] ml-2 whitespace-nowrap">
                              {stats.pass}p {stats.warn > 0 ? `${stats.warn}w ` : ""}{stats.fail > 0 ? `${stats.fail}f` : ""}
                            </span>
                          </div>
                          <div className="flex h-1.5 rounded-full overflow-hidden bg-slate-100">
                            {passPct > 0 && <div className="bg-green-400" style={{ width: `${passPct}%` }} />}
                            {warnPct > 0 && <div className="bg-amber-400" style={{ width: `${warnPct}%` }} />}
                            {failPct > 0 && <div className="bg-red-400" style={{ width: `${failPct}%` }} />}
                          </div>
                        </div>
                      );
                    })}
                  </div>
                </div>
              )}

              {/* Last gate detail */}
              {lastGate && (
                <div>
                  <div className="text-[10px] uppercase text-slate-400 font-semibold mb-1">Last Gate</div>
                  <div className="bg-slate-50 border border-slate-200 rounded-lg px-3 py-2">
                    <div className="flex items-center justify-between mb-1.5">
                      <span className="text-xs font-mono text-slate-400">{lastGate.evaluated_at?.replace("T", " ").slice(0, 19)}</span>
                      <Pill
                        label={lastGate.decision}
                        color={lastGate.decision === "halt" ? "red" : lastGate.decision === "promote_with_warning" ? "amber" : "green"}
                      />
                    </div>
                    <div className="space-y-1">
                      {(lastGate.checks || []).map((c, i) => (
                        <div key={i} className="flex items-center gap-2 text-xs">
                          <span className={`w-2 h-2 rounded-full flex-shrink-0 ${
                            c.status === "pass" ? "bg-green-400" : c.status === "warn" ? "bg-amber-400" : "bg-red-400"
                          }`} />
                          <span className="font-medium text-slate-600 w-36">{c.name}</span>
                          <span className="text-slate-400 truncate">{c.detail}</span>
                        </div>
                      ))}
                    </div>
                    {lastGate.agent_reasoning && (
                      <div className="mt-2 pt-2 border-t border-slate-100">
                        <div className="text-[10px] uppercase text-slate-400 font-semibold mb-1">Agent Reasoning</div>
                        <div className="text-xs text-slate-500 italic">{lastGate.agent_reasoning}</div>
                      </div>
                    )}
                  </div>
                </div>
              )}
            </div>
          )}
        </>
      ) : (
        <div className="text-xs text-slate-300">No data yet</div>
      )}
    </div>
  );
}

function QualityView({ tierFilter, searchQuery }) {
  const [cards, setCards] = useState([]);
  useEffect(() => {
    const tierParam = tierFilter !== "All" ? `?tier=${tierFilter[1]}` : "";
    api("GET", `/api/pipelines${tierParam}`)
      .then(async (pipelines) => {
        const withQuality = await Promise.all(
          pipelines.map(async (p) => {
            try {
              const q = await api("GET", `/api/quality/${p.pipeline_id}?days=7`);
              return { ...p, quality: q };
            } catch {
              return { ...p, quality: null };
            }
          })
        );
        setCards(withQuality);
      })
      .catch(console.error);
  }, [tierFilter]);

  const filtered = searchQuery ? cards.filter((p) => p.pipeline_name?.toLowerCase().includes(searchQuery)) : cards;

  return (
    <div className="px-6 py-4">
      <h1 className="text-lg font-semibold mb-4 text-slate-800">Quality</h1>
      <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-3">
        {filtered.map((p) => <QualityCard key={p.pipeline_id} p={p} />)}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// 6. Approvals View
// ---------------------------------------------------------------------------

function ApprovalCard({ p, isPending, note, setNote, onResolve, connectorCode, setConnectorCode, expandedCode, setExpandedCode, testResults, setTestResults, testing, setTesting }) {
  const [expanded, setExpanded] = useState(false);

  const changeColor = (t) =>
    ({ add_column: "green", drop_column: "red", alter_column_type: "amber", new_connector: "purple",
       change_refresh_type: "blue", change_load_type: "blue", change_merge_keys: "blue",
       change_schedule: "blue", add_table: "green", remove_table: "red", update_connector: "purple" }[t] || "blue");

  const riskColor = (r) => ({ high: "text-red-600 bg-red-50", medium: "text-amber-600 bg-amber-50", low: "text-green-600 bg-green-50" }[r] || "text-slate-500 bg-slate-50");

  const confidenceColor = (c) => c >= 0.9 ? "text-green-600" : c >= 0.7 ? "text-amber-600" : "text-red-600";

  async function toggleCode(proposalId, connectorId) {
    if (expandedCode[proposalId]) {
      setExpandedCode((s) => ({ ...s, [proposalId]: false }));
      return;
    }
    if (!connectorCode[connectorId]) {
      try {
        const detail = await api("GET", `/api/connectors/${connectorId}`);
        setConnectorCode((s) => ({ ...s, [connectorId]: detail.code || "# No code available" }));
      } catch (e) {
        setConnectorCode((s) => ({ ...s, [connectorId]: `# Error loading code: ${e.message}` }));
      }
    }
    setExpandedCode((s) => ({ ...s, [proposalId]: true }));
  }

  async function testConnector(connectorId) {
    setTesting((s) => ({ ...s, [connectorId]: true }));
    try {
      const result = await api("POST", `/api/connectors/${connectorId}/test`);
      setTestResults((s) => ({ ...s, [connectorId]: result }));
    } catch (e) {
      setTestResults((s) => ({ ...s, [connectorId]: { success: false, error: e.message } }));
    }
    setTesting((s) => ({ ...s, [connectorId]: false }));
  }

  const impact = p.impact_analysis || {};
  const hasStateDiff = (p.current_state && Object.keys(p.current_state).length > 0) || (p.proposed_state && Object.keys(p.proposed_state).length > 0);

  return (
    <div className={`border rounded-xl overflow-hidden ${
      isPending
        ? (impact.breaking_change ? "border-red-300 bg-red-50/50" : "border-amber-200 bg-amber-50")
        : "border-slate-200 bg-white"
    }`}>
      <div
        className="flex items-center gap-2 px-4 py-3 cursor-pointer hover:bg-white/50 transition-colors"
        onClick={() => setExpanded(!expanded)}
      >
        <Pill label={p.change_type} color={changeColor(p.change_type)} />
        <Pill label={p.trigger_type} color="gray" />
        {p.pipeline_name && (
          <span className="font-mono text-xs text-slate-600">{p.pipeline_name}</span>
        )}
        {p.connector_name && (
          <span className="font-mono text-xs text-purple-600">{p.connector_name}</span>
        )}
        <span className="flex-1" />
        {impact.breaking_change && <Pill label="BREAKING" color="red" />}
        <span className={`text-xs font-medium ${confidenceColor(p.confidence)}`}>
          {(p.confidence * 100).toFixed(0)}% confidence
        </span>
        {!isPending && <Pill label={p.status} color={p.status === "applied" || p.status === "approved" ? "green" : p.status === "rolled_back" ? "amber" : "red"} />}
        <span className="text-xs text-slate-400">{(isPending ? p.created_at : p.resolved_at)?.slice(0, 16)}</span>
        <span className="text-slate-300 text-xs">{expanded ? "\u25B2" : "\u25BC"}</span>
      </div>

      {expanded && (
        <div className="px-4 pb-4 space-y-3 border-t border-slate-100">
          {/* Reasoning */}
          <div className="pt-3">
            <div className="text-[10px] uppercase text-slate-400 font-semibold mb-1">Agent Reasoning</div>
            <p className="text-sm text-slate-600">{p.reasoning}</p>
          </div>

          {/* Impact analysis */}
          {Object.keys(impact).length > 0 && (
            <div className="bg-white border border-slate-200 rounded-lg px-3 py-2">
              <div className="text-[10px] uppercase text-slate-400 font-semibold mb-2">Impact Analysis</div>
              <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 text-xs">
                {impact.breaking_change != null && (
                  <div>
                    <span className="text-slate-400 block">Breaking Change</span>
                    <span className={`font-medium ${impact.breaking_change ? "text-red-600" : "text-green-600"}`}>
                      {impact.breaking_change ? "Yes" : "No"}
                    </span>
                  </div>
                )}
                {impact.data_loss_risk && (
                  <div>
                    <span className="text-slate-400 block">Data Loss Risk</span>
                    <span className={`font-medium px-1.5 py-0.5 rounded text-xs ${riskColor(impact.data_loss_risk)}`}>
                      {impact.data_loss_risk}
                    </span>
                  </div>
                )}
                {impact.downtime_required != null && (
                  <div>
                    <span className="text-slate-400 block">Downtime Required</span>
                    <span className="font-mono text-slate-700">{impact.downtime_required ? "Yes" : "No"}</span>
                  </div>
                )}
                {impact.affected_pipelines != null && (
                  <div>
                    <span className="text-slate-400 block">Affected Pipelines</span>
                    <span className="font-mono text-slate-700">{impact.affected_pipelines}</span>
                  </div>
                )}
                {impact.affected_consumers != null && (
                  <div>
                    <span className="text-slate-400 block">Affected Consumers</span>
                    <span className="font-mono text-slate-700">{impact.affected_consumers}</span>
                  </div>
                )}
              </div>
            </div>
          )}

          {/* State diff */}
          {hasStateDiff && (
            <div className="bg-white border border-slate-200 rounded-lg px-3 py-2">
              <div className="text-[10px] uppercase text-slate-400 font-semibold mb-2">Change Diff</div>
              <div className="grid grid-cols-2 gap-3">
                <div>
                  <div className="text-[10px] text-red-400 font-semibold mb-1">Current</div>
                  <pre className="text-xs font-mono text-slate-600 bg-red-50/50 rounded p-2 overflow-x-auto max-h-32 overflow-y-auto">
                    {JSON.stringify(p.current_state, null, 2) || "{}"}
                  </pre>
                </div>
                <div>
                  <div className="text-[10px] text-green-500 font-semibold mb-1">Proposed</div>
                  <pre className="text-xs font-mono text-slate-600 bg-green-50/50 rounded p-2 overflow-x-auto max-h-32 overflow-y-auto">
                    {JSON.stringify(p.proposed_state, null, 2) || "{}"}
                  </pre>
                </div>
              </div>
            </div>
          )}

          {/* Trigger detail */}
          {p.trigger_detail && Object.keys(p.trigger_detail).length > 0 && (
            <div className="bg-white border border-slate-200 rounded-lg px-3 py-2">
              <div className="text-[10px] uppercase text-slate-400 font-semibold mb-1">Trigger Detail</div>
              <pre className="text-xs font-mono text-slate-600 overflow-x-auto">
                {JSON.stringify(p.trigger_detail, null, 2)}
              </pre>
            </div>
          )}

          {/* Rollback plan */}
          {p.rollback_plan && (
            <div className="bg-blue-50 border border-blue-200 rounded-lg px-3 py-2">
              <div className="text-[10px] uppercase text-blue-400 font-semibold mb-1">Rollback Plan</div>
              <p className="text-xs text-blue-700">{p.rollback_plan}</p>
            </div>
          )}

          {/* Connector code review (for new_connector / update_connector) */}
          {(p.change_type === "new_connector" || p.change_type === "update_connector") && p.connector_id && (
            <div>
              <div className="flex items-center gap-2">
                <button
                  onClick={() => toggleCode(p.proposal_id, p.connector_id)}
                  className="text-xs text-purple-600 hover:text-purple-800 font-medium"
                >
                  {expandedCode[p.proposal_id] ? "Hide Code" : "Review Connector Code"}
                </button>
                {isPending && (
                  <button
                    onClick={() => testConnector(p.connector_id)}
                    disabled={testing[p.connector_id]}
                    className="text-xs px-2.5 py-1 bg-purple-100 text-purple-700 border border-purple-200 rounded-lg hover:bg-purple-200 disabled:opacity-50"
                  >
                    {testing[p.connector_id] ? "Testing..." : "Test Connector"}
                  </button>
                )}
                {testResults[p.connector_id] && (
                  <span className={`text-xs font-medium ${testResults[p.connector_id].success ? "text-green-600" : "text-red-600"}`}>
                    {testResults[p.connector_id].success ? "PASSED" : "FAILED"}
                    {testResults[p.connector_id].error && ` — ${testResults[p.connector_id].error}`}
                  </span>
                )}
              </div>
              {expandedCode[p.proposal_id] && connectorCode[p.connector_id] && (
                <pre className="mt-2 bg-slate-900 text-green-300 text-xs p-3 rounded-lg overflow-x-auto max-h-80 overflow-y-auto font-mono leading-relaxed">
                  {connectorCode[p.connector_id]}
                </pre>
              )}
            </div>
          )}

          {/* Version info */}
          <div className="flex items-center gap-4 text-[10px] font-mono text-slate-400">
            {p.contract_version_before != null && (
              <span>Contract v{p.contract_version_before}{p.contract_version_after != null && ` → v${p.contract_version_after}`}</span>
            )}
            <span>ID: {p.proposal_id?.slice(0, 12)}</span>
            {p.pipeline_id && <span>Pipeline: {p.pipeline_id?.slice(0, 12)}</span>}
          </div>

          {/* Resolved info */}
          {!isPending && p.resolved_by && (
            <div className="bg-slate-50 border border-slate-200 rounded-lg px-3 py-2">
              <div className="flex items-center gap-3 text-xs">
                <span className="text-slate-400">Resolved by</span>
                <span className="font-medium text-slate-600">{p.resolved_by}</span>
                <span className="text-slate-400">{p.resolved_at?.replace("T", " ").slice(0, 19)}</span>
              </div>
              {p.resolution_note && (
                <p className="text-xs text-slate-500 mt-1 italic">"{p.resolution_note}"</p>
              )}
            </div>
          )}

          {/* Approve/Reject actions */}
          {isPending && (
            <div className="flex items-center gap-2 pt-1">
              <input
                value={note[p.proposal_id] || ""}
                onChange={(e) => setNote((n) => ({ ...n, [p.proposal_id]: e.target.value }))}
                placeholder="Approval note (required for production changes)..."
                className="flex-1 text-xs px-3 py-1.5 border border-slate-300 rounded-lg bg-white text-slate-600 outline-none focus:ring-2 focus:ring-blue-200 focus:border-blue-400"
              />
              <button
                onClick={() => onResolve(p.proposal_id, "approve")}
                className="text-xs px-4 py-1.5 bg-green-600 text-white rounded-lg hover:bg-green-700 font-medium"
              >
                Approve
              </button>
              <button
                onClick={() => onResolve(p.proposal_id, "reject")}
                className="text-xs px-4 py-1.5 bg-red-50 text-red-600 border border-red-200 rounded-lg hover:bg-red-100 font-medium"
              >
                Reject
              </button>
            </div>
          )}
        </div>
      )}
    </div>
  );
}

function ApprovalsView({ searchQuery }) {
  const [pending, setPending] = useState([]);
  const [resolved, setResolved] = useState([]);
  const [note, setNote] = useState({});
  const [connectorCode, setConnectorCode] = useState({});
  const [expandedCode, setExpandedCode] = useState({});
  const [testResults, setTestResults] = useState({});
  const [testing, setTesting] = useState({});
  const [showResolved, setShowResolved] = useState(false);

  useEffect(() => {
    api("GET", "/api/approvals?status=pending").then(setPending).catch(console.error);
    // Fetch all non-pending: applied, approved, rejected, rolled_back
    api("GET", "/api/approvals")
      .then((all) => setResolved(all.filter((p) => p.status !== "pending")))
      .catch(console.error);
  }, []);

  async function handleResolve(id, action) {
    await api("POST", `/api/approvals/${id}`, { action, note: note[id] || "" });
    const resolved_item = pending.find((x) => x.proposal_id === id);
    setPending((p) => p.filter((x) => x.proposal_id !== id));
    if (resolved_item) {
      setResolved((r) => [{ ...resolved_item, status: action === "approve" ? "applied" : "rejected", resolved_at: new Date().toISOString() }, ...r]);
    }
  }

  const matchApproval = (p) => !searchQuery ||
    (p.reasoning || "").toLowerCase().includes(searchQuery) ||
    (p.change_type || "").toLowerCase().includes(searchQuery) ||
    (p.pipeline_name || "").toLowerCase().includes(searchQuery) ||
    (p.connector_name || "").toLowerCase().includes(searchQuery);
  const filteredPending = pending.filter(matchApproval);
  const filteredResolved = resolved.filter(matchApproval);

  const breakingCount = filteredPending.filter((p) => p.impact_analysis?.breaking_change).length;
  const sharedProps = { note, setNote, connectorCode, setConnectorCode, expandedCode, setExpandedCode, testResults, setTestResults, testing, setTesting };

  return (
    <div className="px-6 py-4">
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center gap-3">
          <h1 className="text-lg font-semibold text-slate-800">Approvals</h1>
          {filteredPending.length > 0 && (
            <span className="text-xs px-2.5 py-1 bg-amber-100 text-amber-700 rounded-full font-medium">
              {filteredPending.length} pending
            </span>
          )}
          {breakingCount > 0 && (
            <span className="text-xs px-2.5 py-1 bg-red-100 text-red-700 rounded-full font-medium">
              {breakingCount} breaking
            </span>
          )}
        </div>
        <button
          onClick={() => setShowResolved(!showResolved)}
          className={`text-xs px-3 py-1 rounded-lg font-medium transition-colors ${
            showResolved ? "bg-slate-800 text-white" : "bg-slate-100 text-slate-500 hover:bg-slate-200"
          }`}
        >
          {showResolved ? "Hide" : "Show"} Resolved ({filteredResolved.length})
        </button>
      </div>

      {/* Pending */}
      {filteredPending.length > 0 ? (
        <div className="space-y-3 mb-6">
          {filteredPending.map((p) => (
            <ApprovalCard key={p.proposal_id} p={p} isPending={true} onResolve={handleResolve} {...sharedProps} />
          ))}
        </div>
      ) : (
        <div className="text-sm text-slate-400 py-8 text-center mb-6 bg-green-50 border border-green-200 rounded-xl">
          No pending approvals — all structural changes reviewed.
        </div>
      )}

      {/* Resolved */}
      {showResolved && (
        <div>
          <div className="text-sm font-medium text-slate-400 mb-3">Resolved</div>
          <div className="space-y-2">
            {filteredResolved.map((p) => (
              <ApprovalCard key={p.proposal_id} p={p} isPending={false} onResolve={handleResolve} {...sharedProps} />
            ))}
            {filteredResolved.length === 0 && <div className="text-xs text-slate-300 text-center py-4">No resolved proposals yet.</div>}
          </div>
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// 7. Lineage & DAG View (consolidated from Build 19 + Lineage)
// ---------------------------------------------------------------------------

function DAGView({ searchQuery }) {
  const [dag, setDag] = useState(null);
  const [selected, setSelected] = useState(null);
  const [lineageDetail, setLineageDetail] = useState(null);
  const search = searchQuery || "";
  const [loading, setLoading] = useState(true);
  const [zoom, setZoom] = useState(1);
  const [pan, setPan] = useState({ x: 0, y: 0 });
  const [dragging, setDragging] = useState(false);
  const [dragStart, setDragStart] = useState({ x: 0, y: 0 });
  const [didDrag, setDidDrag] = useState(false);
  const svgRef = useRef(null);
  const containerRef = useRef(null);

  useEffect(() => {
    setLoading(true);
    api("GET", "/api/dag")
      .then(setDag)
      .catch(console.error)
      .finally(() => setLoading(false));
  }, []);

  // Fetch column-level lineage when a node is selected
  useEffect(() => {
    if (!selected) { setLineageDetail(null); return; }
    api("GET", `/api/lineage/${selected}`)
      .then(setLineageDetail)
      .catch(() => setLineageDetail(null));
  }, [selected]);

  if (loading) {
    return (
      <div className="px-6 py-4">
        <h1 className="text-lg font-semibold mb-4 text-slate-800">Lineage</h1>
        <div className="text-sm text-slate-400">Loading graph...</div>
      </div>
    );
  }

  if (!dag || dag.nodes.length === 0) {
    return (
      <div className="px-6 py-4">
        <h1 className="text-lg font-semibold mb-4 text-slate-800">Lineage</h1>
        <div className="text-sm text-slate-400">No pipelines found.</div>
      </div>
    );
  }

  // Search: determine which nodes match and their connected neighbors
  const searchLower = search.toLowerCase().trim();
  const matchedIds = new Set();
  const connectedIds = new Set();
  if (searchLower) {
    dag.nodes.forEach((n) => {
      if (n.name.toLowerCase().includes(searchLower) ||
          n.source.toLowerCase().includes(searchLower) ||
          n.target.toLowerCase().includes(searchLower) ||
          (n.owner || "").toLowerCase().includes(searchLower)) {
        matchedIds.add(n.id);
      }
    });
    // Include direct neighbors of matched nodes
    dag.edges.forEach((e) => {
      if (matchedIds.has(e.from)) connectedIds.add(e.to);
      if (matchedIds.has(e.to)) connectedIds.add(e.from);
    });
  }
  const hasSearch = searchLower.length > 0;
  const isVisible = (id) => !hasSearch || matchedIds.has(id) || connectedIds.has(id);
  const isMatch = (id) => matchedIds.has(id);

  // Topological sort into layers
  const nodeMap = {};
  dag.nodes.forEach((n) => { nodeMap[n.id] = n; });
  const inDegree = {};
  const children = {};
  dag.nodes.forEach((n) => { inDegree[n.id] = 0; children[n.id] = []; });
  dag.edges.forEach((e) => {
    inDegree[e.to] = (inDegree[e.to] || 0) + 1;
    if (!children[e.from]) children[e.from] = [];
    children[e.from].push(e.to);
  });

  const layers = [];
  const visited = new Set();
  let queue = dag.nodes.filter((n) => (inDegree[n.id] || 0) === 0).map((n) => n.id);
  if (queue.length === 0) queue = [dag.nodes[0].id];

  while (queue.length > 0) {
    const layer = [];
    const nextQueue = [];
    queue.forEach((id) => {
      if (!visited.has(id)) {
        visited.add(id);
        layer.push(id);
        (children[id] || []).forEach((cid) => {
          inDegree[cid]--;
          if (inDegree[cid] <= 0 && !visited.has(cid)) {
            nextQueue.push(cid);
          }
        });
      }
    });
    if (layer.length > 0) layers.push(layer);
    queue = nextQueue;
  }
  const remaining = dag.nodes.filter((n) => !visited.has(n.id)).map((n) => n.id);
  if (remaining.length > 0) layers.push(remaining);

  // Layout constants
  const nodeW = 200;
  const nodeH = 72;
  const layerGap = 120;
  const nodeGap = 30;
  const padX = 40;
  const padY = 40;

  const positions = {};
  let maxLayerWidth = 0;
  layers.forEach((layer) => {
    maxLayerWidth = Math.max(maxLayerWidth, layer.length);
  });
  const svgWidth = Math.max(800, maxLayerWidth * (nodeW + nodeGap) + padX * 2);

  layers.forEach((layer, li) => {
    const totalW = layer.length * nodeW + (layer.length - 1) * nodeGap;
    const startX = (svgWidth - totalW) / 2;
    layer.forEach((id, ni) => {
      positions[id] = {
        x: startX + ni * (nodeW + nodeGap),
        y: padY + li * (nodeH + layerGap),
      };
    });
  });

  const svgHeight = padY * 2 + layers.length * (nodeH + layerGap);

  const statusColor = (s) => ({
    active: "#4ade80", complete: "#4ade80",
    paused: "#9ca3af",
    failed: "#f87171", halted: "#f87171",
    archived: "#6b7280",
  }[s] || "#9ca3af");

  const tierColor = (t) => ({
    1: "#ef4444", 2: "#f59e0b", 3: "#3b82f6",
  }[t] || "#6b7280");

  const selectedNode = selected ? nodeMap[selected] : null;

  return (
    <div className="px-6 py-4">
      <div className="flex items-center justify-between mb-1">
        <h1 className="text-lg font-semibold text-slate-800">Lineage</h1>
      </div>
      <div className="text-xs text-slate-400 mb-4">
        {dag.total_pipelines} pipeline(s), {dag.total_edges} dependency edge(s)
        {hasSearch && ` — ${matchedIds.size} match(es)`}
      </div>
      <div className="flex gap-4">
        <div className="flex-1 bg-white border border-slate-200 rounded-xl overflow-hidden relative" style={{ maxHeight: "75vh" }}>
          {/* Zoom controls */}
          <div className="absolute top-3 left-3 z-10 flex flex-col gap-1">
            <button onClick={() => setZoom((z) => Math.min(3, z * 1.25))} className="w-7 h-7 bg-white border border-slate-300 rounded-lg text-slate-600 hover:bg-slate-50 text-sm font-bold shadow-sm">+</button>
            <button onClick={() => setZoom((z) => Math.max(0.15, z / 1.25))} className="w-7 h-7 bg-white border border-slate-300 rounded-lg text-slate-600 hover:bg-slate-50 text-sm font-bold shadow-sm">-</button>
            <button onClick={() => { setZoom(1); setPan({ x: 0, y: 0 }); }} className="w-7 h-7 bg-white border border-slate-300 rounded-lg text-slate-500 hover:bg-slate-50 text-[9px] font-semibold shadow-sm">fit</button>
          </div>
          <div className="absolute top-3 right-3 z-10 text-[10px] text-slate-400 bg-white/80 px-2 py-0.5 rounded">{Math.round(zoom * 100)}%</div>
          <svg
            ref={svgRef}
            width="100%"
            height="100%"
            viewBox={`${-pan.x / zoom} ${-pan.y / zoom} ${svgWidth / zoom} ${svgHeight / zoom}`}
            style={{ minHeight: Math.min(svgHeight * zoom, 600), cursor: dragging ? "grabbing" : "grab" }}
            onWheel={(e) => {
              e.preventDefault();
              const factor = e.deltaY < 0 ? 1.1 : 0.9;
              setZoom((z) => Math.min(3, Math.max(0.15, z * factor)));
            }}
            onMouseDown={(e) => {
              if (e.button === 0) {
                setDragging(true);
                setDidDrag(false);
                setDragStart({ x: e.clientX - pan.x, y: e.clientY - pan.y });
              }
            }}
            onMouseMove={(e) => {
              if (dragging) {
                setDidDrag(true);
                setPan({ x: e.clientX - dragStart.x, y: e.clientY - dragStart.y });
              }
            }}
            onMouseUp={() => setDragging(false)}
            onMouseLeave={() => setDragging(false)}
          >
            <defs>
              <marker id="arrow" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto">
                <path d="M0,0 L8,3 L0,6 Z" fill="#94a3b8" />
              </marker>
              <marker id="arrow-contract" markerWidth="8" markerHeight="6" refX="8" refY="3" orient="auto">
                <path d="M0,0 L8,3 L0,6 Z" fill="#8b5cf6" />
              </marker>
            </defs>

            {/* Edges */}
            {dag.edges.map((e, i) => {
              const from = positions[e.from];
              const to = positions[e.to];
              if (!from || !to) return null;
              const edgeVisible = isVisible(e.from) && isVisible(e.to);
              const x1 = from.x + nodeW / 2;
              const y1 = from.y + nodeH;
              const x2 = to.x + nodeW / 2;
              const y2 = to.y;
              const midY = (y1 + y2) / 2;
              const isContract = e.notes && e.notes.includes("data contract");
              return (
                <path
                  key={i}
                  d={`M${x1},${y1} C${x1},${midY} ${x2},${midY} ${x2},${y2}`}
                  fill="none"
                  stroke={isContract ? "#8b5cf6" : "#cbd5e1"}
                  strokeWidth={isContract ? 2 : 1.5}
                  strokeDasharray={isContract ? "6,3" : "none"}
                  markerEnd={isContract ? "url(#arrow-contract)" : "url(#arrow)"}
                  opacity={hasSearch && !edgeVisible ? 0.1 : 1}
                />
              );
            })}

            {/* Nodes */}
            {dag.nodes.map((node) => {
              const pos = positions[node.id];
              if (!pos) return null;
              const isSelected = selected === node.id;
              const dimmed = hasSearch && !isVisible(node.id);
              const highlighted = hasSearch && isMatch(node.id);
              return (
                <g
                  key={node.id}
                  transform={`translate(${pos.x},${pos.y})`}
                  onClick={() => { if (!didDrag) setSelected(isSelected ? null : node.id); }}
                  style={{ cursor: "pointer" }}
                  opacity={dimmed ? 0.15 : 1}
                >
                  <rect
                    width={nodeW}
                    height={nodeH}
                    rx={10}
                    fill={isSelected ? "#eff6ff" : highlighted ? "#fefce8" : "#fff"}
                    stroke={isSelected ? "#3b82f6" : highlighted ? "#eab308" : "#e2e8f0"}
                    strokeWidth={isSelected ? 2 : highlighted ? 2 : 1}
                  />
                  <rect x={0} y={0} width={4} height={nodeH} rx={2} fill={statusColor(node.status)} />
                  <rect x={nodeW - 30} y={6} width={22} height={16} rx={4} fill={tierColor(node.tier)} opacity={0.15} />
                  <text x={nodeW - 19} y={18} textAnchor="middle" fontSize={9} fontFamily="monospace" fontWeight="600" fill={tierColor(node.tier)}>
                    T{node.tier}
                  </text>
                  <text x={14} y={22} fontSize={11} fontFamily="monospace" fontWeight="600" fill="#1e293b">
                    {node.name.length > 22 ? node.name.slice(0, 20) + ".." : node.name}
                  </text>
                  <text x={14} y={38} fontSize={9} fontFamily="monospace" fill="#94a3b8">
                    {node.source.length > 14 ? node.source.slice(0, 12) + ".." : node.source}
                    {" -> "}
                    {node.target.length > 14 ? node.target.slice(0, 12) + ".." : node.target}
                  </text>
                  <text x={14} y={54} fontSize={9} fontFamily="sans-serif" fill="#94a3b8">
                    {node.last_run
                      ? `Last run: ${node.last_run.rows_loaded} rows`
                      : "No runs yet"}
                  </text>
                  {node.contract_violations > 0 && (
                    <g>
                      <circle cx={nodeW - 12} cy={nodeH - 12} r={8} fill="#fef2f2" stroke="#fca5a5" />
                      <text x={nodeW - 12} y={nodeH - 8} textAnchor="middle" fontSize={8} fontWeight="700" fill="#dc2626">
                        {node.contract_violations}
                      </text>
                    </g>
                  )}
                  {(node.contracts_as_producer > 0 || node.contracts_as_consumer > 0) && (
                    <circle cx={nodeW - 12} cy={36} r={4} fill="#8b5cf6" opacity={0.6} />
                  )}
                </g>
              );
            })}
          </svg>
        </div>

        {/* Detail panel with column-level lineage */}
        {selectedNode && (
          <div className="w-80 bg-white border border-slate-200 rounded-xl p-4 space-y-3 max-h-[75vh] overflow-y-auto">
            <div>
              <div className="text-xs text-slate-400">Pipeline</div>
              <div className="text-sm font-mono font-semibold text-slate-800">{selectedNode.name}</div>
            </div>
            <div className="flex gap-2 flex-wrap">
              <span className="flex items-center gap-1 text-xs">
                <StatusDot status={selectedNode.status} />
                {selectedNode.status}
              </span>
              <TierBadge tier={selectedNode.tier} />
              <Pill label={selectedNode.refresh_type} color="blue" />
            </div>
            <div className="text-xs text-slate-500 space-y-1">
              <div><span className="text-slate-400">Source:</span> {selectedNode.source}</div>
              <div><span className="text-slate-400">Target:</span> {selectedNode.target}</div>
              <div><span className="text-slate-400">Schedule:</span> {selectedNode.schedule_cron}</div>
              {selectedNode.owner && <div><span className="text-slate-400">Owner:</span> {selectedNode.owner}</div>}
            </div>
            {selectedNode.last_run && (
              <div className="border-t border-slate-200 pt-2">
                <div className="text-xs text-slate-400 mb-1">Last Successful Run</div>
                <div className="text-xs text-slate-500 space-y-0.5">
                  <div>{selectedNode.last_run.rows_loaded} rows loaded</div>
                  <div>{selectedNode.last_run.completed_at}</div>
                </div>
              </div>
            )}
            {(selectedNode.contracts_as_producer > 0 || selectedNode.contracts_as_consumer > 0) && (
              <div className="border-t border-slate-200 pt-2">
                <div className="text-xs text-slate-400 mb-1">Data Contracts</div>
                <div className="text-xs text-slate-500 space-y-0.5">
                  {selectedNode.contracts_as_producer > 0 && <div>Producer in {selectedNode.contracts_as_producer} contract(s)</div>}
                  {selectedNode.contracts_as_consumer > 0 && <div>Consumer in {selectedNode.contracts_as_consumer} contract(s)</div>}
                  {selectedNode.contract_violations > 0 && (
                    <div className="text-red-600 font-medium">{selectedNode.contract_violations} violation(s)</div>
                  )}
                </div>
              </div>
            )}

            {/* Upstream / Downstream from lineage API */}
            {lineageDetail && (lineageDetail.upstream.length > 0 || lineageDetail.downstream.length > 0) && (
              <div className="border-t border-slate-200 pt-2">
                <div className="text-xs text-slate-400 mb-1">Dependencies</div>
                {lineageDetail.upstream.length > 0 && (
                  <div className="mb-1">
                    <span className="text-[10px] text-slate-400 uppercase">Upstream</span>
                    {lineageDetail.upstream.map((u) => (
                      <div key={u.pipeline_id} className="text-xs font-mono text-slate-600 ml-2">
                        {u.pipeline_name}
                        <span className="text-slate-400 ml-1">({u.dependency_type})</span>
                      </div>
                    ))}
                  </div>
                )}
                {lineageDetail.downstream.length > 0 && (
                  <div>
                    <span className="text-[10px] text-slate-400 uppercase">Downstream</span>
                    {lineageDetail.downstream.map((d) => (
                      <div key={d.pipeline_id} className="text-xs font-mono text-slate-600 ml-2">
                        {d.pipeline_name}
                        <span className="text-slate-400 ml-1">({d.dependency_type})</span>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            )}

            {/* Column-level lineage */}
            {lineageDetail && lineageDetail.column_lineage && lineageDetail.column_lineage.length > 0 && (
              <div className="border-t border-slate-200 pt-2">
                <div className="text-xs text-slate-400 mb-1">Column Lineage</div>
                <div className="overflow-x-auto">
                  <table className="w-full text-[10px]">
                    <thead>
                      <tr className="text-slate-400 border-b border-slate-200">
                        <th className="text-left py-1 px-1">Source</th>
                        <th className="text-left py-1 px-1">Target</th>
                        <th className="text-left py-1 px-1">Transform</th>
                      </tr>
                    </thead>
                    <tbody>
                      {lineageDetail.column_lineage.map((cl) => (
                        <tr key={cl.lineage_id} className="border-b border-slate-200/50">
                          <td className="py-1 px-1 font-mono text-slate-600">{cl.source_column}</td>
                          <td className="py-1 px-1 font-mono text-slate-600">{cl.target_column}</td>
                          <td className="py-1 px-1 text-slate-400">{cl.transform_logic || "direct"}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}

            {/* Downstream column dependencies */}
            {lineageDetail && lineageDetail.downstream_columns && lineageDetail.downstream_columns.length > 0 && (
              <div className="border-t border-slate-200 pt-2">
                <div className="text-xs text-slate-400 mb-1">Downstream Column Deps</div>
                <div className="overflow-x-auto">
                  <table className="w-full text-[10px]">
                    <thead>
                      <tr className="text-slate-400 border-b border-slate-200">
                        <th className="text-left py-1 px-1">Pipeline</th>
                        <th className="text-left py-1 px-1">Source Col</th>
                        <th className="text-left py-1 px-1">Target Col</th>
                      </tr>
                    </thead>
                    <tbody>
                      {lineageDetail.downstream_columns.map((dc) => (
                        <tr key={dc.lineage_id} className="border-b border-slate-200/50">
                          <td className="py-1 px-1 font-mono text-slate-600">{dc.pipeline_id?.slice(0, 8)}</td>
                          <td className="py-1 px-1 font-mono text-slate-600">{dc.source_column}</td>
                          <td className="py-1 px-1 font-mono text-slate-600">{dc.target_column}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}

            <div className="border-t border-slate-200 pt-2">
              <div className="text-xs font-mono text-slate-400 break-all">{selectedNode.id}</div>
            </div>
          </div>
        )}
      </div>

      {/* Legend */}
      <div className="flex gap-6 mt-4 text-xs text-slate-400 flex-wrap">
        <div className="flex items-center gap-1.5">
          <span className="inline-block w-3 h-3 rounded bg-green-400" /> Active
        </div>
        <div className="flex items-center gap-1.5">
          <span className="inline-block w-3 h-3 rounded bg-gray-400" /> Paused
        </div>
        <div className="flex items-center gap-1.5">
          <span className="inline-block w-3 h-3 rounded bg-red-400" /> Failed/Halted
        </div>
        <div className="flex items-center gap-1.5">
          <svg width="24" height="8"><line x1="0" y1="4" x2="24" y2="4" stroke="#cbd5e1" strokeWidth="1.5" /></svg> Dependency
        </div>
        <div className="flex items-center gap-1.5">
          <svg width="24" height="8"><line x1="0" y1="4" x2="24" y2="4" stroke="#8b5cf6" strokeWidth="2" strokeDasharray="6,3" /></svg> Data Contract
        </div>
        <div className="flex items-center gap-1.5">
          <span className="inline-block w-3 h-3 rounded border-2 border-yellow-400 bg-yellow-50" /> Search match
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// 9. Connectors View (with migration info)
// ---------------------------------------------------------------------------

function ConnectorsView() {
  const [connectors, setConnectors] = useState([]);
  const [migrations, setMigrations] = useState([]);
  const [generating, setGenerating] = useState(false);
  const [form, setForm] = useState({ type: "source", name: "", params: "" });

  useEffect(() => {
    api("GET", "/api/connectors").then(setConnectors).catch(console.error);
    api("GET", "/api/connector-migrations").then(setMigrations).catch(console.error);
  }, []);

  async function generate() {
    setGenerating(true);
    try {
      let params = {};
      try {
        params = JSON.parse(form.params);
      } catch {}
      const result = await api("POST", "/api/connectors/generate", {
        connector_type: form.type,
        db_type: form.name,
        params: params,
      });
      window.alert(`Connector generated! ID: ${result.connector_id}. Check Approvals to approve it.`);
      const updated = await api("GET", "/api/connectors");
      setConnectors(updated);
    } catch (e) {
      window.alert(`Error: ${e.message}`);
    } finally {
      setGenerating(false);
    }
  }

  const statusColor = (s) =>
    ({ active: "green", approved: "blue", draft: "purple", deprecated: "gray", failed: "red" }[s] || "gray");

  return (
    <div className="px-6 py-4">
      <h1 className="text-lg font-semibold mb-4 text-slate-800">Connectors</h1>

      <div className="bg-blue-50 border border-blue-200 rounded-xl p-4 mb-6">
        <div className="text-sm font-medium mb-3 text-blue-700">Generate New Connector</div>
        <div className="flex gap-3 items-end flex-wrap">
          <div>
            <div className="text-xs text-slate-400 mb-1">Type</div>
            <select
              value={form.type}
              onChange={(e) => setForm((f) => ({ ...f, type: e.target.value }))}
              className="px-3 py-1.5 border border-slate-300 rounded-lg text-sm bg-slate-100 text-slate-700"
            >
              <option value="source">Source</option>
              <option value="target">Target</option>
            </select>
          </div>
          <div className="flex-1 min-w-[150px]">
            <div className="text-xs text-slate-400 mb-1">Database type (e.g. postgres, mongodb, snowflake)</div>
            <input
              value={form.name}
              onChange={(e) => setForm((f) => ({ ...f, name: e.target.value }))}
              placeholder="postgres"
              className="w-full px-3 py-1.5 border border-slate-300 rounded-lg text-sm bg-slate-100 text-slate-700"
            />
          </div>
          <div className="flex-1 min-w-[150px]">
            <div className="text-xs text-slate-400 mb-1">Connection params (JSON, optional)</div>
            <input
              value={form.params}
              onChange={(e) => setForm((f) => ({ ...f, params: e.target.value }))}
              placeholder='{"host": "localhost"}'
              className="w-full px-3 py-1.5 border border-slate-300 rounded-lg text-sm font-mono bg-slate-100 text-slate-700"
            />
          </div>
          <button
            onClick={generate}
            disabled={!form.name || generating}
            className="px-4 py-1.5 bg-blue-600 text-white rounded-lg text-sm hover:bg-blue-700 disabled:opacity-50"
          >
            {generating ? "Generating..." : "Generate"}
          </button>
        </div>
      </div>

      <div className="space-y-2">
        {connectors.map((c) => (
          <div
            key={c.connector_id}
            className={`bg-white border rounded-xl px-4 py-3 ${
              c.status === "draft" ? "border-purple-200 bg-purple-50" : "border-slate-200"
            }`}
          >
            <div className="flex items-center gap-3">
              <span className="font-mono text-sm font-medium flex-1 text-slate-700">{c.connector_name}</span>
              <Pill label={c.connector_type} color="blue" />
              <Pill label={c.source_target_type} color="gray" />
              <Pill label={c.status} color={statusColor(c.status)} />
              {c.test_status && (
                <Pill
                  label={c.test_status}
                  color={c.test_status === "passed" ? "green" : c.test_status === "failed" ? "red" : "gray"}
                />
              )}
              <span className="text-xs text-slate-400">{c.generated_by}</span>
            </div>
            {c.status === "draft" && (
              <div className="mt-2 text-xs text-purple-600 bg-purple-50 border border-purple-200 rounded px-2 py-1">
                Awaiting approval -- go to Approvals to review and approve this connector.
              </div>
            )}
          </div>
        ))}
        {connectors.length === 0 && (
          <div className="text-sm text-slate-400 py-8 text-center">
            No connectors yet. Generate one above or use the seed connectors.
          </div>
        )}
      </div>

      {migrations.length > 0 && (
        <div className="mt-8">
          <h2 className="text-sm font-semibold text-slate-500 mb-3">Recent Migrations</h2>
          <div className="bg-white border border-slate-200 rounded-xl overflow-hidden">
            <div className="divide-y divide-slate-200">
              {migrations.slice(0, 10).map((m) => (
                <div key={m.migration_id} className="flex items-center gap-3 px-4 py-2.5 text-xs">
                  <StatusDot status={m.status} />
                  <span className="font-mono text-slate-600">
                    v{m.from_version} -&gt; v{m.to_version}
                  </span>
                  <Pill label={m.migration_type} color="blue" />
                  <Pill label={m.status} color={m.status === "complete" ? "green" : m.status === "failed" ? "red" : "gray"} />
                  <span className="text-slate-400">{m.started_at?.slice(0, 16)}</span>
                  {m.rollback_available && <Pill label="rollback available" color="amber" />}
                </div>
              ))}
            </div>
          </div>
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// 9. Alerts View
// ---------------------------------------------------------------------------

function AlertCard({ a, onAck }) {
  const [expanded, setExpanded] = useState(false);
  const sevColor = (s) => ({ critical: "red", warning: "amber", info: "blue" }[s] || "gray");
  const detail = a.detail || {};
  const hasDetail = Object.keys(detail).length > 0;

  // Determine alert category from summary/detail
  const isFreshness = a.summary?.toLowerCase().includes("freshness") || detail.staleness_minutes != null;
  const isDrift = a.summary?.toLowerCase().includes("schema") || detail.added_columns || detail.removed_columns;
  const isContract = a.summary?.toLowerCase().includes("contract") || detail.contract_id;

  const timeSince = (iso) => {
    if (!iso) return "";
    const mins = Math.round((Date.now() - new Date(iso).getTime()) / 60000);
    if (mins < 60) return `${mins}m ago`;
    if (mins < 1440) return `${Math.round(mins / 60)}h ago`;
    return `${Math.round(mins / 1440)}d ago`;
  };

  return (
    <div className={`border rounded-xl overflow-hidden transition-colors ${
      a.severity === "critical" && !a.acknowledged ? "border-red-300 bg-red-50/70" :
      a.severity === "warning" && !a.acknowledged ? "border-amber-200 bg-amber-50/30" :
      "border-slate-200 bg-white"
    } ${a.acknowledged ? "opacity-60" : ""}`}>
      <div
        className="flex items-center gap-3 px-4 py-3 cursor-pointer hover:bg-white/50"
        onClick={() => setExpanded(!expanded)}
      >
        <StatusDot status={a.severity} />
        <TierBadge tier={a.tier} />
        <span className="font-mono text-sm flex-1 text-slate-700">{a.pipeline_name}</span>
        <Pill label={a.severity} color={sevColor(a.severity)} />
        {isFreshness && <Pill label="freshness" color="blue" />}
        {isDrift && <Pill label="schema" color="purple" />}
        {isContract && <Pill label="contract" color="amber" />}
        <span className="text-xs text-slate-400">{timeSince(a.created_at)}</span>
        {!a.acknowledged && (
          <button
            onClick={(e) => { e.stopPropagation(); onAck(a.alert_id); }}
            className="text-xs px-2.5 py-1 border border-slate-300 text-slate-500 rounded-lg hover:bg-slate-100 font-medium"
          >
            Ack
          </button>
        )}
        {a.acknowledged && <span className="text-xs text-green-600 font-medium">acked</span>}
        <span className="text-slate-300 text-xs">{expanded ? "\u25B2" : "\u25BC"}</span>
      </div>

      {/* Summary always visible below header */}
      <div className="px-4 pb-2 -mt-1">
        <p className="text-xs text-slate-500 ml-8">{a.summary}</p>
      </div>

      {expanded && (
        <div className="px-4 pb-4 pt-1 border-t border-slate-100 space-y-3">
          {/* Freshness detail */}
          {isFreshness && detail.staleness_minutes != null && (
            <div className="bg-white border border-slate-200 rounded-lg px-3 py-2">
              <div className="text-[10px] uppercase text-slate-400 font-semibold mb-2">Freshness Detail</div>
              <div className="grid grid-cols-3 gap-3 text-xs">
                <div>
                  <span className="text-slate-400 block">Staleness</span>
                  <span className="font-mono text-red-600 font-medium">{detail.staleness_minutes?.toFixed(0)}m</span>
                </div>
                <div>
                  <span className="text-slate-400 block">Warn SLA</span>
                  <span className="font-mono text-slate-700">{detail.sla_warn_minutes}m</span>
                </div>
                <div>
                  <span className="text-slate-400 block">Critical SLA</span>
                  <span className="font-mono text-slate-700">{detail.sla_critical_minutes}m</span>
                </div>
              </div>
              {detail.staleness_minutes > 0 && detail.sla_warn_minutes > 0 && (
                <div className="mt-2">
                  <ProgressBar
                    pct={Math.min(100, (detail.staleness_minutes / (detail.sla_critical_minutes || detail.sla_warn_minutes * 3)) * 100)}
                    color={detail.staleness_minutes > (detail.sla_critical_minutes || 999) ? "red" : "amber"}
                  />
                </div>
              )}
            </div>
          )}

          {/* Schema drift detail */}
          {isDrift && (
            <div className="bg-white border border-slate-200 rounded-lg px-3 py-2">
              <div className="text-[10px] uppercase text-slate-400 font-semibold mb-2">Schema Drift Detail</div>
              {detail.added_columns && detail.added_columns.length > 0 && (
                <div className="mb-1">
                  <span className="text-xs text-green-600 font-medium">Added: </span>
                  <span className="text-xs font-mono text-slate-600">{detail.added_columns.join(", ")}</span>
                </div>
              )}
              {detail.removed_columns && detail.removed_columns.length > 0 && (
                <div className="mb-1">
                  <span className="text-xs text-red-600 font-medium">Removed: </span>
                  <span className="text-xs font-mono text-slate-600">{detail.removed_columns.join(", ")}</span>
                </div>
              )}
              {detail.type_changes && detail.type_changes.length > 0 && (
                <div className="mb-1">
                  <span className="text-xs text-amber-600 font-medium">Type changes: </span>
                  <span className="text-xs font-mono text-slate-600">{detail.type_changes.map((c) => typeof c === "string" ? c : `${c.column}: ${c.from} → ${c.to}`).join(", ")}</span>
                </div>
              )}
              {!detail.added_columns && !detail.removed_columns && !detail.type_changes && (
                <pre className="text-xs font-mono text-slate-500 overflow-x-auto">{JSON.stringify(detail, null, 2)}</pre>
              )}
            </div>
          )}

          {/* Contract violation detail */}
          {isContract && (
            <div className="bg-white border border-slate-200 rounded-lg px-3 py-2">
              <div className="text-[10px] uppercase text-slate-400 font-semibold mb-2">Contract Violation</div>
              <div className="grid grid-cols-2 gap-3 text-xs">
                {detail.contract_id && (
                  <div>
                    <span className="text-slate-400 block">Contract</span>
                    <span className="font-mono text-slate-700">{detail.contract_id.slice(0, 12)}</span>
                  </div>
                )}
                {detail.consumer && (
                  <div>
                    <span className="text-slate-400 block">Consumer</span>
                    <span className="font-mono text-slate-700">{detail.consumer}</span>
                  </div>
                )}
                {detail.violation_type && (
                  <div>
                    <span className="text-slate-400 block">Violation</span>
                    <Pill label={detail.violation_type} color="red" />
                  </div>
                )}
              </div>
            </div>
          )}

          {/* Generic detail fallback */}
          {hasDetail && !isFreshness && !isDrift && !isContract && (
            <div className="bg-white border border-slate-200 rounded-lg px-3 py-2">
              <div className="text-[10px] uppercase text-slate-400 font-semibold mb-1">Detail</div>
              <pre className="text-xs font-mono text-slate-500 overflow-x-auto">{JSON.stringify(detail, null, 2)}</pre>
            </div>
          )}

          {/* Ack info */}
          {a.acknowledged && (
            <div className="text-xs text-slate-400">
              Acknowledged{a.acknowledged_by ? ` by ${a.acknowledged_by}` : ""}{a.acknowledged_at ? ` at ${a.acknowledged_at.replace("T", " ").slice(0, 19)}` : ""}
            </div>
          )}

          {/* Metadata */}
          <div className="text-[10px] font-mono text-slate-400">
            Alert: {a.alert_id?.slice(0, 12)} &middot; Pipeline: {a.pipeline_id?.slice(0, 12)} &middot; {a.created_at?.replace("T", " ").slice(0, 19)}
          </div>
        </div>
      )}
    </div>
  );
}

function AlertsView({ tierFilter, searchQuery }) {
  const [alerts, setAlerts] = useState([]);
  const [sevFilter, setSevFilter] = useState("all");
  const [ackFilter, setAckFilter] = useState("unacked");
  useEffect(() => {
    const tierParam = tierFilter !== "All" ? `&tier=${tierFilter[1]}` : "";
    api("GET", `/api/observability/alerts?hours=168${tierParam}`).then(setAlerts).catch(console.error);
  }, [tierFilter]);

  async function ack(id) {
    await api("POST", `/api/observability/alerts/${id}/acknowledge`);
    setAlerts((a) => a.map((x) => (x.alert_id === id ? { ...x, acknowledged: true, acknowledged_at: new Date().toISOString() } : x)));
  }

  const searched = searchQuery
    ? alerts.filter((a) => a.pipeline_name?.toLowerCase().includes(searchQuery) || (a.summary || "").toLowerCase().includes(searchQuery))
    : alerts;
  const sevFiltered = sevFilter === "all" ? searched : searched.filter((a) => a.severity === sevFilter);
  const filtered = ackFilter === "all" ? sevFiltered
    : ackFilter === "unacked" ? sevFiltered.filter((a) => !a.acknowledged)
    : sevFiltered.filter((a) => a.acknowledged);

  const critCount = alerts.filter((a) => a.severity === "critical" && !a.acknowledged).length;
  const warnCount = alerts.filter((a) => a.severity === "warning" && !a.acknowledged).length;

  return (
    <div className="px-6 py-4">
      <div className="flex items-center justify-between mb-4">
        <div className="flex items-center gap-3">
          <h1 className="text-lg font-semibold text-slate-800">Alerts</h1>
          {critCount > 0 && (
            <span className="text-xs px-2.5 py-1 bg-red-100 text-red-700 rounded-full font-medium">{critCount} critical</span>
          )}
          {warnCount > 0 && (
            <span className="text-xs px-2.5 py-1 bg-amber-100 text-amber-700 rounded-full font-medium">{warnCount} warning</span>
          )}
        </div>
        <div className="flex gap-1">
          {[["all", "All"], ["critical", "Critical"], ["warning", "Warning"], ["info", "Info"]].map(([val, label]) => (
            <button key={val} onClick={() => setSevFilter(val)}
              className={`px-3 py-1 rounded-lg text-xs font-medium transition-colors ${
                sevFilter === val ? "bg-slate-800 text-white" : "bg-slate-100 text-slate-500 hover:bg-slate-200"
              }`}
            >{label}</button>
          ))}
          <span className="w-px bg-slate-200 mx-1" />
          {[["unacked", "Open"], ["acked", "Acked"], ["all", "All"]].map(([val, label]) => (
            <button key={val} onClick={() => setAckFilter(val)}
              className={`px-3 py-1 rounded-lg text-xs font-medium transition-colors ${
                ackFilter === val ? "bg-slate-800 text-white" : "bg-slate-100 text-slate-500 hover:bg-slate-200"
              }`}
            >{label}</button>
          ))}
        </div>
      </div>
      <div className="space-y-2">
        {filtered.map((a) => <AlertCard key={a.alert_id} a={a} onAck={ack} />)}
        {filtered.length === 0 && (
          <div className="text-sm text-slate-400 py-8 text-center bg-green-50 border border-green-200 rounded-xl">
            {alerts.length === 0 ? "No alerts in the last 7 days." : "No alerts match the current filters."}
          </div>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// 10. Costs View
// ---------------------------------------------------------------------------

function CostsView() {
  const [costs, setCosts] = useState([]);
  const [summary, setSummary] = useState(null);
  const [hours, setHours] = useState(24);

  useEffect(() => {
    api("GET", `/api/agent-costs?hours=${hours}`).then(setCosts).catch(console.error);
    api("GET", "/api/agent-costs/summary").then(setSummary).catch(console.error);
  }, [hours]);

  return (
    <div className="px-6 py-4">
      <h1 className="text-lg font-semibold mb-4 text-slate-800">Agent Costs</h1>

      {summary && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
          {Object.entries(summary).map(([key, val]) => (
            <div key={key} className="bg-white border border-slate-200 rounded-xl px-4 py-3">
              <div className="text-xs text-slate-400 mb-1">{key.replace(/_/g, " ")}</div>
              <div className="text-lg font-semibold font-mono text-slate-700">
                {typeof val === "number" ? (key.includes("cost") || key.includes("usd") ? `$${val.toFixed(4)}` : val.toLocaleString()) : String(val)}
              </div>
            </div>
          ))}
        </div>
      )}

      <div className="flex items-center gap-3 mb-4">
        <span className="text-sm text-slate-400">Time range:</span>
        {[24, 48, 168, 720].map((h) => (
          <button
            key={h}
            onClick={() => setHours(h)}
            className={`text-xs px-3 py-1 rounded ${
              hours === h ? "bg-blue-600 text-white" : "bg-slate-100 text-slate-500 hover:bg-slate-200"
            }`}
          >
            {h <= 48 ? `${h}h` : `${Math.round(h / 24)}d`}
          </button>
        ))}
      </div>

      <div className="bg-white border border-slate-200 rounded-xl overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="text-slate-400 border-b border-slate-200 text-xs">
              <th className="text-left py-2.5 px-4">Time</th>
              <th className="text-left py-2.5 px-4">Pipeline</th>
              <th className="text-left py-2.5 px-4">Operation</th>
              <th className="text-left py-2.5 px-4">Model</th>
              <th className="text-right py-2.5 px-4">Input</th>
              <th className="text-right py-2.5 px-4">Output</th>
              <th className="text-right py-2.5 px-4">Cost</th>
            </tr>
          </thead>
          <tbody>
            {costs.map((c) => (
              <tr key={c.cost_id} className="border-b border-slate-200/50 hover:bg-slate-50/50">
                <td className="py-2 px-4 text-xs font-mono text-slate-400">{c.created_at?.slice(0, 16)}</td>
                <td className="py-2 px-4 text-xs font-mono text-slate-600">{c.pipeline_id?.slice(0, 8) || "--"}</td>
                <td className="py-2 px-4">
                  <Pill label={c.operation} color="blue" />
                </td>
                <td className="py-2 px-4 text-xs text-slate-500">{c.model}</td>
                <td className="py-2 px-4 text-xs font-mono text-slate-500 text-right">
                  {c.input_tokens?.toLocaleString()}
                </td>
                <td className="py-2 px-4 text-xs font-mono text-slate-500 text-right">
                  {c.output_tokens?.toLocaleString()}
                </td>
                <td className="py-2 px-4 text-xs font-mono text-slate-700 text-right">${c.cost_usd?.toFixed(4)}</td>
              </tr>
            ))}
          </tbody>
        </table>
        {costs.length === 0 && (
          <div className="text-sm text-slate-400 py-8 text-center">No agent cost data in this time range.</div>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Settings View — Branding + Agent Costs
// ---------------------------------------------------------------------------

function SettingsView({ branding, onBrandingChange }) {
  const [tab, setTab] = useState("usage");
  const tabs = [
    { id: "usage", label: "Usage" },
    { id: "branding", label: "Branding" },
    { id: "costs", label: "Agent Costs" },
  ];

  return (
    <div className="px-6 py-4">
      <h1 className="text-lg font-semibold mb-4 text-slate-800">Settings</h1>
      <div className="flex gap-1 mb-6">
        {tabs.map((t) => (
          <button
            key={t.id}
            onClick={() => setTab(t.id)}
            className={`text-xs px-4 py-1.5 rounded-lg font-medium transition-colors ${
              tab === t.id
                ? "bg-blue-600 text-white"
                : "bg-slate-100 text-slate-500 hover:bg-slate-200"
            }`}
          >
            {t.label}
          </button>
        ))}
      </div>
      {tab === "usage" && <AnalyticsView />}
      {tab === "branding" && <BrandingSettings branding={branding} onBrandingChange={onBrandingChange} />}
      {tab === "costs" && <CostsView />}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Analytics — GitHub-style contribution heatmap + usage breakdown
// ---------------------------------------------------------------------------
// GitHub-style contribution heatmap (full year, month labels)
// ---------------------------------------------------------------------------

function ContributionGraph({ heatmap, chatActivity }) {
  // Merge runs + chats into one map keyed by date
  const runMap = {};
  const chatMap = {};
  (heatmap || []).forEach((d) => { runMap[d.day] = d.runs || 0; });
  (chatActivity || []).forEach((d) => { chatMap[d.day] = d.chats || 0; });

  // Generate full year of days (52 weeks + partial)
  const allDays = [];
  const now = new Date();
  const totalDays = 371; // ~53 weeks
  for (let i = totalDays - 1; i >= 0; i--) {
    const d = new Date(now);
    d.setDate(d.getDate() - i);
    const key = d.toISOString().slice(0, 10);
    allDays.push({ date: d, key, runs: runMap[key] || 0, chats: chatMap[key] || 0, total: (runMap[key] || 0) + (chatMap[key] || 0) });
  }

  // Group into weeks (Sunday = start)
  const weeks = [];
  let week = [];
  allDays.forEach((d) => {
    if (d.date.getDay() === 0 && week.length > 0) {
      weeks.push(week);
      week = [];
    }
    week.push(d);
  });
  if (week.length > 0) weeks.push(week);

  const maxVal = Math.max(1, ...allDays.map((d) => d.total));
  const totalContribs = allDays.reduce((s, d) => s + d.total, 0);
  const MONTHS = ["Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"];

  // Find month label positions
  const monthLabels = [];
  let lastMonth = -1;
  weeks.forEach((w, wi) => {
    const firstDay = w[0];
    const m = firstDay.date.getMonth();
    if (m !== lastMonth) {
      monthLabels.push({ month: MONTHS[m], col: wi });
      lastMonth = m;
    }
  });

  const greenScale = (v) => {
    if (v === 0) return "#ebedf0";
    const t = v / maxVal;
    if (t <= 0.25) return "#9be9a8";
    if (t <= 0.5) return "#40c463";
    if (t <= 0.75) return "#30a14e";
    return "#216e39";
  };

  return (
    <div className="bg-white border border-slate-200 rounded-xl p-5">
      <div className="flex items-center justify-between mb-3">
        <div className="text-sm text-slate-700">
          <span className="font-semibold">{totalContribs} contributions</span>
          <span className="text-slate-400 ml-1">in the last year</span>
        </div>
      </div>

      {/* Month labels */}
      <div className="overflow-x-auto">
        <div style={{ minWidth: weeks.length * 13 + 30 }}>
          <div className="flex ml-[30px] mb-1">
            {monthLabels.map((m, i) => (
              <div key={i} className="text-[10px] text-slate-400" style={{ position: "relative", left: m.col * 13 - (i > 0 ? monthLabels[i - 1].col * 13 + (MONTHS[monthLabels[i - 1]?.month] || "").length * 5 : 0) }}>
                {m.month}
              </div>
            ))}
          </div>

          {/* Grid */}
          <div className="flex gap-[2px]">
            {/* Day labels */}
            <div className="flex flex-col gap-[2px] mr-1 shrink-0" style={{ width: 24 }}>
              {["", "Mon", "", "Wed", "", "Fri", ""].map((l, i) => (
                <div key={i} className="h-[11px] text-[9px] text-slate-400 leading-[11px]">{l}</div>
              ))}
            </div>
            {/* Weeks */}
            {weeks.map((w, wi) => (
              <div key={wi} className="flex flex-col gap-[2px]">
                {[0, 1, 2, 3, 4, 5, 6].map((dow) => {
                  const cell = w.find((d) => d.date.getDay() === dow);
                  if (!cell) return <div key={dow} className="w-[11px] h-[11px]" />;
                  return (
                    <div
                      key={dow}
                      className="w-[11px] h-[11px] rounded-sm outline outline-1 outline-slate-200/50"
                      style={{ background: greenScale(cell.total) }}
                      title={`${cell.key}: ${cell.runs} runs, ${cell.chats} chats`}
                    />
                  );
                })}
              </div>
            ))}
          </div>

          {/* Legend */}
          <div className="flex items-center gap-1 mt-2 justify-end">
            <span className="text-[9px] text-slate-400">Less</span>
            {["#ebedf0", "#9be9a8", "#40c463", "#30a14e", "#216e39"].map((c, i) => (
              <div key={i} className="w-[10px] h-[10px] rounded-sm" style={{ background: c }} />
            ))}
            <span className="text-[9px] text-slate-400">More</span>
          </div>
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Activity Timeline — GitHub-style contribution feed
// ---------------------------------------------------------------------------

const ACTIVITY_ICONS = {
  pipeline_run: (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#22c55e" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <polyline points="22 12 18 12 15 21 9 3 6 12 2 12"/>
    </svg>
  ),
  chat: (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#3b82f6" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M21 15a2 2 0 0 1-2 2H7l-4 4V5a2 2 0 0 1 2-2h14a2 2 0 0 1 2 2z"/>
    </svg>
  ),
  connector: (
    <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="#8b5cf6" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M12 2v6m0 8v6M4.93 4.93l4.24 4.24m5.66 5.66 4.24 4.24M2 12h6m8 0h6M4.93 19.07l4.24-4.24m5.66-5.66 4.24-4.24"/>
    </svg>
  ),
};

function ActivityTimeline({ timeline }) {
  if (!timeline || timeline.length === 0) {
    return <div className="text-sm text-slate-400 py-8 text-center">No activity in this period.</div>;
  }

  const fmtAction = (a) => {
    if (!a) return "interacted";
    return a.replace(/_/g, " ").replace(/\b\w/g, (c) => c.toUpperCase());
  };

  return (
    <div>
      <h2 className="text-sm font-semibold text-slate-700 mb-4">Contribution activity</h2>
      <div className="space-y-0">
        {timeline.map((group) => (
          <div key={group.day}>
            {/* Date header with line */}
            <div className="flex items-center gap-3 mb-3 mt-5 first:mt-0">
              <span className="text-xs font-semibold text-slate-600 whitespace-nowrap">
                {new Date(group.day + "T12:00:00").toLocaleDateString("en-US", { month: "long", day: "numeric", year: "numeric" })}
              </span>
              <div className="flex-1 h-px bg-slate-200" />
            </div>

            {/* Events for this day */}
            <div className="ml-2 space-y-2">
              {/* Pipeline runs */}
              {(() => {
                const runs = group.events.filter((e) => e.type === "pipeline_run");
                if (runs.length === 0) return null;
                const totalRuns = runs.reduce((s, r) => s + r.count, 0);
                const pipelineCount = new Set(runs.map((r) => r.pipeline_id)).size;
                return (
                  <div className="flex gap-3">
                    <div className="w-8 h-8 rounded-full bg-green-50 border border-green-200 flex items-center justify-center shrink-0">
                      {ACTIVITY_ICONS.pipeline_run}
                    </div>
                    <div className="flex-1 pt-1">
                      <div className="text-sm text-slate-700">
                        Ran <span className="font-semibold">{totalRuns} pipeline run{totalRuns > 1 ? "s" : ""}</span> across {pipelineCount} pipeline{pipelineCount > 1 ? "s" : ""}
                      </div>
                      <div className="mt-1.5 space-y-1">
                        {runs.map((r) => (
                          <div key={r.pipeline_id} className="flex items-center gap-2 text-xs">
                            <StatusDot status={r.status} />
                            <span className="font-mono text-blue-600">{r.pipeline_name}</span>
                            <span className="text-slate-400">{r.count} run{r.count > 1 ? "s" : ""}</span>
                            {r.count > 1 && (
                              <div className="flex gap-px ml-1">
                                {Array.from({ length: Math.min(r.count, 20) }).map((_, i) => (
                                  <div key={i} className="w-2.5 h-2.5 rounded-sm bg-green-500" />
                                ))}
                              </div>
                            )}
                          </div>
                        ))}
                      </div>
                    </div>
                  </div>
                );
              })()}

              {/* Chat interactions */}
              {(() => {
                const chats = group.events.filter((e) => e.type === "chat");
                if (chats.length === 0) return null;
                const totalChats = chats.reduce((s, c) => s + c.count, 0);
                return (
                  <div className="flex gap-3">
                    <div className="w-8 h-8 rounded-full bg-blue-50 border border-blue-200 flex items-center justify-center shrink-0">
                      {ACTIVITY_ICONS.chat}
                    </div>
                    <div className="flex-1 pt-1">
                      <div className="text-sm text-slate-700">
                        <span className="font-semibold">{totalChats} chat interaction{totalChats > 1 ? "s" : ""}</span> with the agent
                      </div>
                      <div className="mt-1.5 flex flex-wrap gap-1.5">
                        {chats.map((c, i) => (
                          <span key={i} className="text-[10px] px-2 py-0.5 rounded-full bg-blue-50 text-blue-600 border border-blue-200">
                            {fmtAction(c.action)} ({c.count})
                          </span>
                        ))}
                      </div>
                    </div>
                  </div>
                );
              })()}

              {/* Connectors */}
              {(() => {
                const conns = group.events.filter((e) => e.type === "connector");
                if (conns.length === 0) return null;
                return (
                  <div className="flex gap-3">
                    <div className="w-8 h-8 rounded-full bg-violet-50 border border-violet-200 flex items-center justify-center shrink-0">
                      {ACTIVITY_ICONS.connector}
                    </div>
                    <div className="flex-1 pt-1">
                      <div className="text-sm text-slate-700">
                        {conns.length === 1 ? "Generated" : `Generated ${conns.length}`} connector{conns.length > 1 ? "s" : ""}
                      </div>
                      <div className="mt-1.5 space-y-1">
                        {conns.map((c, i) => (
                          <div key={i} className="text-xs flex items-center gap-2">
                            <Pill label={c.connector_type} color="purple" />
                            <span className="font-mono text-slate-600">{c.name}</span>
                            <Pill label={c.status} color="green" />
                          </div>
                        ))}
                      </div>
                    </div>
                  </div>
                );
              })()}
            </div>
          </div>
        ))}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Usage View — GitHub profile-style
// ---------------------------------------------------------------------------

function AnalyticsView() {
  const [data, setData] = useState(null);
  const [timeline, setTimeline] = useState(null);
  const [days, setDays] = useState(365);

  useEffect(() => {
    api("GET", `/api/analytics/activity?days=${days}`).then(setData).catch(console.error);
    api("GET", `/api/analytics/timeline?days=${days}`).then((d) => setTimeline(d.timeline || [])).catch(console.error);
  }, [days]);

  if (!data) return <div className="text-sm text-slate-400 py-8 text-center">Loading...</div>;

  const s = data.summary || {};

  return (
    <div className="space-y-6 max-w-4xl">
      {/* Summary row */}
      <div className="grid grid-cols-3 md:grid-cols-6 gap-3">
        {[
          { label: "Runs", value: s.total_runs?.toLocaleString() || "0" },
          { label: "Chats", value: s.total_chats?.toLocaleString() || "0" },
          { label: "Users", value: s.active_users?.toLocaleString() || "0" },
          { label: "Pipelines", value: s.total_pipelines?.toLocaleString() || "0" },
          { label: "Cost", value: `$${(s.total_cost || 0).toFixed(2)}` },
          { label: "Tokens", value: s.total_tokens >= 1000000 ? `${(s.total_tokens / 1000000).toFixed(1)}M` : s.total_tokens >= 1000 ? `${Math.round(s.total_tokens / 1000)}K` : (s.total_tokens || 0).toLocaleString() },
        ].map((c) => (
          <div key={c.label} className="text-center">
            <div className="text-lg font-semibold font-mono text-slate-700">{c.value}</div>
            <div className="text-[10px] text-slate-400 uppercase tracking-wider">{c.label}</div>
          </div>
        ))}
      </div>

      {/* Contribution heatmap */}
      <ContributionGraph heatmap={data.heatmap} chatActivity={data.chat_activity} />

      {/* Activity timeline */}
      {timeline && <ActivityTimeline timeline={timeline} />}
    </div>
  );
}

function BrandingSettings({ branding, onBrandingChange }) {
  const [appName, setAppName] = useState(branding?.app_name || "DAPOS");
  const [logoUrl, setLogoUrl] = useState(branding?.logo_url || "");
  const [saving, setSaving] = useState(false);
  const [msg, setMsg] = useState("");
  const fileRef = useRef(null);

  useEffect(() => {
    setAppName(branding?.app_name || "DAPOS");
    setLogoUrl(branding?.logo_url || "");
  }, [branding]);

  const handleLogoUpload = (e) => {
    const file = e.target.files?.[0];
    if (!file) return;
    if (file.size > 256 * 1024) {
      setMsg("File too large (max 256KB)");
      return;
    }
    const reader = new FileReader();
    reader.onload = () => setLogoUrl(reader.result);
    reader.readAsDataURL(file);
  };

  const handleSave = async () => {
    setSaving(true);
    setMsg("");
    try {
      if (logoUrl && logoUrl.startsWith("data:image/")) {
        await api("POST", "/api/settings/branding/logo", { data_url: logoUrl });
      }
      await api("PUT", "/api/settings/branding", { app_name: appName, logo_url: logoUrl });
      onBrandingChange({ app_name: appName, logo_url: logoUrl });
      setMsg("Saved");
    } catch (err) {
      setMsg("Error: " + err.message);
    }
    setSaving(false);
  };

  const handleReset = async () => {
    setSaving(true);
    try {
      await api("PUT", "/api/settings/branding", { app_name: "DAPOS", logo_url: "" });
      setAppName("DAPOS");
      setLogoUrl("");
      onBrandingChange({ app_name: "DAPOS", logo_url: "" });
      setMsg("Reset to defaults");
    } catch (err) {
      setMsg("Error: " + err.message);
    }
    setSaving(false);
  };

  return (
    <div className="max-w-lg space-y-6">
      <div className="bg-white border border-slate-200 rounded-xl p-5 space-y-5">
        <div>
          <label className="block text-xs font-medium text-slate-500 mb-2">Application Name</label>
          <input
            value={appName}
            onChange={(e) => setAppName(e.target.value)}
            className="w-full text-sm px-3 py-2 border border-slate-300 rounded-lg bg-slate-50 focus:outline-none focus:border-blue-400 text-slate-700"
            placeholder="DAPOS"
          />
        </div>

        <div>
          <label className="block text-xs font-medium text-slate-500 mb-2">Logo</label>
          <div className="flex items-center gap-4">
            <div className="w-14 h-14 border border-slate-200 rounded-lg flex items-center justify-center bg-slate-950 shrink-0">
              {logoUrl ? (
                <img src={logoUrl} alt="Logo" className="w-10 h-10 object-contain" />
              ) : (
                <DefaultLogo size={32} />
              )}
            </div>
            <div className="flex-1 space-y-2">
              <button
                onClick={() => fileRef.current?.click()}
                className="text-xs px-3 py-1.5 bg-blue-600 text-white rounded-lg hover:bg-blue-700 transition-colors"
              >
                Upload Image
              </button>
              {logoUrl && (
                <button
                  onClick={() => setLogoUrl("")}
                  className="text-xs px-3 py-1.5 bg-slate-100 text-slate-500 rounded-lg hover:bg-slate-200 ml-2"
                >
                  Remove
                </button>
              )}
              <input
                ref={fileRef}
                type="file"
                accept="image/png,image/jpeg,image/svg+xml,image/webp"
                onChange={handleLogoUpload}
                className="hidden"
              />
              <div className="text-[10px] text-slate-400">PNG, JPG, SVG, or WebP. Max 256KB.</div>
            </div>
          </div>
        </div>

        <div className="flex items-center gap-3 pt-2 border-t border-slate-100">
          <button
            onClick={handleSave}
            disabled={saving}
            className="text-xs px-4 py-2 bg-blue-600 text-white rounded-lg hover:bg-blue-700 disabled:opacity-50 font-medium"
          >
            {saving ? "Saving..." : "Save Branding"}
          </button>
          <button
            onClick={handleReset}
            disabled={saving}
            className="text-xs px-4 py-2 bg-slate-100 text-slate-500 rounded-lg hover:bg-slate-200 disabled:opacity-50"
          >
            Reset to Default
          </button>
          {msg && (
            <span className={`text-xs ${msg.startsWith("Error") ? "text-red-500" : "text-green-600"}`}>{msg}</span>
          )}
        </div>
      </div>

      <div className="bg-white border border-slate-200 rounded-xl p-5">
        <div className="text-xs font-medium text-slate-500 mb-3">Preview</div>
        <div className="bg-slate-950 rounded-lg px-4 py-3 flex items-center gap-2.5">
          {logoUrl ? (
            <img src={logoUrl} alt="Preview" className="w-7 h-7 rounded object-contain" />
          ) : (
            <DefaultLogo size={28} />
          )}
          <div>
            <div className="text-sm font-semibold text-white font-ui tracking-wide">{appName || "DAPOS"}</div>
            <div className="text-[10px] text-slate-500">Agentic Data Platform</div>
          </div>
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Metrics View (Build 31)
// ---------------------------------------------------------------------------

function MetricsView() {
  const [metrics, setMetrics] = useState([]);
  const [pipelines, setPipelines] = useState([]);
  const [selectedPipeline, setSelectedPipeline] = useState("");
  const [expanded, setExpanded] = useState(null);
  const [snapshots, setSnapshots] = useState({});
  const [trend, setTrend] = useState({});
  const [suggesting, setSuggesting] = useState(false);
  const [suggestions, setSuggestions] = useState(null);
  const [creating, setCreating] = useState(false);
  const [computing, setComputing] = useState(null);
  const [editing, setEditing] = useState(null);
  const [editForm, setEditForm] = useState({});
  const [saving, setSaving] = useState(false);
  const [showHistory, setShowHistory] = useState(null);
  const [reasoningDetail, setReasoningDetail] = useState({});

  const loadMetrics = useCallback(() => {
    const url = selectedPipeline ? `/api/metrics?pipeline_id=${selectedPipeline}` : "/api/metrics";
    api("GET", url).then((r) => setMetrics(r.metrics || [])).catch(console.error);
  }, [selectedPipeline]);

  useEffect(() => {
    api("GET", "/api/pipelines").then((r) => setPipelines(r.pipelines || [])).catch(console.error);
  }, []);

  useEffect(() => { loadMetrics(); }, [loadMetrics]);

  const handleExpand = (mid) => {
    if (expanded === mid) { setExpanded(null); return; }
    setExpanded(mid);
    api("GET", `/api/metrics/${mid}`).then((r) => {
      setSnapshots((p) => ({ ...p, [mid]: r.snapshots || [] }));
      setReasoningDetail((p) => ({ ...p, [mid]: r.reasoning_history || [] }));
    }).catch(console.error);
    api("GET", `/api/metrics/${mid}/trend`).then((r) => {
      setTrend((p) => ({ ...p, [mid]: r }));
    }).catch(console.error);
  };

  const handleSuggest = async () => {
    if (!selectedPipeline) return;
    setSuggesting(true);
    try {
      const r = await api("POST", `/api/metrics/suggest/${selectedPipeline}`);
      setSuggestions(r.suggestions || []);
    } catch (e) { console.error(e); }
    setSuggesting(false);
  };

  const handleCreateFromSuggestion = async (s) => {
    setCreating(true);
    try {
      await api("POST", "/api/metrics", {
        pipeline_id: selectedPipeline,
        metric_name: s.metric_name,
        description: s.description || s.rationale || "",
        metric_type: s.metric_type || "custom",
        reasoning: s.reasoning || s.rationale || "",
      });
      loadMetrics();
      setSuggestions((prev) => prev.filter((x) => x.metric_name !== s.metric_name));
    } catch (e) { console.error(e); }
    setCreating(false);
  };

  const handleCompute = async (mid) => {
    setComputing(mid);
    try {
      const r = await api("POST", `/api/metrics/${mid}/compute`);
      setSnapshots((p) => ({ ...p, [mid]: [r, ...(p[mid] || [])] }));
    } catch (e) { console.error(e); }
    setComputing(null);
  };

  const handleEdit = (m) => {
    setEditing(m.metric_id);
    setEditForm({
      metric_name: m.metric_name || "",
      description: m.description || "",
      sql_expression: m.sql_expression || "",
      schedule_cron: m.schedule_cron || "",
      enabled: m.enabled !== false,
    });
  };

  const handleSaveEdit = async (mid) => {
    setSaving(true);
    try {
      await api("PATCH", `/api/metrics/${mid}`, editForm);
      setEditing(null);
      loadMetrics();
      // Refresh detail
      handleExpand(mid);
    } catch (e) { console.error(e); }
    setSaving(false);
  };

  const handleToggleEnabled = async (m) => {
    try {
      await api("PATCH", `/api/metrics/${m.metric_id}`, { enabled: !m.enabled });
      loadMetrics();
    } catch (e) { console.error(e); }
  };

  const handleDelete = async (mid) => {
    try {
      await api("DELETE", `/api/metrics/${mid}`);
      loadMetrics();
      if (expanded === mid) setExpanded(null);
    } catch (e) { console.error(e); }
  };

  // Mini sparkline SVG
  const Sparkline = ({ data }) => {
    if (!data || data.length < 2) return null;
    const vals = data.map((d) => d.value);
    const mn = Math.min(...vals), mx = Math.max(...vals);
    const range = mx - mn || 1;
    const w = 120, h = 32;
    const points = vals.map((v, i) => `${(i / (vals.length - 1)) * w},${h - ((v - mn) / range) * h}`).join(" ");
    return (
      <svg width={w} height={h} className="inline-block ml-2">
        <polyline points={points} fill="none" stroke="#6366f1" strokeWidth="1.5" />
      </svg>
    );
  };

  return (
    <div className="px-6 py-4">
      <h1 className="text-lg font-semibold mb-4 text-slate-800">Metrics & KPIs</h1>

      <div className="flex items-center gap-3 mb-4 flex-wrap">
        <select
          value={selectedPipeline}
          onChange={(e) => setSelectedPipeline(e.target.value)}
          className="text-sm border border-slate-200 rounded-lg px-3 py-1.5 bg-white text-slate-600"
        >
          <option value="">All pipelines</option>
          {pipelines.map((p) => (
            <option key={p.pipeline_id} value={p.pipeline_id}>{p.pipeline_name}</option>
          ))}
        </select>
        {selectedPipeline && (
          <button
            onClick={handleSuggest}
            disabled={suggesting}
            className="text-xs px-3 py-1.5 rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 disabled:opacity-50"
          >
            {suggesting ? "Thinking..." : "Suggest Metrics"}
          </button>
        )}
      </div>

      {suggestions && suggestions.length > 0 && (
        <div className="mb-6 bg-indigo-50 border border-indigo-200 rounded-xl p-4">
          <div className="text-sm font-semibold text-indigo-800 mb-2">Agent Suggestions</div>
          <div className="space-y-2">
            {suggestions.map((s, i) => (
              <div key={i} className="flex items-center justify-between bg-white rounded-lg px-3 py-2 border border-indigo-100">
                <div className="flex-1 min-w-0">
                  <span className="text-sm font-medium text-slate-700">{s.metric_name}</span>
                  <span className="text-xs text-slate-400 ml-2">{s.metric_type || "custom"}</span>
                  {s.description && <div className="text-xs text-slate-500 mt-0.5">{s.description}</div>}
                  {(s.reasoning || s.rationale) && <div className="text-xs text-indigo-500 mt-0.5 italic">{s.reasoning || s.rationale}</div>}
                </div>
                <button
                  onClick={() => handleCreateFromSuggestion(s)}
                  disabled={creating}
                  className="text-xs px-2.5 py-1 rounded bg-indigo-600 text-white hover:bg-indigo-700 disabled:opacity-50"
                >
                  Create
                </button>
              </div>
            ))}
          </div>
        </div>
      )}

      {metrics.length === 0 ? (
        <div className="text-sm text-slate-400 py-8 text-center">
          No metrics defined yet. Select a pipeline and click "Suggest Metrics" to get started.
        </div>
      ) : (
        <div className="space-y-2">
          {metrics.map((m) => (
            <div key={m.metric_id} className="bg-white border border-slate-200 rounded-xl overflow-hidden">
              <div
                className="flex items-center justify-between px-4 py-3 cursor-pointer hover:bg-slate-50/50"
                onClick={() => handleExpand(m.metric_id)}
              >
                <div className="flex items-center gap-3">
                  <span className="text-sm font-medium text-slate-700">{m.metric_name}</span>
                  <span className="text-xs px-2 py-0.5 rounded bg-indigo-50 text-indigo-600 border border-indigo-100">
                    {m.metric_type}
                  </span>
                  {m.schedule_cron && (
                    <span className="text-xs px-1.5 py-0.5 rounded bg-green-50 text-green-600 border border-green-100" title={m.schedule_cron}>auto</span>
                  )}
                  {!m.enabled && (
                    <span className="text-xs px-1.5 py-0.5 rounded bg-slate-100 text-slate-400 border border-slate-200">off</span>
                  )}
                  {m.latest_value !== undefined && m.latest_value !== null && (
                    <span className="text-sm font-mono font-semibold text-indigo-700">{Number(m.latest_value).toLocaleString()}</span>
                  )}
                  {snapshots[m.metric_id] && <Sparkline data={[...(snapshots[m.metric_id] || [])].reverse()} />}
                </div>
                <div className="flex items-center gap-2">
                  <span className="text-xs text-slate-400 font-mono">{m.pipeline_name || m.pipeline_id?.slice(0, 8)}</span>
                  <span className="text-slate-300">{expanded === m.metric_id ? "−" : "+"}</span>
                </div>
              </div>

              {expanded === m.metric_id && (
                <div className="border-t border-slate-200 px-4 py-3 bg-slate-50/30">

                  {editing === m.metric_id ? (
                    <div className="space-y-2 mb-3">
                      <div>
                        <label className="text-xs text-slate-500 block mb-0.5">Name</label>
                        <input value={editForm.metric_name} onChange={(e) => setEditForm((f) => ({ ...f, metric_name: e.target.value }))}
                          className="w-full text-sm border border-slate-200 rounded px-2 py-1" />
                      </div>
                      <div>
                        <label className="text-xs text-slate-500 block mb-0.5">Description</label>
                        <input value={editForm.description} onChange={(e) => setEditForm((f) => ({ ...f, description: e.target.value }))}
                          className="w-full text-sm border border-slate-200 rounded px-2 py-1" />
                      </div>
                      <div>
                        <label className="text-xs text-slate-500 block mb-0.5">SQL Expression</label>
                        <textarea value={editForm.sql_expression} onChange={(e) => setEditForm((f) => ({ ...f, sql_expression: e.target.value }))}
                          className="w-full text-xs font-mono border border-slate-200 rounded px-2 py-1.5 bg-slate-900 text-green-300" rows={3} />
                      </div>
                      <div>
                        <label className="text-xs text-slate-500 block mb-0.5">Schedule (cron)</label>
                        <input value={editForm.schedule_cron} onChange={(e) => setEditForm((f) => ({ ...f, schedule_cron: e.target.value }))}
                          placeholder="e.g. */15 * * * * (every 15 min)" className="w-full text-sm font-mono border border-slate-200 rounded px-2 py-1" />
                        <div className="text-xs text-slate-400 mt-0.5">Leave empty for manual-only computation</div>
                      </div>
                      <div className="flex items-center gap-2">
                        <input type="checkbox" checked={editForm.enabled} onChange={(e) => setEditForm((f) => ({ ...f, enabled: e.target.checked }))} id={`en-${m.metric_id}`} />
                        <label htmlFor={`en-${m.metric_id}`} className="text-xs text-slate-600">Enabled</label>
                      </div>
                      <div className="flex gap-2 pt-1">
                        <button onClick={() => handleSaveEdit(m.metric_id)} disabled={saving}
                          className="text-xs px-3 py-1 rounded bg-indigo-600 text-white hover:bg-indigo-700 disabled:opacity-50">
                          {saving ? "Saving..." : "Save"}
                        </button>
                        <button onClick={() => setEditing(null)}
                          className="text-xs px-3 py-1 rounded bg-slate-100 text-slate-600 hover:bg-slate-200">Cancel</button>
                      </div>
                    </div>
                  ) : (
                    <>
                      <div className="text-xs text-slate-500 mb-2">{m.description}</div>
                      {m.reasoning && (
                        <div className="bg-violet-50 border border-violet-200 rounded-lg p-3 mb-3">
                          <div className="flex items-center justify-between mb-1">
                            <span className="text-xs font-semibold text-violet-800">Agent Reasoning</span>
                            {reasoningDetail[m.metric_id] && reasoningDetail[m.metric_id].length > 1 && (
                              <button
                                onClick={(e) => { e.stopPropagation(); setShowHistory(showHistory === m.metric_id ? null : m.metric_id); }}
                                className="text-xs text-violet-500 hover:text-violet-700 underline"
                              >
                                {showHistory === m.metric_id ? "Hide history" : `History (${reasoningDetail[m.metric_id].length})`}
                              </button>
                            )}
                          </div>
                          <div className="text-xs text-violet-700">{m.reasoning}</div>
                          {showHistory === m.metric_id && reasoningDetail[m.metric_id] && (
                            <div className="mt-2 border-t border-violet-200 pt-2 space-y-2">
                              {reasoningDetail[m.metric_id].slice().reverse().map((h, i) => (
                                <div key={i} className="text-xs border-l-2 border-violet-300 pl-2">
                                  <div className="flex items-center gap-2 text-violet-400 mb-0.5">
                                    <span className="font-mono">{(h.at || "").slice(0, 16)}</span>
                                    <span className="px-1.5 py-0.5 rounded bg-violet-100 text-violet-600">{h.trigger}</span>
                                    {h.by && <span>by {h.by}</span>}
                                  </div>
                                  <div className="text-violet-600">{h.reasoning}</div>
                                  {h.change_summary && <div className="text-violet-400 italic mt-0.5">Changed: {h.change_summary}</div>}
                                </div>
                              ))}
                            </div>
                          )}
                        </div>
                      )}
                      {m.sql_expression && (
                        <pre className="text-xs bg-slate-900 text-green-300 rounded-lg p-3 mb-3 overflow-x-auto font-mono">{m.sql_expression}</pre>
                      )}
                      {m.schedule_cron && (
                        <div className="text-xs text-slate-500 mb-2">
                          <span className="text-slate-400">Schedule:</span>{" "}
                          <span className="font-mono text-indigo-600">{m.schedule_cron}</span>
                        </div>
                      )}
                      {!m.enabled && (
                        <div className="text-xs text-amber-600 mb-2">Disabled — metric will not auto-compute</div>
                      )}
                    </>
                  )}

                  <div className="flex gap-2 mb-3">
                    <button
                      onClick={() => handleCompute(m.metric_id)}
                      disabled={computing === m.metric_id}
                      className="text-xs px-3 py-1 rounded bg-indigo-600 text-white hover:bg-indigo-700 disabled:opacity-50"
                    >
                      {computing === m.metric_id ? "Computing..." : "Compute Now"}
                    </button>
                    <button onClick={() => handleEdit(m)}
                      className="text-xs px-3 py-1 rounded bg-slate-100 text-slate-600 border border-slate-200 hover:bg-slate-200">
                      Edit
                    </button>
                    <button onClick={() => handleToggleEnabled(m)}
                      className={`text-xs px-3 py-1 rounded border ${m.enabled ? "bg-amber-50 text-amber-600 border-amber-200 hover:bg-amber-100" : "bg-green-50 text-green-600 border-green-200 hover:bg-green-100"}`}>
                      {m.enabled ? "Disable" : "Enable"}
                    </button>
                    <button
                      onClick={() => handleDelete(m.metric_id)}
                      className="text-xs px-3 py-1 rounded bg-red-50 text-red-600 border border-red-200 hover:bg-red-100"
                    >
                      Delete
                    </button>
                  </div>

                  {trend[m.metric_id] && trend[m.metric_id].narrative && (
                    <div className="bg-indigo-50 border border-indigo-200 rounded-lg p-3 mb-3">
                      <div className="text-xs font-semibold text-indigo-800 mb-1">Agent Trend Analysis</div>
                      <div className="text-xs text-indigo-700">{trend[m.metric_id].narrative}</div>
                      {trend[m.metric_id].direction && (
                        <span className="inline-block text-xs mt-1 px-2 py-0.5 rounded bg-indigo-100 text-indigo-600">
                          {trend[m.metric_id].direction}
                        </span>
                      )}
                    </div>
                  )}

                  {snapshots[m.metric_id] && snapshots[m.metric_id].length > 0 && (
                    <div>
                      <div className="text-xs font-semibold text-slate-500 mb-1">Recent Snapshots</div>
                      <table className="w-full text-xs">
                        <thead>
                          <tr className="text-slate-400 border-b border-slate-200">
                            <th className="text-left py-1.5 px-2">Time</th>
                            <th className="text-right py-1.5 px-2">Value</th>
                          </tr>
                        </thead>
                        <tbody>
                          {snapshots[m.metric_id].slice(0, 10).map((s) => (
                            <tr key={s.snapshot_id} className="border-b border-slate-200/50">
                              <td className="py-1 px-2 font-mono text-slate-400">{s.computed_at?.slice(0, 16)}</td>
                              <td className="py-1 px-2 font-mono text-slate-700 text-right">{Number(s.value).toLocaleString()}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  )}
                </div>
              )}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Docs View
// ---------------------------------------------------------------------------

function simpleMarkdown(md) {
  // Minimal markdown to HTML: headings, bold, italic, code, links, lists, tables, hr
  let html = md
    // Code blocks (``` ... ```)
    .replace(/```(\w*)\n([\s\S]*?)```/g, (_, lang, code) =>
      `<pre class="bg-slate-900 text-green-300 p-4 rounded-lg overflow-x-auto text-xs font-mono my-3"><code>${code.replace(/</g, "&lt;").replace(/>/g, "&gt;")}</code></pre>`)
    // Inline code
    .replace(/`([^`]+)`/g, '<code class="bg-slate-100 text-slate-700 px-1.5 py-0.5 rounded text-xs font-mono">$1</code>')
    // Headers
    .replace(/^#### (.+)$/gm, '<h4 class="text-sm font-semibold text-slate-800 mt-5 mb-2">$1</h4>')
    .replace(/^### (.+)$/gm, '<h3 class="text-base font-semibold text-slate-800 mt-6 mb-2">$1</h3>')
    .replace(/^## (.+)$/gm, '<h2 class="text-lg font-semibold text-slate-900 mt-8 mb-3 pb-2 border-b border-slate-200">$1</h2>')
    .replace(/^# (.+)$/gm, '<h1 class="text-2xl font-bold text-slate-900 mb-4">$1</h1>')
    // Bold and italic
    .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
    .replace(/\*(.+?)\*/g, '<em>$1</em>')
    // Links
    .replace(/\[([^\]]+)\]\(([^)]+)\)/g, '<a class="text-blue-600 hover:underline cursor-pointer" data-doc-link="$2">$1</a>')
    // HR
    .replace(/^---$/gm, '<hr class="my-6 border-slate-200" />')
    // Tables
    .replace(/^\|(.+)\|$/gm, (match) => {
      const cells = match.split("|").filter(c => c.trim()).map(c => c.trim());
      if (cells.every(c => /^[-:]+$/.test(c))) return "<!--table-sep-->";
      return "<tr>" + cells.map(c => `<td class="px-3 py-2 text-xs border border-slate-200">${c}</td>`).join("") + "</tr>";
    });
  // Wrap table rows
  html = html.replace(/((<tr>.*<\/tr>\n?)+)/g, (block) => {
    const cleaned = block.replace(/<!--table-sep-->\n?/g, "");
    return `<table class="w-full border-collapse my-4 text-sm">${cleaned}</table>`;
  });
  // Unordered lists
  html = html.replace(/^- (.+)$/gm, '<li class="ml-4 text-sm text-slate-600 list-disc">$1</li>');
  html = html.replace(/((<li.*<\/li>\n?)+)/g, '<ul class="my-2 space-y-1">$1</ul>');
  // Paragraphs (lines that aren't already HTML)
  html = html.replace(/^(?!<[a-z/!]|<!--)(.+)$/gm, '<p class="text-sm text-slate-600 my-2 leading-relaxed">$1</p>');
  return html;
}

// ---------------------------------------------------------------------------
// Agent Knowledge View (Build 32)
// ---------------------------------------------------------------------------
function AgentKnowledgeView() {
  const [tab, setTab] = useState("knowledge"); // knowledge | prompt | kpis
  const [systemPrompt, setSystemPrompt] = useState("");
  const [bk, setBk] = useState(null);
  const [saving, setSaving] = useState(false);
  const [kpiText, setKpiText] = useState("");
  const [parsing, setParsing] = useState(false);
  const [form, setForm] = useState({});

  useEffect(() => {
    api("GET", "/api/agent/system-prompt")
      .then((r) => setSystemPrompt(r.system_prompt || r.prompt || ""))
      .catch((e) => { console.error("Failed to load system prompt:", e); setSystemPrompt("Error loading system prompt. Check console."); });
    api("GET", "/api/settings/business-knowledge")
      .then((r) => { setBk(r); setForm(r); })
      .catch(console.error);
  }, []);

  const handleSave = async () => {
    setSaving(true);
    try {
      const r = await api("PUT", "/api/settings/business-knowledge", form);
      setBk(r);
      setForm(r);
    } catch (e) { console.error(e); }
    setSaving(false);
  };

  const handleParseKpis = async () => {
    if (!kpiText.trim()) return;
    setParsing(true);
    try {
      const r = await api("POST", "/api/settings/business-knowledge/parse-kpis", { text: kpiText });
      const existing = form.kpi_definitions || [];
      setForm({ ...form, kpi_definitions: [...existing, ...(r.kpi_definitions || [])] });
      setKpiText("");
    } catch (e) { console.error(e); }
    setParsing(false);
  };

  const removeKpi = (idx) => {
    const kpis = [...(form.kpi_definitions || [])];
    kpis.splice(idx, 1);
    setForm({ ...form, kpi_definitions: kpis });
  };

  const addGlossaryTerm = () => {
    const term = prompt("Term name:");
    if (!term) return;
    const def = prompt("Definition:");
    if (!def) return;
    setForm({ ...form, glossary: { ...(form.glossary || {}), [term]: def } });
  };

  const removeGlossaryTerm = (term) => {
    const g = { ...(form.glossary || {}) };
    delete g[term];
    setForm({ ...form, glossary: g });
  };

  const tabs = [
    { id: "knowledge", label: "Business Knowledge" },
    { id: "kpis", label: "KPI Definitions" },
    { id: "prompt", label: "System Prompt" },
  ];

  return (
    <div className="px-6 py-4">
      <h1 className="text-lg font-semibold mb-4 text-slate-800">Agent Knowledge</h1>
      <p className="text-xs text-slate-400 mb-4">
        Configure what the agent knows about your business. This context feeds into all agent reasoning — metric suggestions, quality decisions, failure diagnosis, and more.
      </p>

      <div className="flex gap-1 mb-4 border-b border-slate-200">
        {tabs.map((t) => (
          <button key={t.id} onClick={() => setTab(t.id)}
            className={`text-xs px-3 py-2 font-medium border-b-2 transition ${tab === t.id ? "border-indigo-600 text-indigo-600" : "border-transparent text-slate-400 hover:text-slate-600"}`}>
            {t.label}
          </button>
        ))}
      </div>

      {tab === "knowledge" && (
        <div className="space-y-4 max-w-2xl">
          <div>
            <label className="text-xs font-medium text-slate-500 block mb-1">Company Name</label>
            <input value={form.company_name || ""} onChange={(e) => setForm({ ...form, company_name: e.target.value })}
              className="w-full text-sm border border-slate-200 rounded-lg px-3 py-2 bg-white" placeholder="Acme Corp" />
          </div>
          <div>
            <label className="text-xs font-medium text-slate-500 block mb-1">Industry</label>
            <input value={form.industry || ""} onChange={(e) => setForm({ ...form, industry: e.target.value })}
              className="w-full text-sm border border-slate-200 rounded-lg px-3 py-2 bg-white" placeholder="E-commerce, SaaS, Fintech..." />
          </div>
          <div>
            <label className="text-xs font-medium text-slate-500 block mb-1">Business Description</label>
            <textarea value={form.business_description || ""} onChange={(e) => setForm({ ...form, business_description: e.target.value })}
              rows={3} className="w-full text-sm border border-slate-200 rounded-lg px-3 py-2 bg-white" placeholder="Describe your business, what you do, who your customers are..." />
          </div>
          <div>
            <label className="text-xs font-medium text-slate-500 block mb-1">Datasets Description</label>
            <textarea value={form.datasets_description || ""} onChange={(e) => setForm({ ...form, datasets_description: e.target.value })}
              rows={3} className="w-full text-sm border border-slate-200 rounded-lg px-3 py-2 bg-white" placeholder="Describe your data sources, what each dataset contains, key tables..." />
          </div>
          <div>
            <label className="text-xs font-medium text-slate-500 block mb-1">Custom Agent Instructions</label>
            <textarea value={form.custom_instructions || ""} onChange={(e) => setForm({ ...form, custom_instructions: e.target.value })}
              rows={2} className="w-full text-sm border border-slate-200 rounded-lg px-3 py-2 bg-white" placeholder="Any additional instructions for the agent..." />
          </div>

          <div>
            <div className="flex items-center justify-between mb-2">
              <label className="text-xs font-medium text-slate-500">Business Glossary</label>
              <button onClick={addGlossaryTerm} className="text-xs px-2 py-1 rounded bg-slate-100 text-slate-600 hover:bg-slate-200">+ Add Term</button>
            </div>
            {form.glossary && Object.keys(form.glossary).length > 0 ? (
              <div className="space-y-1">
                {Object.entries(form.glossary).map(([term, def]) => (
                  <div key={term} className="flex items-center justify-between bg-slate-50 rounded-lg px-3 py-1.5 text-xs">
                    <div><span className="font-semibold text-slate-700">{term}</span>: <span className="text-slate-500">{def}</span></div>
                    <button onClick={() => removeGlossaryTerm(term)} className="text-red-400 hover:text-red-600 ml-2">x</button>
                  </div>
                ))}
              </div>
            ) : (
              <div className="text-xs text-slate-400 italic">No glossary terms defined yet.</div>
            )}
          </div>

          <button onClick={handleSave} disabled={saving}
            className="text-xs px-4 py-2 rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 disabled:opacity-50">
            {saving ? "Saving..." : "Save Business Knowledge"}
          </button>
          {bk?.updated_at && <div className="text-xs text-slate-400">Last updated: {bk.updated_at?.slice(0, 16)} by {bk.updated_by || "—"}</div>}
        </div>
      )}

      {tab === "kpis" && (
        <div className="max-w-2xl">
          <p className="text-xs text-slate-400 mb-3">
            Define your business KPIs here. Paste text from documents, Slack, or email — the agent will parse it into structured definitions.
            These KPIs feed into metric suggestions and transform generation.
          </p>

          <div className="mb-4">
            <textarea value={kpiText} onChange={(e) => setKpiText(e.target.value)}
              rows={5} className="w-full text-sm border border-slate-200 rounded-lg px-3 py-2 bg-white font-mono"
              placeholder={"Paste KPI definitions here, e.g.:\n- Monthly Active Users: count of unique users with at least 1 login in 30 days\n- Revenue per customer: total_revenue / active_customers\n- Churn rate: customers lost / total customers * 100"} />
            <button onClick={handleParseKpis} disabled={parsing || !kpiText.trim()}
              className="mt-2 text-xs px-3 py-1.5 rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 disabled:opacity-50">
              {parsing ? "Parsing..." : "Parse with Agent"}
            </button>
          </div>

          <div className="text-xs font-medium text-slate-500 mb-2">Defined KPIs ({(form.kpi_definitions || []).length})</div>
          {(form.kpi_definitions || []).length > 0 ? (
            <div className="space-y-2 mb-4">
              {(form.kpi_definitions || []).map((kpi, i) => (
                <div key={i} className="bg-white border border-slate-200 rounded-lg px-4 py-3">
                  <div className="flex items-center justify-between">
                    <div className="text-sm font-medium text-slate-700">{kpi.name}</div>
                    <button onClick={() => removeKpi(i)} className="text-xs text-red-400 hover:text-red-600">Remove</button>
                  </div>
                  <div className="text-xs text-slate-500 mt-1">{kpi.description}</div>
                  {kpi.formula && <div className="text-xs text-indigo-500 mt-1 font-mono">Formula: {kpi.formula}</div>}
                  {kpi.dimensions && kpi.dimensions.length > 0 && (
                    <div className="text-xs text-slate-400 mt-1">Dimensions: {kpi.dimensions.join(", ")}</div>
                  )}
                  {kpi.unit && <div className="text-xs text-slate-400 mt-1">Unit: {kpi.unit}</div>}
                </div>
              ))}
            </div>
          ) : (
            <div className="text-xs text-slate-400 italic mb-4">No KPI definitions yet. Paste text above and click "Parse with Agent".</div>
          )}

          <button onClick={handleSave} disabled={saving}
            className="text-xs px-4 py-2 rounded-lg bg-indigo-600 text-white hover:bg-indigo-700 disabled:opacity-50">
            {saving ? "Saving..." : "Save KPI Definitions"}
          </button>
        </div>
      )}

      {tab === "prompt" && (
        <div className="max-w-3xl">
          <p className="text-xs text-slate-400 mb-3">
            This is the system prompt the agent uses for all reasoning. It is read-only — configured by the platform.
            Business knowledge (left tab) is appended automatically.
          </p>
          <pre className="text-xs bg-slate-900 text-green-300 rounded-xl p-4 overflow-auto max-h-[70vh] font-mono whitespace-pre-wrap leading-relaxed">
            {systemPrompt || "Loading..."}
          </pre>
        </div>
      )}
    </div>
  );
}

function DocsView() {
  const [docList, setDocList] = useState([]);
  const [currentDoc, setCurrentDoc] = useState(null);
  const [content, setContent] = useState("");
  const [loading, setLoading] = useState(true);
  const [history, setHistory] = useState([]);

  useEffect(() => {
    api("GET", "/api/docs").then(d => {
      setDocList(d.docs || []);
      setLoading(false);
      // Auto-load index
      loadDoc("index.md");
    }).catch(() => setLoading(false));
  }, []);

  function loadDoc(path) {
    if (currentDoc) {
      setHistory(h => [...h, currentDoc]);
    }
    setCurrentDoc(path);
    setContent("");
    api("GET", `/api/docs/${path}`).then(d => {
      setContent(d.content || "");
    }).catch(e => setContent(`Error loading ${path}: ${e.message}`));
  }

  function goBack() {
    if (history.length > 0) {
      const prev = history[history.length - 1];
      setHistory(h => h.slice(0, -1));
      setCurrentDoc(prev);
      api("GET", `/api/docs/${prev}`).then(d => {
        setContent(d.content || "");
      }).catch(() => {});
    }
  }

  // Handle internal doc link clicks
  function handleContentClick(e) {
    const link = e.target.closest("[data-doc-link]");
    if (link) {
      e.preventDefault();
      let docPath = link.getAttribute("data-doc-link");
      // Resolve relative paths
      if (!docPath.startsWith("http")) {
        if (currentDoc && currentDoc.includes("/")) {
          const dir = currentDoc.substring(0, currentDoc.lastIndexOf("/"));
          docPath = dir + "/" + docPath;
        }
        if (!docPath.endsWith(".md")) docPath += ".md";
        loadDoc(docPath);
      }
    }
  }

  // Group docs by section
  const sections = {};
  docList.forEach(d => {
    const s = d.section || "root";
    if (!sections[s]) sections[s] = [];
    sections[s].push(d);
  });

  const sectionLabels = {
    root: "Getting Started",
    concepts: "Concepts",
    agent: "Agent Intelligence",
    advanced: "Advanced",
    contributing: "Contributing",
  };

  return (
    <div className="flex h-full">
      <div className="w-56 border-r border-slate-200 overflow-y-auto bg-slate-50 p-3">
        <div className="text-xs font-semibold text-slate-500 uppercase tracking-wider mb-3">Documentation</div>
        {Object.entries(sections).map(([section, docs]) => (
          <div key={section} className="mb-4">
            <div className="text-xs font-semibold text-slate-400 uppercase tracking-wider mb-1.5 px-2">
              {sectionLabels[section] || section}
            </div>
            {docs.map(d => (
              <button
                key={d.path}
                onClick={() => loadDoc(d.path)}
                className={`w-full text-left text-xs px-2 py-1.5 rounded transition-colors ${
                  currentDoc === d.path
                    ? "bg-blue-50 text-blue-700 font-medium"
                    : "text-slate-600 hover:bg-slate-100 hover:text-slate-800"
                }`}
              >
                {d.title}
              </button>
            ))}
          </div>
        ))}
      </div>
      <div className="flex-1 overflow-y-auto">
        <div className="max-w-3xl mx-auto px-8 py-6">
          {history.length > 0 && (
            <button
              onClick={goBack}
              className="text-xs text-blue-600 hover:text-blue-800 mb-4 flex items-center gap-1"
            >
              &larr; Back
            </button>
          )}
          {loading ? (
            <div className="text-sm text-slate-400">Loading docs...</div>
          ) : content ? (
            <div
              onClick={handleContentClick}
              dangerouslySetInnerHTML={{ __html: simpleMarkdown(content) }}
            />
          ) : (
            <div className="text-sm text-slate-400">Select a document from the sidebar.</div>
          )}
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// App Shell
// ---------------------------------------------------------------------------

function App() {
  const [view, setView] = useState(() => {
    const saved = sessionStorage.getItem("pa_view") || "command";
    return saved === "costs" ? "settings" : saved;
  });
  const [tierFilter, setTierFilter] = useState(() => sessionStorage.getItem("pa_tier") || "All");
  const [searchQuery, setSearchQuery] = useState("");
  const [authState, setAuthState] = useState(() => {
    const token = getToken();
    const userStr = localStorage.getItem("pa_user");
    if (token && userStr) {
      try {
        return { loggedIn: true, user: JSON.parse(userStr) };
      } catch {
        return { loggedIn: false, user: null };
      }
    }
    return { loggedIn: false, user: null };
  });
  const [authEnabled, setAuthEnabled] = useState(null);
  const [guideStep, setGuideStep] = useState(null);
  const [branding, setBranding] = useState({ app_name: "DAPOS", logo_url: "" });

  // Load branding
  useEffect(() => {
    if (authState.loggedIn) {
      api("GET", "/api/settings/branding").then(setBranding).catch(() => {});
    }
  }, [authState.loggedIn]);

  // Show onboarding for users who haven't completed it
  useEffect(() => {
    if (authState.loggedIn && !localStorage.getItem("pa_onboarding_done")) {
      setGuideStep(0);
    }
  }, [authState.loggedIn]);

  // Persist view and tier to sessionStorage
  useEffect(() => { sessionStorage.setItem("pa_view", view); }, [view]);
  useEffect(() => { sessionStorage.setItem("pa_tier", tierFilter); }, [tierFilter]);

  useEffect(() => {
    fetch(API + "/health")
      .then((r) => r.json())
      .then((data) => {
        setAuthEnabled(data.auth_enabled === true);
        if (!data.auth_enabled) {
          setAuthState({ loggedIn: true, user: { user_id: "anonymous", username: "anonymous", role: "admin" } });
          if (!localStorage.getItem("pa_onboarding_done")) {
            setGuideStep(0);
          }
        }
      })
      .catch(() => setAuthEnabled(false));
  }, []);

  function handleLogin(data) {
    setAuthState({ loggedIn: true, user: { user_id: data.user_id, username: data.username, role: data.role } });
    if (!localStorage.getItem("pa_onboarding_done")) {
      setGuideStep(0);
    }
  }

  function handleGuideNav(navId) {
    const idx = GUIDE_ORDER.indexOf(navId);
    if (idx >= 0) setGuideStep(idx);
  }

  function handleGuideFinish() {
    localStorage.setItem("pa_onboarding_done", "1");
    setGuideStep(null);
  }

  function handleLogout() {
    clearToken();
    setAuthState({ loggedIn: false, user: null });
  }

  // Loading state while checking auth
  if (authEnabled === null) {
    return React.createElement("div", { className: "flex items-center justify-center h-screen bg-bg text-text-primary" }, "Loading...");
  }

  // Show login if not authenticated and auth is enabled
  if (!authState.loggedIn && authEnabled) {
    return React.createElement(Login, { onLogin: handleLogin });
  }

  // CommandView is always mounted so chat history survives tab switches.
  // Other views render on demand.
  const sq = searchQuery.toLowerCase();
  const otherViews = {
    pipelines: <PipelinesView tierFilter={tierFilter} searchQuery={sq} />,
    activity: <ActivityView searchQuery={sq} />,
    freshness: <FreshnessView tierFilter={tierFilter} searchQuery={sq} />,
    quality: <QualityView tierFilter={tierFilter} searchQuery={sq} />,
    approvals: <ApprovalsView searchQuery={sq} />,
    dag: <DAGView searchQuery={sq} />,
    connectors: <ConnectorsView />,
    alerts: <AlertsView tierFilter={tierFilter} searchQuery={sq} />,
    metrics: <MetricsView />,
    settings: <SettingsView branding={branding} onBrandingChange={setBranding} />,
    agent: <AgentKnowledgeView />,
    docs: <DocsView />,
  };

  return (
    <div className="flex h-screen overflow-hidden">
      {guideStep !== null && (
        <GuideTooltip
          guideStep={guideStep}
          setView={setView}
          onGuideNav={handleGuideNav}
          onGuideFinish={handleGuideFinish}
        />
      )}
      <Sidebar
        view={view}
        setView={setView}
        tierFilter={tierFilter}
        setTierFilter={setTierFilter}
        searchQuery={searchQuery}
        setSearchQuery={setSearchQuery}
        user={authState.user}
        onLogout={handleLogout}
        guideStep={guideStep}
        onGuideNav={handleGuideNav}
        branding={branding}
      />
      <main className="flex-1 overflow-y-auto bg-slate-50">
        <div style={{ display: view === "command" ? "flex" : "none", flexDirection: "column", height: "100%" }}>
          <CommandView />
        </div>
        {view !== "command" && otherViews[view]}
      </main>
    </div>
  );
}

ReactDOM.createRoot(document.getElementById("root")).render(<App />);

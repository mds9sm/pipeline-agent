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

function TierBadge({ tier }) {
  const colors = {
    1: "bg-red-50 text-red-700 border border-red-200",
    2: "bg-amber-50 text-amber-700 border border-amber-200",
    3: "bg-blue-50 text-blue-700 border border-blue-200",
  };
  return (
    <span className={`text-xs font-mono font-medium px-1.5 py-0.5 rounded ${colors[tier] || "bg-stone-100 text-stone-500 border border-stone-300"}`}>
      T{tier}
    </span>
  );
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
  return <span className={`inline-block w-2 h-2 rounded-full ${map[status] || "bg-stone-400"}`} />;
}

function Pill({ label, color = "blue" }) {
  const colors = {
    blue: "bg-blue-50 text-blue-700 border border-blue-200",
    green: "bg-green-50 text-green-700 border border-green-200",
    amber: "bg-amber-50 text-amber-700 border border-amber-200",
    red: "bg-red-50 text-red-700 border border-red-200",
    purple: "bg-purple-50 text-purple-700 border border-purple-200",
    gray: "bg-stone-100 text-stone-500 border border-stone-200",
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
    <div className="h-1.5 bg-stone-200 rounded-full overflow-hidden">
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
    <div className="border border-stone-200 rounded-lg px-3 py-2">
      <div className="flex items-center gap-2 text-xs flex-wrap">
        <StatusDot status={r.status} />
        <span className="font-mono text-stone-400">{r.started_at?.slice(0, 16)}</span>
        {fmtDuration && <span className="text-stone-400">{fmtDuration}</span>}
        <Pill label={r.run_mode || "scheduled"} color="blue" />
        {r.triggered_by_pipeline_id && (
          <span className="text-[10px] text-stone-400 italic">from {r.triggered_by_pipeline_id?.slice(0, 8)}</span>
        )}
        <span className="text-stone-500">{r.rows_extracted?.toLocaleString()} extracted</span>
        {r.rows_loaded > 0 && <span className="text-stone-500">{r.rows_loaded?.toLocaleString()} loaded</span>}
        {fmtBytes(r.staging_size_bytes) && <span className="text-stone-400">{fmtBytes(r.staging_size_bytes)}</span>}
        <Pill
          label={r.gate_decision || r.status}
          color={r.gate_decision === "halt" ? "red" : r.gate_decision === "promote_with_warning" ? "amber" : "green"}
        />
      </div>
      {(r.watermark_before || r.watermark_after) && (
        <div className="text-xs text-stone-400 mt-1 font-mono">
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
            <div className="mt-1 bg-stone-50 rounded p-2 space-y-0.5">
              {checks.map((c, i) => (
                <div key={i} className="flex items-center gap-2 text-xs">
                  <span className={`w-2 h-2 rounded-full ${
                    c.status === "pass" ? "bg-green-400" :
                    c.status === "warn" ? "bg-amber-400" : "bg-red-400"
                  }`} />
                  <span className="font-medium text-stone-600">{c.name}</span>
                  {c.detail && <span className="text-stone-400 truncate">{c.detail}</span>}
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
    <div className="bg-stone-50 border border-stone-300 rounded-lg px-4 py-3">
      <div className="flex items-center justify-between mb-2">
        <span className="text-xs font-semibold text-stone-500">Error Budget</span>
        <Pill
          label={eb.escalated ? "EXHAUSTED" : `${utilizationPct.toFixed(1)}% used`}
          color={color}
        />
      </div>
      <ProgressBar pct={utilizationPct} color={color} />
      <div className="text-xs text-stone-400 mt-1">
        {eb.successful_runs}/{eb.total_runs} runs successful ({eb.window_days}d window) — threshold {((eb.budget_threshold || 0.9) * 100).toFixed(0)}%
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
    <div className="min-h-screen flex items-center justify-center bg-stone-50">
      <div className="w-96 bg-white border border-stone-200 rounded-xl p-8 shadow-lg">
        <div className="text-center mb-8">
          <div className="text-2xl font-semibold text-stone-800 font-ui">DAPOS</div>
          <div className="text-sm text-stone-400 mt-1">Sign in to continue</div>
        </div>
        <form onSubmit={handleSubmit} className="space-y-4">
          <div>
            <label className="block text-xs text-stone-400 mb-1.5">Username</label>
            <input
              value={username}
              onChange={(e) => setUsername(e.target.value)}
              className="w-full px-4 py-2.5 bg-stone-100 border border-stone-300 rounded-lg text-sm text-stone-700 outline-none focus:border-blue-500"
              autoFocus
            />
          </div>
          <div>
            <label className="block text-xs text-stone-400 mb-1.5">Password</label>
            <input
              type="password"
              value={password}
              onChange={(e) => setPassword(e.target.value)}
              className="w-full px-4 py-2.5 bg-stone-100 border border-stone-300 rounded-lg text-sm text-stone-700 outline-none focus:border-blue-500"
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
// Sidebar
// ---------------------------------------------------------------------------

const NAV = [
  { id: "command", label: "Command", icon: ">" },
  { id: "pipelines", label: "Pipelines", icon: "|" },
  { id: "activity", label: "Activity", icon: "#" },
  { id: "freshness", label: "Freshness", icon: "~" },
  { id: "quality", label: "Quality", icon: "+" },
  { id: "approvals", label: "Approvals", icon: "?" },
  { id: "dag", label: "Lineage", icon: "%" },
  { id: "connectors", label: "Connectors", icon: "@" },
  { id: "alerts", label: "Alerts", icon: "!" },
  { id: "costs", label: "Costs", icon: "$" },
];

function Sidebar({ view, setView, tierFilter, setTierFilter, user, onLogout }) {
  return (
    <div className="w-56 min-h-screen bg-white border-r border-stone-200 flex flex-col">
      <div className="px-5 py-4 border-b border-stone-200">
        <div className="text-sm font-semibold text-stone-800 font-ui">DAPOS</div>
        <div className="text-xs text-stone-400 mt-0.5">Agentic Data Platform</div>
      </div>
      <nav className="flex-1 px-2 py-3 space-y-0.5">
        {NAV.map((n) => (
          <button
            key={n.id}
            onClick={() => setView(n.id)}
            className={`w-full flex items-center gap-2.5 px-3 py-2 rounded-lg text-sm transition-colors ${
              view === n.id
                ? "bg-blue-50 text-blue-700 font-medium"
                : "text-stone-500 hover:bg-stone-100 hover:text-stone-700"
            }`}
          >
            <span className="text-xs font-mono w-4 text-center opacity-60">{n.icon}</span>
            {n.label}
          </button>
        ))}
      </nav>
      <div className="px-3 py-3 border-t border-stone-200">
        <div className="text-xs text-stone-400 mb-2 px-1">Tier filter</div>
        <div className="flex gap-1">
          {["All", "T1", "T2", "T3"].map((t) => (
            <button
              key={t}
              onClick={() => setTierFilter(t)}
              className={`flex-1 text-xs py-1 rounded ${
                tierFilter === t
                  ? "bg-blue-600 text-white"
                  : "bg-stone-100 text-stone-400 hover:bg-stone-200 hover:text-stone-600"
              }`}
            >
              {t}
            </button>
          ))}
        </div>
      </div>
      {user && (
        <div className="px-3 py-3 border-t border-stone-200">
          <div className="flex items-center justify-between">
            <div>
              <div className="text-xs text-stone-600 font-medium">{user.username}</div>
              <div className="text-xs text-stone-300">{user.role}</div>
            </div>
            <button onClick={onLogout} className="text-xs text-stone-400 hover:text-stone-600 px-2 py-1 rounded hover:bg-stone-100">
              Logout
            </button>
          </div>
        </div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// 1. Command View (agent-routed)
// ---------------------------------------------------------------------------

function CommandView() {
  const [messages, setMessages] = useState([
    { role: "agent", text: "Hello! I'm DAPOS. I can help you connect to databases, discover schemas, set up data pipelines, analyze quality, and much more.\n\nTry asking me to discover tables in a database, profile a table, or create a pipeline. What would you like to do?" },
  ]);
  const [input, setInput] = useState("");
  const [loading, setLoading] = useState(false);
  const [sessionId] = useState(() => "session-" + Date.now());
  const endRef = useRef();

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
      <div className="px-6 py-4 border-b border-stone-200">
        <h1 className="text-lg font-semibold text-stone-800">Command</h1>
        <p className="text-sm text-stone-400">Chat with DAPOS</p>
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
                  ? "bg-stone-100 border border-stone-300 text-stone-700"
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
            <div className="px-4 py-3 rounded-xl bg-stone-100 border border-stone-300 text-stone-400 text-sm animate-pulse">
              Thinking...
            </div>
          </div>
        )}
        <div ref={endRef} />
      </div>
      <div className="px-6 py-3 border-t border-stone-200">
        <div className="flex flex-wrap gap-2 mb-3">
          {chips.map((c) => (
            <button
              key={c}
              onClick={() => send(c)}
              className="text-xs px-3 py-1.5 bg-stone-100 hover:bg-stone-200 text-stone-500 hover:text-stone-700 rounded-full border border-stone-300 transition-colors"
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
            className="flex-1 px-4 py-2.5 bg-stone-100 border border-stone-300 rounded-lg text-sm text-stone-700 outline-none focus:border-blue-500 placeholder-stone-400"
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


function PipelinesView({ tierFilter }) {
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

  async function addDep(pipelineId) {
    const depId = window.prompt("Enter upstream pipeline ID to depend on:");
    if (!depId) return;
    try {
      await api("POST", `/api/pipelines/${pipelineId}/dependencies`, { depends_on_id: depId });
      const d = await api("GET", `/api/pipelines/${pipelineId}`);
      setDetail(d);
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
      <h1 className="text-lg font-semibold mb-4 text-stone-800">Pipelines</h1>
      <div className="space-y-2">
        {pipelines.map((p) => (
          <div key={p.pipeline_id} className="bg-white border border-stone-200 rounded-xl overflow-hidden">
            <div
              className="flex items-center gap-3 px-4 py-3 cursor-pointer hover:bg-stone-50 transition-colors"
              onClick={() => expand(p)}
            >
              <StatusDot status={p.status} />
              <TierBadge tier={p.tier} />
              <span className="font-medium text-sm flex-1 font-mono text-stone-700">{p.pipeline_name}</span>
              <span className="text-xs text-stone-400">
                {p.source} -&gt; {p.target}
              </span>
              <Pill label={p.refresh_type} color="blue" />
              <Pill label={p.load_type} color="purple" />
              <span className="text-xs text-stone-400 font-mono">{p.schedule_cron}</span>
              {p.owner && <span className="text-xs text-stone-400">{p.owner}</span>}
            </div>
            {expanded === p.pipeline_id && detail && (
              <div className="border-t border-stone-200 px-4 py-4 bg-stone-50/50 space-y-4">
                <div className="grid grid-cols-3 gap-4 text-sm">
                  <div>
                    <span className="text-stone-400">Incremental col</span>
                    <br />
                    <span className="font-mono text-stone-600">{detail.incremental_column || "--"}</span>
                  </div>
                  <div>
                    <span className="text-stone-400">Merge keys</span>
                    <br />
                    <span className="font-mono text-stone-600">{detail.merge_keys?.join(", ") || "--"}</span>
                  </div>
                  <div>
                    <span className="text-stone-400">Version</span>
                    <br />
                    <span className="font-mono text-stone-600">v{detail.version}</span>
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
                          <span className="text-stone-600">{c.changed_by || "system"}</span>
                          <span className="text-stone-400">{c.source}</span>
                          {c.reason && <span className="text-stone-400 italic truncate max-w-xs">— {c.reason}</span>}
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                <div>
                  <div className="text-xs font-semibold text-stone-500 mb-2">Recent Runs</div>
                  <div className="space-y-1.5">
                    {runs.map((r) => <RunRow key={r.run_id} r={r} />)}
                    {runs.length === 0 && <div className="text-xs text-stone-300">No runs yet</div>}
                  </div>
                </div>

                {/* ---- Dependencies (Build 11) ---- */}
                {detail.dependencies && (
                  <div className="bg-stone-50 border border-stone-200 rounded-lg px-4 py-3 space-y-2">
                    <div className="text-xs font-semibold text-stone-500 mb-1">Dependencies</div>
                    <div className="grid grid-cols-2 gap-4">
                      <div>
                        <div className="text-[10px] text-stone-400 mb-1 uppercase tracking-wider">Upstream (depends on)</div>
                        {detail.dependencies.upstream?.length > 0 ? detail.dependencies.upstream.map((d) => (
                          <div key={d.dependency_id} className="flex items-center gap-2 text-xs border border-stone-200 rounded px-2 py-1 mb-1 bg-white">
                            <span className="font-mono text-stone-600">{d.depends_on_name || d.depends_on_id}</span>
                            <Pill label={d.dependency_type} color="blue" />
                            {d.notes && <span className="text-stone-400 italic text-[10px]">{d.notes}</span>}
                            <button onClick={() => removeDep(detail.pipeline_id, d.dependency_id)} className="text-red-400 hover:text-red-600 ml-auto text-[10px]">Remove</button>
                          </div>
                        )) : <div className="text-xs text-stone-300">No upstream dependencies</div>}
                        <button onClick={() => addDep(detail.pipeline_id)} className="text-[10px] text-blue-500 hover:text-blue-700 mt-1">+ Add dependency</button>
                      </div>
                      <div>
                        <div className="text-[10px] text-stone-400 mb-1 uppercase tracking-wider">Downstream</div>
                        <div className="text-xs text-stone-500">{detail.dependencies.downstream_count || 0} pipeline(s) depend on this</div>
                      </div>
                    </div>
                  </div>
                )}

                {/* ---- Metadata (Build 11) ---- */}
                {detail.metadata?.length > 0 && (
                  <div className="bg-stone-50 border border-stone-200 rounded-lg px-4 py-3">
                    <div className="text-xs font-semibold text-stone-500 mb-2">Pipeline Metadata</div>
                    <div className="grid grid-cols-3 gap-2">
                      {detail.metadata.map((m) => (
                        <div key={m.namespace + "/" + m.key} className="border border-stone-200 rounded px-2 py-1 bg-white">
                          <div className="text-[10px] text-stone-400">{m.namespace}/{m.key}</div>
                          <div className="text-xs font-mono text-stone-600 truncate">{JSON.stringify(m.value?.value ?? m.value)}</div>
                          <div className="text-[10px] text-stone-300">{(m.updated_at || "").slice(0, 16)}</div>
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                {/* ---- Post-Promotion Hooks (Build 13) ---- */}
                {!editForm && (detail.post_promotion_hooks || []).length > 0 && (
                  <div className="bg-stone-50 border border-stone-200 rounded-lg px-4 py-3">
                    <div className="text-xs font-semibold text-stone-500 mb-2">Post-Promotion Hooks</div>
                    <div className="space-y-2">
                      {detail.post_promotion_hooks.map((h) => {
                        const result = (detail.hook_results || {})[h.metadata_key || h.name];
                        return (
                          <div key={h.hook_id} className="flex items-start gap-2 text-xs">
                            <Pill label={h.enabled ? "on" : "off"} color={h.enabled ? "green" : "stone"} />
                            <div className="flex-1 min-w-0">
                              <div className="font-semibold text-stone-700">{h.name || "unnamed"}</div>
                              <div className="font-mono text-[10px] text-stone-400 truncate" title={h.sql}>{h.sql}</div>
                              {h.description && <div className="text-[10px] text-stone-400">{h.description}</div>}
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
                  <div className="bg-stone-50 border border-stone-200 rounded-lg px-4 py-2">
                    <div className="flex items-center gap-2">
                      <span className="text-xs font-semibold text-stone-500">Schema Policy</span>
                      {detail.schema_change_policy_is_custom && <Pill label="custom" color="purple" />}
                    </div>
                    <div className="text-xs text-stone-500 mt-1">
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
                      <div className="text-xs font-semibold text-stone-500 mb-1.5">Schedule</div>
                      <div className="grid grid-cols-4 gap-2">
                        <label className="text-xs text-stone-500">
                          Cron
                          <input className="block w-full mt-0.5 px-2 py-1 border border-stone-300 rounded text-xs font-mono bg-white" value={editForm.schedule_cron} onChange={(e) => setEditForm({...editForm, schedule_cron: e.target.value})} />
                        </label>
                        <label className="text-xs text-stone-500">
                          Retry attempts
                          <input type="number" className="block w-full mt-0.5 px-2 py-1 border border-stone-300 rounded text-xs bg-white" value={editForm.retry_max_attempts} onChange={(e) => setEditForm({...editForm, retry_max_attempts: parseInt(e.target.value) || 0})} />
                        </label>
                        <label className="text-xs text-stone-500">
                          Backoff (s)
                          <input type="number" className="block w-full mt-0.5 px-2 py-1 border border-stone-300 rounded text-xs bg-white" value={editForm.retry_backoff_seconds} onChange={(e) => setEditForm({...editForm, retry_backoff_seconds: parseInt(e.target.value) || 0})} />
                        </label>
                        <label className="text-xs text-stone-500">
                          Timeout (s)
                          <input type="number" className="block w-full mt-0.5 px-2 py-1 border border-stone-300 rounded text-xs bg-white" value={editForm.timeout_seconds} onChange={(e) => setEditForm({...editForm, timeout_seconds: parseInt(e.target.value) || 0})} />
                        </label>
                      </div>
                    </div>

                    {/* Strategy */}
                    <div>
                      <div className="text-xs font-semibold text-stone-500 mb-1.5">Strategy</div>
                      <div className="grid grid-cols-3 gap-2">
                        <label className="text-xs text-stone-500">
                          Refresh type
                          <select className="block w-full mt-0.5 px-2 py-1 border border-stone-300 rounded text-xs bg-white" value={editForm.refresh_type} onChange={(e) => setEditForm({...editForm, refresh_type: e.target.value})}>
                            <option value="full">full</option>
                            <option value="incremental">incremental</option>
                          </select>
                        </label>
                        <label className="text-xs text-stone-500">
                          Load type
                          <select className="block w-full mt-0.5 px-2 py-1 border border-stone-300 rounded text-xs bg-white" value={editForm.load_type} onChange={(e) => setEditForm({...editForm, load_type: e.target.value})}>
                            <option value="append">append</option>
                            <option value="merge">merge</option>
                          </select>
                        </label>
                        <label className="text-xs text-stone-500">
                          Replication
                          <select className="block w-full mt-0.5 px-2 py-1 border border-stone-300 rounded text-xs bg-white" value={editForm.replication_method} onChange={(e) => setEditForm({...editForm, replication_method: e.target.value})}>
                            <option value="watermark">watermark</option>
                            <option value="cdc">cdc</option>
                            <option value="snapshot">snapshot</option>
                          </select>
                        </label>
                      </div>
                      <div className="grid grid-cols-3 gap-2 mt-2">
                        <label className="text-xs text-stone-500">
                          Incremental column
                          <input className="block w-full mt-0.5 px-2 py-1 border border-stone-300 rounded text-xs font-mono bg-white" value={editForm.incremental_column} onChange={(e) => setEditForm({...editForm, incremental_column: e.target.value})} />
                        </label>
                        <label className="text-xs text-stone-500">
                          Merge keys (comma-sep)
                          <input className="block w-full mt-0.5 px-2 py-1 border border-stone-300 rounded text-xs font-mono bg-white" value={editForm.merge_keys} onChange={(e) => setEditForm({...editForm, merge_keys: e.target.value})} />
                        </label>
                        <div className="flex items-end gap-2">
                          <label className="text-xs text-stone-500 flex-1">
                            Watermark
                            <input className="block w-full mt-0.5 px-2 py-1 border border-stone-300 rounded text-xs font-mono bg-white" value={editForm.last_watermark} disabled={editForm.reset_watermark} onChange={(e) => setEditForm({...editForm, last_watermark: e.target.value})} />
                          </label>
                          <button
                            onClick={() => setEditForm({...editForm, reset_watermark: !editForm.reset_watermark})}
                            className={`text-xs px-2 py-1 rounded border mb-0.5 ${editForm.reset_watermark ? "bg-red-100 border-red-300 text-red-600" : "border-stone-300 text-stone-500 hover:bg-stone-100"}`}
                          >
                            {editForm.reset_watermark ? "Will Reset" : "Reset"}
                          </button>
                        </div>
                      </div>
                    </div>

                    {/* Quality */}
                    <div>
                      <div className="text-xs font-semibold text-stone-500 mb-1.5">Quality Thresholds</div>
                      <div className="grid grid-cols-3 gap-2">
                        <label className="text-xs text-stone-500">
                          Count tolerance
                          <input type="number" step="0.001" className="block w-full mt-0.5 px-2 py-1 border border-stone-300 rounded text-xs bg-white" value={editForm.count_tolerance} onChange={(e) => setEditForm({...editForm, count_tolerance: e.target.value})} />
                        </label>
                        <label className="text-xs text-stone-500">
                          Volume Z warn
                          <input type="number" step="0.1" className="block w-full mt-0.5 px-2 py-1 border border-stone-300 rounded text-xs bg-white" value={editForm.volume_z_score_warn} onChange={(e) => setEditForm({...editForm, volume_z_score_warn: e.target.value})} />
                        </label>
                        <label className="text-xs text-stone-500">
                          Volume Z fail
                          <input type="number" step="0.1" className="block w-full mt-0.5 px-2 py-1 border border-stone-300 rounded text-xs bg-white" value={editForm.volume_z_score_fail} onChange={(e) => setEditForm({...editForm, volume_z_score_fail: e.target.value})} />
                        </label>
                        <label className="text-xs text-stone-500">
                          Null rate stddev
                          <input type="number" step="0.1" className="block w-full mt-0.5 px-2 py-1 border border-stone-300 rounded text-xs bg-white" value={editForm.null_rate_stddev_threshold} onChange={(e) => setEditForm({...editForm, null_rate_stddev_threshold: e.target.value})} />
                        </label>
                        <label className="text-xs text-stone-500">
                          Freshness warn ×
                          <input type="number" step="0.1" className="block w-full mt-0.5 px-2 py-1 border border-stone-300 rounded text-xs bg-white" value={editForm.freshness_warn_multiplier} onChange={(e) => setEditForm({...editForm, freshness_warn_multiplier: e.target.value})} />
                        </label>
                        <label className="text-xs text-stone-500">
                          Freshness fail ×
                          <input type="number" step="0.1" className="block w-full mt-0.5 px-2 py-1 border border-stone-300 rounded text-xs bg-white" value={editForm.freshness_fail_multiplier} onChange={(e) => setEditForm({...editForm, freshness_fail_multiplier: e.target.value})} />
                        </label>
                      </div>
                      <div className="flex gap-4 mt-2">
                        <label className="text-xs text-stone-500 flex items-center gap-1.5">
                          <input type="checkbox" checked={editForm.promote_on_warn} onChange={(e) => setEditForm({...editForm, promote_on_warn: e.target.checked})} />
                          Promote on warn
                        </label>
                        <label className="text-xs text-stone-500 flex items-center gap-1.5">
                          <input type="checkbox" checked={editForm.halt_on_first_fail} onChange={(e) => setEditForm({...editForm, halt_on_first_fail: e.target.checked})} />
                          Halt on first fail
                        </label>
                      </div>
                    </div>

                    {/* Observability */}
                    <div>
                      <div className="text-xs font-semibold text-stone-500 mb-1.5">Observability</div>
                      <div className="grid grid-cols-4 gap-2">
                        <label className="text-xs text-stone-500">
                          Tier
                          <select className="block w-full mt-0.5 px-2 py-1 border border-stone-300 rounded text-xs bg-white" value={editForm.tier} onChange={(e) => setEditForm({...editForm, tier: parseInt(e.target.value)})}>
                            <option value={1}>T1 - Critical</option>
                            <option value={2}>T2 - Standard</option>
                            <option value={3}>T3 - Best-effort</option>
                          </select>
                        </label>
                        <label className="text-xs text-stone-500">
                          Owner
                          <input className="block w-full mt-0.5 px-2 py-1 border border-stone-300 rounded text-xs bg-white" value={editForm.owner} onChange={(e) => setEditForm({...editForm, owner: e.target.value})} />
                        </label>
                        <label className="text-xs text-stone-500">
                          Freshness column
                          <input className="block w-full mt-0.5 px-2 py-1 border border-stone-300 rounded text-xs font-mono bg-white" value={editForm.freshness_column} onChange={(e) => setEditForm({...editForm, freshness_column: e.target.value})} />
                        </label>
                        <label className="text-xs text-stone-500 flex items-end gap-1.5 pb-0.5">
                          <input type="checkbox" checked={editForm.auto_approve_additive_schema} onChange={(e) => setEditForm({...editForm, auto_approve_additive_schema: e.target.checked})} />
                          Auto-approve additive
                        </label>
                      </div>
                      <label className="text-xs text-stone-500 block mt-2">
                        Tags (JSON)
                        <textarea className="block w-full mt-0.5 px-2 py-1 border border-stone-300 rounded text-xs font-mono bg-white" rows={2} value={editForm.tags_json} onChange={(e) => setEditForm({...editForm, tags_json: e.target.value})} />
                      </label>
                    </div>

                    {/* Schema Change Policy (Build 12) */}
                    <div>
                      <div className="text-xs font-semibold text-stone-500 mb-1.5">Schema Change Policy</div>
                      <div className="grid grid-cols-4 gap-2">
                        <label className="text-xs text-stone-500">
                          New columns
                          <select className="block w-full mt-0.5 px-2 py-1 border border-stone-300 rounded text-xs bg-white" value={editForm.on_new_column} onChange={(e) => setEditForm({...editForm, on_new_column: e.target.value})}>
                            <option value="auto_add">Auto-add</option>
                            <option value="propose">Propose</option>
                            <option value="ignore">Ignore</option>
                          </select>
                        </label>
                        <label className="text-xs text-stone-500">
                          Dropped columns
                          <select className="block w-full mt-0.5 px-2 py-1 border border-stone-300 rounded text-xs bg-white" value={editForm.on_dropped_column} onChange={(e) => setEditForm({...editForm, on_dropped_column: e.target.value})}>
                            <option value="halt">Halt</option>
                            <option value="propose">Propose</option>
                            <option value="ignore">Ignore</option>
                          </select>
                        </label>
                        <label className="text-xs text-stone-500">
                          Type changes
                          <select className="block w-full mt-0.5 px-2 py-1 border border-stone-300 rounded text-xs bg-white" value={editForm.on_type_change} onChange={(e) => setEditForm({...editForm, on_type_change: e.target.value})}>
                            <option value="auto_widen">Auto-widen (safe)</option>
                            <option value="propose">Propose</option>
                            <option value="halt">Halt</option>
                          </select>
                        </label>
                        <label className="text-xs text-stone-500">
                          Nullable changes
                          <select className="block w-full mt-0.5 px-2 py-1 border border-stone-300 rounded text-xs bg-white" value={editForm.on_nullable_change} onChange={(e) => setEditForm({...editForm, on_nullable_change: e.target.value})}>
                            <option value="auto_accept">Auto-accept</option>
                            <option value="propose">Propose</option>
                            <option value="halt">Halt</option>
                          </select>
                        </label>
                      </div>
                      <label className="text-xs text-stone-500 flex items-center gap-1.5 mt-2">
                        <input type="checkbox" checked={editForm.propagate_to_downstream} onChange={(e) => setEditForm({...editForm, propagate_to_downstream: e.target.checked})} />
                        Propagate changes to downstream pipelines
                      </label>
                      <div className="text-[10px] text-stone-400 mt-1">Defaults based on tier. Override per-pipeline here.</div>
                    </div>

                    {/* Post-Promotion Hooks (Build 13) */}
                    <div>
                      <div className="text-xs font-semibold text-stone-500 mb-1.5">Post-Promotion SQL Hooks</div>
                      <textarea
                        className="block w-full px-2 py-1.5 border border-stone-300 rounded text-xs font-mono bg-white"
                        rows={6}
                        value={editForm.hooks_json}
                        onChange={(e) => setEditForm({...editForm, hooks_json: e.target.value})}
                        placeholder={'[\n  {"name": "row_count", "sql": "SELECT COUNT(*) as cnt FROM ...", "metadata_key": "total_rows"}\n]'}
                      />
                      <div className="text-[10px] text-stone-400 mt-1">
                        JSON array. Each hook: name, sql, metadata_key, description, enabled (true), timeout_seconds (30), fail_pipeline_on_error (false)
                      </div>
                      <div className="text-[10px] text-stone-400 mt-0.5">
                        Template variables: <code className="bg-stone-100 px-0.5 rounded">{"{{watermark_after}}"}</code> <code className="bg-stone-100 px-0.5 rounded">{"{{watermark_before}}"}</code> <code className="bg-stone-100 px-0.5 rounded">{"{{run_id}}"}</code> <code className="bg-stone-100 px-0.5 rounded">{"{{batch_id}}"}</code> <code className="bg-stone-100 px-0.5 rounded">{"{{target_schema}}"}</code> <code className="bg-stone-100 px-0.5 rounded">{"{{target_table}}"}</code> <code className="bg-stone-100 px-0.5 rounded">{"{{rows_extracted}}"}</code> <code className="bg-stone-100 px-0.5 rounded">{"{{pipeline_name}}"}</code> | Upstream: <code className="bg-stone-100 px-0.5 rounded">{"{{upstream_watermark_after}}"}</code> <code className="bg-stone-100 px-0.5 rounded">{"{{upstream_run_id}}"}</code> <code className="bg-stone-100 px-0.5 rounded">{"{{upstream_rows_extracted}}"}</code> <code className="bg-stone-100 px-0.5 rounded">{"{{upstream_pipeline_id}}"}</code>
                      </div>
                    </div>

                    {/* Footer: reason + save */}
                    <div className="flex items-end gap-2 pt-2 border-t border-blue-200">
                      <label className="text-xs text-stone-500 flex-1">
                        Change reason
                        <input className="block w-full mt-0.5 px-2 py-1 border border-stone-300 rounded text-xs bg-white" placeholder="Why are you making this change?" value={editForm.reason} onChange={(e) => setEditForm({...editForm, reason: e.target.value})} />
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
                        className="text-xs px-3 py-1.5 border border-stone-300 text-stone-500 rounded-lg hover:bg-stone-100"
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
                    className={`text-xs px-3 py-1.5 border rounded-lg ${yamlView ? "border-amber-300 text-amber-600 bg-amber-50" : "border-stone-300 text-stone-500 hover:bg-stone-100"}`}
                  >
                    {yamlView ? "Hide YAML" : "View YAML"}
                  </button>
                  <button
                    onClick={loadTimeline}
                    className={`text-xs px-3 py-1.5 border rounded-lg ${timeline.length > 0 ? "border-purple-300 text-purple-600 bg-purple-50" : "border-stone-300 text-stone-500 hover:bg-stone-100"}`}
                  >
                    {timeline.length > 0 ? "Hide Timeline" : "Timeline"}
                  </button>
                  {p.status === "active" ? (
                    <button
                      onClick={() => pause(p.pipeline_id)}
                      className="text-xs px-3 py-1.5 border border-stone-300 text-stone-500 rounded-lg hover:bg-stone-100"
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
                  <div className="bg-stone-900 rounded-xl p-4 overflow-auto max-h-96">
                    <pre className="text-xs text-green-400 font-mono whitespace-pre">{yamlView}</pre>
                  </div>
                )}

                {/* Timeline */}
                {timeline.length > 0 && (
                  <div>
                    <div className="text-xs font-semibold text-stone-500 mb-2">Change Timeline</div>
                    <div className="space-y-1.5">
                      {timeline.filter((e) => e.type === "decision").map((e, i) => (
                        <div key={i} className="border border-stone-200 rounded-lg px-3 py-2 bg-white">
                          <div className="flex items-center gap-2 text-xs">
                            <span className="px-1.5 py-0.5 bg-purple-100 text-purple-700 rounded text-[10px] font-medium">{e.decision_type || "decision"}</span>
                            <span className="text-stone-400 font-mono">{e.timestamp?.slice(0, 16)}</span>
                          </div>
                          {e.detail && (
                            <div className="text-xs text-stone-500 mt-1 font-mono truncate">{typeof e.detail === "string" ? e.detail : JSON.stringify(e.detail)}</div>
                          )}
                          {e.reasoning && (
                            <div className="text-xs text-stone-400 mt-0.5 italic">{e.reasoning}</div>
                          )}
                        </div>
                      ))}
                      {timeline.filter((e) => e.type === "decision").length === 0 && (
                        <div className="text-xs text-stone-300">No change events recorded yet</div>
                      )}
                    </div>
                  </div>
                )}
              </div>
            )}
          </div>
        ))}
        {pipelines.length === 0 && (
          <div className="text-sm text-stone-400 py-8 text-center">
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
    <div className="border-b border-stone-200">
      <div
        className="flex items-center gap-3 px-4 py-2.5 hover:bg-stone-50 text-sm cursor-pointer"
        onClick={() => setExpanded(!expanded)}
      >
        <StatusDot status={r.status} />
        <TierBadge tier={r.tier} />
        <span className="text-stone-400 font-mono text-xs w-32">{r.started_at?.slice(0, 16)}</span>
        <span className="font-mono font-medium flex-1 text-stone-700">{r.pipeline_name}</span>
        <span className="text-stone-400 text-xs">{r.rows_extracted?.toLocaleString()} rows</span>
        {r.gate_decision && (
          <Pill
            label={r.gate_decision}
            color={r.gate_decision === "halt" ? "red" : r.gate_decision === "promote_with_warning" ? "amber" : "green"}
          />
        )}
        {r.error && <span className="text-xs text-red-600 truncate max-w-[200px]">{r.error}</span>}
        <span className="text-stone-300 text-xs">{expanded ? "\u25B2" : "\u25BC"}</span>
      </div>
      {expanded && (
        <div className="px-4 pb-4 pt-1 bg-stone-50/50 space-y-3">
          {/* Run metadata grid */}
          <div className="grid grid-cols-2 sm:grid-cols-4 gap-3 text-xs">
            <div>
              <span className="text-stone-400 block">Duration</span>
              <span className="font-mono text-stone-700">{fmtDur}</span>
            </div>
            <div>
              <span className="text-stone-400 block">Mode</span>
              <span className="font-mono text-stone-700">{r.run_mode || "scheduled"}</span>
            </div>
            <div>
              <span className="text-stone-400 block">Extracted</span>
              <span className="font-mono text-stone-700">{r.rows_extracted?.toLocaleString() ?? "--"}</span>
            </div>
            <div>
              <span className="text-stone-400 block">Loaded</span>
              <span className="font-mono text-stone-700">{r.rows_loaded?.toLocaleString() ?? "--"}</span>
            </div>
            <div>
              <span className="text-stone-400 block">Staging Size</span>
              <span className="font-mono text-stone-700">{fmtBytes(r.staging_size_bytes) || "--"}</span>
            </div>
            <div>
              <span className="text-stone-400 block">Retries</span>
              <span className="font-mono text-stone-700">{r.retry_count ?? 0}</span>
            </div>
            <div>
              <span className="text-stone-400 block">Started</span>
              <span className="font-mono text-stone-700">{r.started_at?.replace("T", " ").slice(0, 19) || "--"}</span>
            </div>
            <div>
              <span className="text-stone-400 block">Completed</span>
              <span className="font-mono text-stone-700">{r.completed_at?.replace("T", " ").slice(0, 19) || "--"}</span>
            </div>
          </div>

          {/* Watermarks */}
          {(r.watermark_before || r.watermark_after) && (
            <div className="bg-white border border-stone-200 rounded-lg px-3 py-2">
              <div className="text-[10px] uppercase text-stone-400 font-semibold mb-1">Watermark</div>
              <div className="flex items-center gap-2 text-xs font-mono text-stone-600">
                <span className="bg-stone-100 rounded px-2 py-0.5">{r.watermark_before || "null"}</span>
                <span className="text-stone-400">&rarr;</span>
                <span className="bg-blue-50 text-blue-700 rounded px-2 py-0.5">{r.watermark_after || "null"}</span>
              </div>
            </div>
          )}

          {/* Triggered by */}
          {r.triggered_by_pipeline_id && (
            <div className="text-xs text-stone-500">
              Triggered by pipeline <span className="font-mono text-stone-600">{r.triggered_by_pipeline_id.slice(0, 8)}</span>
              {r.triggered_by_run_id && <span> (run <span className="font-mono text-stone-600">{r.triggered_by_run_id.slice(0, 8)}</span>)</span>}
            </div>
          )}

          {/* Quality gate checks */}
          {checks.length > 0 && (
            <div className="bg-white border border-stone-200 rounded-lg px-3 py-2">
              <div className="flex items-center justify-between mb-2">
                <span className="text-[10px] uppercase text-stone-400 font-semibold">Quality Gate</span>
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
                    <span className="font-medium text-stone-600 w-36">{c.name}</span>
                    <span className="text-stone-400 truncate">{c.detail}</span>
                  </div>
                ))}
              </div>
            </div>
          )}

          {/* Error detail */}
          {r.error && (
            <div className="bg-red-50 border border-red-200 rounded-lg px-3 py-2">
              <div className="text-[10px] uppercase text-red-400 font-semibold mb-1">Error</div>
              <div className="text-xs text-red-700 font-mono whitespace-pre-wrap">{r.error}</div>
            </div>
          )}

          {/* Run ID */}
          <div className="text-[10px] font-mono text-stone-400">Run ID: {r.run_id}</div>
        </div>
      )}
    </div>
  );
}

function ActivityView() {
  const [runs, setRuns] = useState([]);
  const [filter, setFilter] = useState("all");
  useEffect(() => {
    api("GET", "/api/pipelines")
      .then(async (pipelines) => {
        const allRuns = await Promise.all(
          pipelines.slice(0, 20).map((p) =>
            api("GET", `/api/pipelines/${p.pipeline_id}/runs?limit=10`)
              .then((rs) => rs.map((r) => ({ ...r, pipeline_name: p.pipeline_name, tier: p.tier })))
              .catch(() => [])
          )
        );
        const flat = allRuns.flat().sort((a, b) => (b.started_at || "").localeCompare(a.started_at || ""));
        setRuns(flat.slice(0, 100));
      })
      .catch(console.error);
  }, []);

  const filtered = filter === "all" ? runs
    : filter === "failed" ? runs.filter((r) => r.status === "failed" || r.status === "halted")
    : filter === "complete" ? runs.filter((r) => r.status === "complete")
    : runs;

  return (
    <div className="px-6 py-4">
      <div className="flex items-center justify-between mb-4">
        <h1 className="text-lg font-semibold text-stone-800">Activity</h1>
        <div className="flex gap-1">
          {["all", "complete", "failed"].map((f) => (
            <button
              key={f}
              onClick={() => setFilter(f)}
              className={`px-3 py-1 rounded-lg text-xs font-medium transition-colors ${
                filter === f
                  ? "bg-stone-800 text-white"
                  : "bg-stone-100 text-stone-500 hover:bg-stone-200"
              }`}
            >
              {f === "all" ? `All (${runs.length})` : f === "complete" ? "Completed" : "Failed/Halted"}
            </button>
          ))}
        </div>
      </div>
      <div className="bg-white border border-stone-200 rounded-xl overflow-hidden">
        {filtered.map((r) => <ActivityRunDetail key={r.run_id} r={r} />)}
        {filtered.length === 0 && (
          <div className="text-sm text-stone-400 py-8 text-center">No activity yet.</div>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// 4. Freshness View
// ---------------------------------------------------------------------------

function FreshnessView({ tierFilter }) {
  const [data, setData] = useState({});
  useEffect(() => {
    const tierParam = tierFilter !== "All" ? `?tier=${tierFilter[1]}` : "";
    api("GET", `/api/observability/freshness${tierParam}`)
      .then((d) => setData(d.tiers || {}))
      .catch(console.error);
  }, [tierFilter]);

  return (
    <div className="px-6 py-4">
      <h1 className="text-lg font-semibold mb-4 text-stone-800">Freshness</h1>
      {Object.entries(data).map(([tier, pipelines]) => (
        <div key={tier} className="mb-6">
          <div className="flex items-center gap-2 mb-3">
            <TierBadge tier={parseInt(tier)} />
            <span className="text-sm font-medium text-stone-500">{pipelines.length} pipeline(s)</span>
          </div>
          <div className="space-y-2">
            {pipelines.map((p) => {
              const pct = Math.min(100, (p.staleness_minutes / (p.freshness_sla_minutes * 5)) * 100);
              const color = p.status === "fresh" ? "green" : p.status === "warning" ? "amber" : "red";
              return (
                <div
                  key={p.pipeline_id}
                  className={`bg-white border rounded-xl px-4 py-3 ${
                    p.status === "critical" ? "border-red-200 bg-red-50" : "border-stone-200"
                  }`}
                >
                  <div className="flex items-center gap-3 mb-2">
                    <StatusDot status={p.status} />
                    <span className="font-mono text-sm font-medium flex-1 text-stone-700">{p.pipeline_name}</span>
                    <Pill label={p.status} color={color} />
                    <span className="text-xs text-stone-400">
                      {p.staleness_minutes?.toFixed(0)}m / {p.freshness_sla_minutes}m SLA
                    </span>
                  </div>
                  <ProgressBar pct={pct} color={color} />
                </div>
              );
            })}
          </div>
        </div>
      ))}
      {Object.keys(data).length === 0 && (
        <div className="text-sm text-stone-400 py-8 text-center">No freshness data yet.</div>
      )}
    </div>
  );
}

// ---------------------------------------------------------------------------
// 5. Quality View
// ---------------------------------------------------------------------------

function QualityView({ tierFilter }) {
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

  const gateColor = (d) =>
    d === "promote" ? "bg-green-500" : d === "promote_with_warning" ? "bg-amber-500" : "bg-red-500";

  return (
    <div className="px-6 py-4">
      <h1 className="text-lg font-semibold mb-4 text-stone-800">Quality</h1>
      <div className="grid grid-cols-1 gap-4 md:grid-cols-2 xl:grid-cols-3">
        {cards.map((p) => {
          const q = p.quality;
          const passRate = q?.summary?.pass_rate ?? null;
          const gates = q?.gates?.slice(0, 20) || [];
          return (
            <div key={p.pipeline_id} className="bg-white border border-stone-200 rounded-xl px-4 py-4">
              <div className="flex items-center gap-2 mb-2">
                <TierBadge tier={p.tier} />
                <span className="font-mono text-sm font-medium flex-1 truncate text-stone-700">
                  {p.pipeline_name}
                </span>
              </div>
              {q ? (
                <>
                  <div
                    className="text-3xl font-semibold font-mono mb-1"
                    style={{
                      color: passRate > 0.95 ? "#16a34a" : passRate > 0.8 ? "#d97706" : "#dc2626",
                    }}
                  >
                    {passRate !== null ? `${(passRate * 100).toFixed(1)}%` : "--"}
                  </div>
                  <div className="text-xs text-stone-400 mb-3">{q.summary?.total_runs} runs (7d)</div>
                  <div className="flex flex-wrap gap-1">
                    {gates.map((g, i) => (
                      <div key={i} className={`w-3.5 h-3.5 rounded-sm ${gateColor(g.decision)}`} title={g.decision} />
                    ))}
                  </div>
                </>
              ) : (
                <div className="text-xs text-stone-300">No data yet</div>
              )}
            </div>
          );
        })}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// 6. Approvals View
// ---------------------------------------------------------------------------

function ApprovalsView() {
  const [pending, setPending] = useState([]);
  const [resolved, setResolved] = useState([]);
  const [note, setNote] = useState({});
  const [connectorCode, setConnectorCode] = useState({});
  const [expandedCode, setExpandedCode] = useState({});
  const [testResults, setTestResults] = useState({});
  const [testing, setTesting] = useState({});

  useEffect(() => {
    api("GET", "/api/approvals?status=pending").then(setPending).catch(console.error);
    api("GET", "/api/approvals?status=applied").then(setResolved).catch(console.error);
  }, []);

  async function resolve(id, action) {
    await api("POST", `/api/approvals/${id}`, { action, note: note[id] || "" });
    setPending((p) => p.filter((x) => x.proposal_id !== id));
  }

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

  const changeColor = (t) =>
    ({ add_column: "green", drop_column: "red", alter_column_type: "amber", new_connector: "purple" }[t] || "blue");

  return (
    <div className="px-6 py-4">
      <h1 className="text-lg font-semibold mb-4 text-stone-800">Approvals</h1>
      {pending.length > 0 && (
        <div className="mb-6 space-y-3">
          <div className="text-sm font-medium text-amber-600">Pending ({pending.length})</div>
          {pending.map((p) => (
            <div key={p.proposal_id} className="bg-amber-50 border border-amber-200 rounded-xl px-4 py-4">
              <div className="flex items-center gap-2 mb-2">
                <Pill label={p.change_type} color={changeColor(p.change_type)} />
                <Pill label={p.trigger_type} color="gray" />
                <span className="text-xs text-stone-400 ml-auto">
                  confidence: {(p.confidence * 100).toFixed(0)}%
                </span>
              </div>
              <p className="text-sm text-stone-600 mb-2">{p.reasoning}</p>
              {p.impact_analysis?.breaking_change && (
                <div className="text-xs text-red-600 mb-2">
                  Breaking change -- {p.impact_analysis.data_loss_risk} data loss risk
                </div>
              )}
              {p.change_type === "new_connector" && p.connector_id && (
                <div className="mt-2 mb-2">
                  <div className="flex items-center gap-2">
                    <button
                      onClick={() => toggleCode(p.proposal_id, p.connector_id)}
                      className="text-xs text-purple-600 hover:text-purple-800 font-medium"
                    >
                      {expandedCode[p.proposal_id] ? "Hide Code" : "View Connector Code"}
                    </button>
                    <button
                      onClick={() => testConnector(p.connector_id)}
                      disabled={testing[p.connector_id]}
                      className="text-xs px-2.5 py-1 bg-purple-100 text-purple-700 border border-purple-200 rounded-lg hover:bg-purple-200 disabled:opacity-50"
                    >
                      {testing[p.connector_id] ? "Testing..." : "Test Connector"}
                    </button>
                    {testResults[p.connector_id] && (
                      <span className={`text-xs font-medium ${testResults[p.connector_id].success ? "text-green-600" : "text-red-600"}`}>
                        {testResults[p.connector_id].success ? "PASSED" : "FAILED"}
                        {testResults[p.connector_id].error && ` - ${testResults[p.connector_id].error}`}
                      </span>
                    )}
                  </div>
                  {expandedCode[p.proposal_id] && connectorCode[p.connector_id] && (
                    <pre className="mt-2 bg-stone-900 text-green-300 text-xs p-3 rounded-lg overflow-x-auto max-h-80 overflow-y-auto font-mono leading-relaxed">
                      {connectorCode[p.connector_id]}
                    </pre>
                  )}
                </div>
              )}
              <div className="flex items-center gap-2 mt-3">
                <input
                  value={note[p.proposal_id] || ""}
                  onChange={(e) => setNote((n) => ({ ...n, [p.proposal_id]: e.target.value }))}
                  placeholder="Optional note..."
                  className="flex-1 text-xs px-3 py-1.5 border border-stone-300 rounded-lg bg-stone-100 text-stone-600 outline-none"
                />
                <button
                  onClick={() => resolve(p.proposal_id, "approve")}
                  className="text-xs px-3 py-1.5 bg-green-600 text-white rounded-lg hover:bg-green-700"
                >
                  Approve
                </button>
                <button
                  onClick={() => resolve(p.proposal_id, "reject")}
                  className="text-xs px-3 py-1.5 bg-red-50 text-red-600 border border-red-200 rounded-lg hover:bg-red-100"
                >
                  Reject
                </button>
              </div>
            </div>
          ))}
        </div>
      )}
      <div>
        <div className="text-sm font-medium text-stone-400 mb-3">Resolved</div>
        <div className="space-y-2">
          {resolved.map((p) => (
            <div key={p.proposal_id} className="bg-white border border-stone-200 rounded-xl px-4 py-3 opacity-60">
              <div className="flex items-center gap-2">
                <Pill label={p.change_type} color="gray" />
                <span className="text-xs text-stone-400">{p.resolved_at?.slice(0, 16)}</span>
                <Pill label={p.status} color={p.status === "applied" ? "green" : "red"} />
                <span className="text-xs text-stone-400">{p.resolved_by}</span>
              </div>
            </div>
          ))}
          {resolved.length === 0 && <div className="text-xs text-stone-300">No resolved proposals yet.</div>}
        </div>
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// 7. Lineage & DAG View (consolidated from Build 19 + Lineage)
// ---------------------------------------------------------------------------

function DAGView() {
  const [dag, setDag] = useState(null);
  const [selected, setSelected] = useState(null);
  const [lineageDetail, setLineageDetail] = useState(null);
  const [search, setSearch] = useState("");
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
        <h1 className="text-lg font-semibold mb-4 text-stone-800">Lineage</h1>
        <div className="text-sm text-stone-400">Loading graph...</div>
      </div>
    );
  }

  if (!dag || dag.nodes.length === 0) {
    return (
      <div className="px-6 py-4">
        <h1 className="text-lg font-semibold mb-4 text-stone-800">Lineage</h1>
        <div className="text-sm text-stone-400">No pipelines found.</div>
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
        <h1 className="text-lg font-semibold text-stone-800">Lineage</h1>
        <div className="relative">
          <input
            type="text"
            value={search}
            onChange={(e) => setSearch(e.target.value)}
            placeholder="Search pipelines..."
            className="w-64 text-xs border border-stone-300 rounded-lg px-3 py-1.5 bg-white focus:outline-none focus:ring-2 focus:ring-blue-200 focus:border-blue-400 font-mono"
          />
          {search && (
            <button
              onClick={() => setSearch("")}
              className="absolute right-2 top-1/2 -translate-y-1/2 text-stone-400 hover:text-stone-600 text-xs"
            >
              x
            </button>
          )}
        </div>
      </div>
      <div className="text-xs text-stone-400 mb-4">
        {dag.total_pipelines} pipeline(s), {dag.total_edges} dependency edge(s)
        {hasSearch && ` — ${matchedIds.size} match(es)`}
      </div>
      <div className="flex gap-4">
        <div className="flex-1 bg-white border border-stone-200 rounded-xl overflow-hidden relative" style={{ maxHeight: "75vh" }}>
          {/* Zoom controls */}
          <div className="absolute top-3 left-3 z-10 flex flex-col gap-1">
            <button onClick={() => setZoom((z) => Math.min(3, z * 1.25))} className="w-7 h-7 bg-white border border-stone-300 rounded-lg text-stone-600 hover:bg-stone-50 text-sm font-bold shadow-sm">+</button>
            <button onClick={() => setZoom((z) => Math.max(0.15, z / 1.25))} className="w-7 h-7 bg-white border border-stone-300 rounded-lg text-stone-600 hover:bg-stone-50 text-sm font-bold shadow-sm">-</button>
            <button onClick={() => { setZoom(1); setPan({ x: 0, y: 0 }); }} className="w-7 h-7 bg-white border border-stone-300 rounded-lg text-stone-500 hover:bg-stone-50 text-[9px] font-semibold shadow-sm">fit</button>
          </div>
          <div className="absolute top-3 right-3 z-10 text-[10px] text-stone-400 bg-white/80 px-2 py-0.5 rounded">{Math.round(zoom * 100)}%</div>
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
          <div className="w-80 bg-white border border-stone-200 rounded-xl p-4 space-y-3 max-h-[75vh] overflow-y-auto">
            <div>
              <div className="text-xs text-stone-400">Pipeline</div>
              <div className="text-sm font-mono font-semibold text-stone-800">{selectedNode.name}</div>
            </div>
            <div className="flex gap-2 flex-wrap">
              <span className="flex items-center gap-1 text-xs">
                <StatusDot status={selectedNode.status} />
                {selectedNode.status}
              </span>
              <TierBadge tier={selectedNode.tier} />
              <Pill label={selectedNode.refresh_type} color="blue" />
            </div>
            <div className="text-xs text-stone-500 space-y-1">
              <div><span className="text-stone-400">Source:</span> {selectedNode.source}</div>
              <div><span className="text-stone-400">Target:</span> {selectedNode.target}</div>
              <div><span className="text-stone-400">Schedule:</span> {selectedNode.schedule_cron}</div>
              {selectedNode.owner && <div><span className="text-stone-400">Owner:</span> {selectedNode.owner}</div>}
            </div>
            {selectedNode.last_run && (
              <div className="border-t border-stone-200 pt-2">
                <div className="text-xs text-stone-400 mb-1">Last Successful Run</div>
                <div className="text-xs text-stone-500 space-y-0.5">
                  <div>{selectedNode.last_run.rows_loaded} rows loaded</div>
                  <div>{selectedNode.last_run.completed_at}</div>
                </div>
              </div>
            )}
            {(selectedNode.contracts_as_producer > 0 || selectedNode.contracts_as_consumer > 0) && (
              <div className="border-t border-stone-200 pt-2">
                <div className="text-xs text-stone-400 mb-1">Data Contracts</div>
                <div className="text-xs text-stone-500 space-y-0.5">
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
              <div className="border-t border-stone-200 pt-2">
                <div className="text-xs text-stone-400 mb-1">Dependencies</div>
                {lineageDetail.upstream.length > 0 && (
                  <div className="mb-1">
                    <span className="text-[10px] text-stone-400 uppercase">Upstream</span>
                    {lineageDetail.upstream.map((u) => (
                      <div key={u.pipeline_id} className="text-xs font-mono text-stone-600 ml-2">
                        {u.pipeline_name}
                        <span className="text-stone-400 ml-1">({u.dependency_type})</span>
                      </div>
                    ))}
                  </div>
                )}
                {lineageDetail.downstream.length > 0 && (
                  <div>
                    <span className="text-[10px] text-stone-400 uppercase">Downstream</span>
                    {lineageDetail.downstream.map((d) => (
                      <div key={d.pipeline_id} className="text-xs font-mono text-stone-600 ml-2">
                        {d.pipeline_name}
                        <span className="text-stone-400 ml-1">({d.dependency_type})</span>
                      </div>
                    ))}
                  </div>
                )}
              </div>
            )}

            {/* Column-level lineage */}
            {lineageDetail && lineageDetail.column_lineage && lineageDetail.column_lineage.length > 0 && (
              <div className="border-t border-stone-200 pt-2">
                <div className="text-xs text-stone-400 mb-1">Column Lineage</div>
                <div className="overflow-x-auto">
                  <table className="w-full text-[10px]">
                    <thead>
                      <tr className="text-stone-400 border-b border-stone-200">
                        <th className="text-left py-1 px-1">Source</th>
                        <th className="text-left py-1 px-1">Target</th>
                        <th className="text-left py-1 px-1">Transform</th>
                      </tr>
                    </thead>
                    <tbody>
                      {lineageDetail.column_lineage.map((cl) => (
                        <tr key={cl.lineage_id} className="border-b border-stone-200/50">
                          <td className="py-1 px-1 font-mono text-stone-600">{cl.source_column}</td>
                          <td className="py-1 px-1 font-mono text-stone-600">{cl.target_column}</td>
                          <td className="py-1 px-1 text-stone-400">{cl.transform_logic || "direct"}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}

            {/* Downstream column dependencies */}
            {lineageDetail && lineageDetail.downstream_columns && lineageDetail.downstream_columns.length > 0 && (
              <div className="border-t border-stone-200 pt-2">
                <div className="text-xs text-stone-400 mb-1">Downstream Column Deps</div>
                <div className="overflow-x-auto">
                  <table className="w-full text-[10px]">
                    <thead>
                      <tr className="text-stone-400 border-b border-stone-200">
                        <th className="text-left py-1 px-1">Pipeline</th>
                        <th className="text-left py-1 px-1">Source Col</th>
                        <th className="text-left py-1 px-1">Target Col</th>
                      </tr>
                    </thead>
                    <tbody>
                      {lineageDetail.downstream_columns.map((dc) => (
                        <tr key={dc.lineage_id} className="border-b border-stone-200/50">
                          <td className="py-1 px-1 font-mono text-stone-600">{dc.pipeline_id?.slice(0, 8)}</td>
                          <td className="py-1 px-1 font-mono text-stone-600">{dc.source_column}</td>
                          <td className="py-1 px-1 font-mono text-stone-600">{dc.target_column}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}

            <div className="border-t border-stone-200 pt-2">
              <div className="text-xs font-mono text-stone-400 break-all">{selectedNode.id}</div>
            </div>
          </div>
        )}
      </div>

      {/* Legend */}
      <div className="flex gap-6 mt-4 text-xs text-stone-400 flex-wrap">
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
      <h1 className="text-lg font-semibold mb-4 text-stone-800">Connectors</h1>

      <div className="bg-blue-50 border border-blue-200 rounded-xl p-4 mb-6">
        <div className="text-sm font-medium mb-3 text-blue-700">Generate New Connector</div>
        <div className="flex gap-3 items-end flex-wrap">
          <div>
            <div className="text-xs text-stone-400 mb-1">Type</div>
            <select
              value={form.type}
              onChange={(e) => setForm((f) => ({ ...f, type: e.target.value }))}
              className="px-3 py-1.5 border border-stone-300 rounded-lg text-sm bg-stone-100 text-stone-700"
            >
              <option value="source">Source</option>
              <option value="target">Target</option>
            </select>
          </div>
          <div className="flex-1 min-w-[150px]">
            <div className="text-xs text-stone-400 mb-1">Database type (e.g. postgres, mongodb, snowflake)</div>
            <input
              value={form.name}
              onChange={(e) => setForm((f) => ({ ...f, name: e.target.value }))}
              placeholder="postgres"
              className="w-full px-3 py-1.5 border border-stone-300 rounded-lg text-sm bg-stone-100 text-stone-700"
            />
          </div>
          <div className="flex-1 min-w-[150px]">
            <div className="text-xs text-stone-400 mb-1">Connection params (JSON, optional)</div>
            <input
              value={form.params}
              onChange={(e) => setForm((f) => ({ ...f, params: e.target.value }))}
              placeholder='{"host": "localhost"}'
              className="w-full px-3 py-1.5 border border-stone-300 rounded-lg text-sm font-mono bg-stone-100 text-stone-700"
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
              c.status === "draft" ? "border-purple-200 bg-purple-50" : "border-stone-200"
            }`}
          >
            <div className="flex items-center gap-3">
              <span className="font-mono text-sm font-medium flex-1 text-stone-700">{c.connector_name}</span>
              <Pill label={c.connector_type} color="blue" />
              <Pill label={c.source_target_type} color="gray" />
              <Pill label={c.status} color={statusColor(c.status)} />
              {c.test_status && (
                <Pill
                  label={c.test_status}
                  color={c.test_status === "passed" ? "green" : c.test_status === "failed" ? "red" : "gray"}
                />
              )}
              <span className="text-xs text-stone-400">{c.generated_by}</span>
            </div>
            {c.status === "draft" && (
              <div className="mt-2 text-xs text-purple-600 bg-purple-50 border border-purple-200 rounded px-2 py-1">
                Awaiting approval -- go to Approvals to review and approve this connector.
              </div>
            )}
          </div>
        ))}
        {connectors.length === 0 && (
          <div className="text-sm text-stone-400 py-8 text-center">
            No connectors yet. Generate one above or use the seed connectors.
          </div>
        )}
      </div>

      {migrations.length > 0 && (
        <div className="mt-8">
          <h2 className="text-sm font-semibold text-stone-500 mb-3">Recent Migrations</h2>
          <div className="bg-white border border-stone-200 rounded-xl overflow-hidden">
            <div className="divide-y divide-stone-200">
              {migrations.slice(0, 10).map((m) => (
                <div key={m.migration_id} className="flex items-center gap-3 px-4 py-2.5 text-xs">
                  <StatusDot status={m.status} />
                  <span className="font-mono text-stone-600">
                    v{m.from_version} -&gt; v{m.to_version}
                  </span>
                  <Pill label={m.migration_type} color="blue" />
                  <Pill label={m.status} color={m.status === "complete" ? "green" : m.status === "failed" ? "red" : "gray"} />
                  <span className="text-stone-400">{m.started_at?.slice(0, 16)}</span>
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

function AlertsView({ tierFilter }) {
  const [alerts, setAlerts] = useState([]);
  useEffect(() => {
    const tierParam = tierFilter !== "All" ? `&tier=${tierFilter[1]}` : "";
    api("GET", `/api/observability/alerts?hours=48${tierParam}`).then(setAlerts).catch(console.error);
  }, [tierFilter]);

  async function ack(id) {
    await api("POST", `/api/observability/alerts/${id}/acknowledge`);
    setAlerts((a) => a.map((x) => (x.alert_id === id ? { ...x, acknowledged: true } : x)));
  }

  const sevColor = (s) => ({ critical: "red", warning: "amber", info: "blue" }[s] || "gray");

  return (
    <div className="px-6 py-4">
      <h1 className="text-lg font-semibold mb-4 text-stone-800">
        Alerts
        <span className="ml-2 text-sm font-normal text-stone-400">
          ({alerts.filter((a) => !a.acknowledged).length} unacknowledged)
        </span>
      </h1>
      <div className="space-y-2">
        {alerts.map((a) => (
          <div
            key={a.alert_id}
            className={`bg-white border rounded-xl px-4 py-3 ${
              a.severity === "critical" && !a.acknowledged ? "border-red-200 bg-red-50" : "border-stone-200"
            }`}
          >
            <div className="flex items-center gap-3">
              <StatusDot status={a.severity} />
              <TierBadge tier={a.tier} />
              <span className="font-mono text-sm flex-1 text-stone-700">{a.pipeline_name}</span>
              <Pill label={a.severity} color={sevColor(a.severity)} />
              <span className="text-xs text-stone-400">{a.created_at?.slice(0, 16)}</span>
              {!a.acknowledged && (
                <button
                  onClick={() => ack(a.alert_id)}
                  className="text-xs px-2 py-1 border border-stone-300 text-stone-500 rounded hover:bg-stone-100"
                >
                  Ack
                </button>
              )}
              {a.acknowledged && <span className="text-xs text-green-600">acked</span>}
            </div>
            <p className="text-xs text-stone-500 mt-1 ml-8">{a.summary}</p>
          </div>
        ))}
        {alerts.length === 0 && (
          <div className="text-sm text-stone-400 py-8 text-center">No alerts in the last 48 hours.</div>
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
      <h1 className="text-lg font-semibold mb-4 text-stone-800">Agent Costs</h1>

      {summary && (
        <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
          {Object.entries(summary).map(([key, val]) => (
            <div key={key} className="bg-white border border-stone-200 rounded-xl px-4 py-3">
              <div className="text-xs text-stone-400 mb-1">{key.replace(/_/g, " ")}</div>
              <div className="text-lg font-semibold font-mono text-stone-700">
                {typeof val === "number" ? (key.includes("cost") || key.includes("usd") ? `$${val.toFixed(4)}` : val.toLocaleString()) : String(val)}
              </div>
            </div>
          ))}
        </div>
      )}

      <div className="flex items-center gap-3 mb-4">
        <span className="text-sm text-stone-400">Time range:</span>
        {[24, 48, 168, 720].map((h) => (
          <button
            key={h}
            onClick={() => setHours(h)}
            className={`text-xs px-3 py-1 rounded ${
              hours === h ? "bg-blue-600 text-white" : "bg-stone-100 text-stone-500 hover:bg-stone-200"
            }`}
          >
            {h <= 48 ? `${h}h` : `${Math.round(h / 24)}d`}
          </button>
        ))}
      </div>

      <div className="bg-white border border-stone-200 rounded-xl overflow-hidden">
        <table className="w-full text-sm">
          <thead>
            <tr className="text-stone-400 border-b border-stone-200 text-xs">
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
              <tr key={c.cost_id} className="border-b border-stone-200/50 hover:bg-stone-50/50">
                <td className="py-2 px-4 text-xs font-mono text-stone-400">{c.created_at?.slice(0, 16)}</td>
                <td className="py-2 px-4 text-xs font-mono text-stone-600">{c.pipeline_id?.slice(0, 8) || "--"}</td>
                <td className="py-2 px-4">
                  <Pill label={c.operation} color="blue" />
                </td>
                <td className="py-2 px-4 text-xs text-stone-500">{c.model}</td>
                <td className="py-2 px-4 text-xs font-mono text-stone-500 text-right">
                  {c.input_tokens?.toLocaleString()}
                </td>
                <td className="py-2 px-4 text-xs font-mono text-stone-500 text-right">
                  {c.output_tokens?.toLocaleString()}
                </td>
                <td className="py-2 px-4 text-xs font-mono text-stone-700 text-right">${c.cost_usd?.toFixed(4)}</td>
              </tr>
            ))}
          </tbody>
        </table>
        {costs.length === 0 && (
          <div className="text-sm text-stone-400 py-8 text-center">No agent cost data in this time range.</div>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// App Shell
// ---------------------------------------------------------------------------

function App() {
  const [view, setView] = useState("command");
  const [tierFilter, setTierFilter] = useState("All");
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

  useEffect(() => {
    fetch(API + "/health")
      .then((r) => r.json())
      .then((data) => {
        setAuthEnabled(data.auth_enabled === true);
        if (!data.auth_enabled) {
          setAuthState({ loggedIn: true, user: { user_id: "anonymous", username: "anonymous", role: "admin" } });
        }
      })
      .catch(() => setAuthEnabled(false));
  }, []);

  function handleLogin(data) {
    setAuthState({ loggedIn: true, user: { user_id: data.user_id, username: data.username, role: data.role } });
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
  const otherViews = {
    pipelines: <PipelinesView tierFilter={tierFilter} />,
    activity: <ActivityView />,
    freshness: <FreshnessView tierFilter={tierFilter} />,
    quality: <QualityView tierFilter={tierFilter} />,
    approvals: <ApprovalsView />,
    dag: <DAGView />,
    connectors: <ConnectorsView />,
    alerts: <AlertsView tierFilter={tierFilter} />,
    costs: <CostsView />,
  };

  return (
    <div className="flex h-screen overflow-hidden">
      <Sidebar
        view={view}
        setView={setView}
        tierFilter={tierFilter}
        setTierFilter={setTierFilter}
        user={authState.user}
        onLogout={handleLogout}
      />
      <main className="flex-1 overflow-y-auto">
        <div style={{ display: view === "command" ? "flex" : "none", flexDirection: "column", height: "100%" }}>
          <CommandView />
        </div>
        {view !== "command" && otherViews[view]}
      </main>
    </div>
  );
}

ReactDOM.createRoot(document.getElementById("root")).render(<App />);

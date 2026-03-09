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
  { id: "lineage", label: "Lineage", icon: "/" },
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

  useEffect(() => {
    const tierParam = tierFilter !== "All" ? `&tier=${tierFilter[1]}` : "";
    api("GET", `/api/pipelines?${tierParam}`).then(setPipelines).catch(console.error);
  }, [tierFilter]);

  async function expand(p) {
    if (expanded === p.pipeline_id) {
      setExpanded(null);
      return;
    }
    setExpanded(p.pipeline_id);
    const [d, r] = await Promise.all([
      api("GET", `/api/pipelines/${p.pipeline_id}`),
      api("GET", `/api/pipelines/${p.pipeline_id}/runs?limit=5`),
    ]);
    setDetail(d);
    setRuns(r);
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

  function budgetColor(eb) {
    if (!eb) return "gray";
    const pct = eb.utilization_pct || 0;
    if (pct < 50) return "green";
    if (pct < 80) return "amber";
    return "red";
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

                {detail.error_budget && (
                  <div className="bg-stone-50 border border-stone-300 rounded-lg px-4 py-3">
                    <div className="flex items-center justify-between mb-2">
                      <span className="text-xs font-semibold text-stone-500">Error Budget</span>
                      <Pill
                        label={`${detail.error_budget.utilization_pct.toFixed(1)}% used`}
                        color={budgetColor(detail.error_budget)}
                      />
                    </div>
                    <ProgressBar
                      pct={detail.error_budget.utilization_pct}
                      color={budgetColor(detail.error_budget)}
                    />
                    <div className="text-xs text-stone-400 mt-1">
                      {detail.error_budget.remaining_minutes?.toFixed(0)}m remaining of{" "}
                      {detail.error_budget.total_budget_minutes}m
                    </div>
                  </div>
                )}

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

                <div>
                  <div className="text-xs font-semibold text-stone-500 mb-2">Recent Runs</div>
                  <div className="space-y-1.5">
                    {runs.map((r) => {
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
                      const [showQuality, setShowQuality] = React.useState(false);
                      return (
                        <div key={r.run_id} className="border border-stone-200 rounded-lg px-3 py-2">
                          <div className="flex items-center gap-2 text-xs flex-wrap">
                            <StatusDot status={r.status} />
                            <span className="font-mono text-stone-400">{r.started_at?.slice(0, 16)}</span>
                            {fmtDuration && <span className="text-stone-400">{fmtDuration}</span>}
                            <Pill label={r.run_mode || "scheduled"} color="blue" />
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
                              watermark: {r.watermark_before || "null"} → {r.watermark_after || "null"}
                            </div>
                          )}
                          {r.quality_results && (
                            <div className="mt-1">
                              <button
                                onClick={() => setShowQuality(!showQuality)}
                                className="text-xs text-blue-500 hover:text-blue-700"
                              >
                                {showQuality ? "Hide" : "Show"} quality checks ({Object.keys(r.quality_results).length})
                              </button>
                              {showQuality && (
                                <div className="mt-1 bg-stone-50 rounded p-2 space-y-0.5">
                                  {Object.entries(r.quality_results).map(([check, result]) => (
                                    <div key={check} className="flex items-center gap-2 text-xs">
                                      <span className={`w-2 h-2 rounded-full ${
                                        result.status === "pass" ? "bg-green-400" :
                                        result.status === "warn" ? "bg-amber-400" : "bg-red-400"
                                      }`} />
                                      <span className="font-medium text-stone-600">{check}</span>
                                      {result.detail && <span className="text-stone-400 truncate">{typeof result.detail === 'string' ? result.detail : JSON.stringify(result.detail)}</span>}
                                    </div>
                                  ))}
                                </div>
                              )}
                            </div>
                          )}
                          {r.error && <div className="text-xs text-red-500 mt-1 truncate">{r.error}</div>}
                        </div>
                      );
                    })}
                    {runs.length === 0 && <div className="text-xs text-stone-300">No runs yet</div>}
                  </div>
                </div>

                <div className="flex gap-2">
                  <button
                    onClick={() => trigger(p.pipeline_id)}
                    className="text-xs px-3 py-1.5 bg-blue-600 text-white rounded-lg hover:bg-blue-700"
                  >
                    Trigger Run
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

function ActivityView() {
  const [runs, setRuns] = useState([]);
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

  return (
    <div className="px-6 py-4">
      <h1 className="text-lg font-semibold mb-4 text-stone-800">Activity</h1>
      <div className="bg-white border border-stone-200 rounded-xl overflow-hidden">
        <div className="divide-y divide-stone-200">
          {runs.map((r) => (
            <div key={r.run_id} className="flex items-center gap-3 px-4 py-2.5 hover:bg-stone-50 text-sm">
              <StatusDot status={r.status} />
              <TierBadge tier={r.tier} />
              <span className="text-stone-400 font-mono text-xs w-32">{r.started_at?.slice(0, 16)}</span>
              <span className="font-mono font-medium flex-1 text-stone-700">{r.pipeline_name}</span>
              <span className="text-stone-400">{r.rows_extracted?.toLocaleString()} rows</span>
              {r.gate_decision && (
                <Pill
                  label={r.gate_decision}
                  color={r.gate_decision === "halt" ? "red" : r.gate_decision === "promote_with_warning" ? "amber" : "green"}
                />
              )}
              {r.error && <span className="text-xs text-red-600 truncate max-w-xs">{r.error}</span>}
            </div>
          ))}
          {runs.length === 0 && (
            <div className="text-sm text-stone-400 py-8 text-center">No activity yet.</div>
          )}
        </div>
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
// 7. Lineage View (with column-level lineage)
// ---------------------------------------------------------------------------

function LineageView() {
  const [pipelines, setPipelines] = useState([]);
  const [selected, setSelected] = useState(null);
  const [graph, setGraph] = useState(null);

  useEffect(() => {
    api("GET", "/api/pipelines").then(setPipelines).catch(console.error);
  }, []);

  async function select(p) {
    setSelected(p.pipeline_id);
    const g = await api("GET", `/api/lineage/${p.pipeline_id}`);
    setGraph(g);
  }

  const statusColor = (s) => ({ active: "#4ade80", failed: "#f87171", paused: "#6b7280" }[s] || "#6b7280");

  return (
    <div className="px-6 py-4">
      <h1 className="text-lg font-semibold mb-4 text-stone-800">Lineage</h1>
      <div className="flex gap-4">
        <div className="w-56 space-y-1 max-h-[70vh] overflow-y-auto">
          {pipelines.map((p) => (
            <button
              key={p.pipeline_id}
              onClick={() => select(p)}
              className={`w-full text-left px-3 py-2 rounded-lg text-xs font-mono transition-colors ${
                selected === p.pipeline_id
                  ? "bg-blue-50 text-blue-700 border border-blue-200"
                  : "text-stone-500 hover:bg-stone-100 hover:text-stone-700"
              }`}
            >
              {p.pipeline_name}
            </button>
          ))}
        </div>
        {graph ? (
          <div className="flex-1 bg-white border border-stone-200 rounded-xl p-6">
            <div className="flex flex-col items-center gap-6">
              {graph.upstream.length > 0 && (
                <div>
                  <div className="text-xs text-stone-400 mb-2 text-center">Upstream</div>
                  <div className="flex gap-3 flex-wrap justify-center">
                    {graph.upstream.map((u) => (
                      <div
                        key={u.pipeline_id}
                        className="bg-stone-100 rounded-lg px-3 py-2 text-center border-2"
                        style={{ borderColor: statusColor(u.status) }}
                      >
                        <div className="text-xs font-mono text-stone-700">{u.pipeline_name}</div>
                        <TierBadge tier={u.tier} />
                      </div>
                    ))}
                  </div>
                  <div className="text-center text-stone-300 text-xl my-2">|</div>
                </div>
              )}
              <div className="bg-blue-50 rounded-lg px-4 py-3 border-2 border-blue-500 text-center">
                <div className="font-mono font-semibold text-sm text-blue-700">{graph.pipeline_name}</div>
              </div>
              {graph.downstream.length > 0 && (
                <div>
                  <div className="text-center text-stone-300 text-xl mb-2">|</div>
                  <div className="text-xs text-stone-400 mb-2 text-center">Downstream</div>
                  <div className="flex gap-3 flex-wrap justify-center">
                    {graph.downstream.map((d) => (
                      <div
                        key={d.pipeline_id}
                        className="bg-stone-100 rounded-lg px-3 py-2 text-center border-2"
                        style={{ borderColor: statusColor(d.status) }}
                      >
                        <div className="text-xs font-mono text-stone-700">{d.pipeline_name}</div>
                        <TierBadge tier={d.tier} />
                      </div>
                    ))}
                  </div>
                </div>
              )}
              {graph.upstream.length === 0 && graph.downstream.length === 0 && (
                <div className="text-sm text-stone-400">No dependencies declared for this pipeline.</div>
              )}
            </div>

            {graph.column_lineage && graph.column_lineage.length > 0 && (
              <div className="mt-8 border-t border-stone-200 pt-4">
                <div className="text-xs font-semibold text-stone-500 mb-3">Column-Level Lineage</div>
                <div className="overflow-x-auto">
                  <table className="w-full text-xs">
                    <thead>
                      <tr className="text-stone-400 border-b border-stone-200">
                        <th className="text-left py-2 px-2">Source Column</th>
                        <th className="text-left py-2 px-2">Target Column</th>
                        <th className="text-left py-2 px-2">Transform</th>
                      </tr>
                    </thead>
                    <tbody>
                      {graph.column_lineage.map((cl) => (
                        <tr key={cl.lineage_id} className="border-b border-stone-200/50">
                          <td className="py-1.5 px-2 font-mono text-stone-600">{cl.source_column}</td>
                          <td className="py-1.5 px-2 font-mono text-stone-600">{cl.target_column}</td>
                          <td className="py-1.5 px-2 text-stone-400">{cl.transform_logic || "direct"}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}

            {graph.downstream_columns && graph.downstream_columns.length > 0 && (
              <div className="mt-4 border-t border-stone-200 pt-4">
                <div className="text-xs font-semibold text-stone-500 mb-3">Downstream Column Dependencies</div>
                <div className="overflow-x-auto">
                  <table className="w-full text-xs">
                    <thead>
                      <tr className="text-stone-400 border-b border-stone-200">
                        <th className="text-left py-2 px-2">Pipeline</th>
                        <th className="text-left py-2 px-2">Source Column</th>
                        <th className="text-left py-2 px-2">Target Column</th>
                      </tr>
                    </thead>
                    <tbody>
                      {graph.downstream_columns.map((dc) => (
                        <tr key={dc.lineage_id} className="border-b border-stone-200/50">
                          <td className="py-1.5 px-2 font-mono text-stone-600">{dc.pipeline_id?.slice(0, 8)}...</td>
                          <td className="py-1.5 px-2 font-mono text-stone-600">{dc.source_column}</td>
                          <td className="py-1.5 px-2 font-mono text-stone-600">{dc.target_column}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}
          </div>
        ) : (
          <div className="flex-1 flex items-center justify-center text-sm text-stone-400">
            Select a pipeline to view its lineage
          </div>
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// 8. Connectors View (with migration info)
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
    lineage: <LineageView />,
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

function displayAddr(config) {
  const configured = (config?.addr || "").trim();
  if (!configured) {
    return window.location.host || "unknown";
  }

  if (configured.startsWith(":") || configured.startsWith("0.0.0.0:") || configured.startsWith("[::]:")) {
    return window.location.host || configured;
  }

  return configured;
}

function connectionLabel(health) {
  if (health === "ok") {
    return "Connected";
  }

  if (health === "degraded") {
    return "Degraded";
  }

  if (health === "disconnected") {
    return "Disconnected";
  }

  return "Connecting";
}

export default function Header({ version, health, updatedAt, config }) {
  return (
    <header className="dq-header">
      <div className="dq-logo-wrap">
        <div className="dq-brand-copy">
          <div className="dq-logo">
            <span className="dq-logo-drift">Drift</span>
            <span className="dq-logo-q">Q</span>
          </div>
          <div className="dq-brand-meta">
            <span className="dq-sub">Dashboard</span>
            <span className="dq-version">{version.version || "dev"}</span>
          </div>
        </div>
      </div>

      <div className="dq-status-wrap">
        <span className={`dq-dot ${health === "ok" ? "ok" : health === "disconnected" ? "offline" : "warn"}`} />
        <span className="dq-status-text">{connectionLabel(health)} - {displayAddr(config)}</span>
        <span className="dq-status-text dim">{health === "ok" ? "updated" : "last update"} {updatedAt}</span>
      </div>
    </header>
  );
}

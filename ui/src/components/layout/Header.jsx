export default function Header({ version, health, updatedAt }) {
  return (
    <header className="dq-header">
      <div className="dq-logo-wrap">
        <div className="dq-logo">
          <span className="dq-logo-drift">Drift</span>
          <span className="dq-logo-q">Q</span>
        </div>
        <span className="dq-sub">Dashboard</span>
        <span className="dq-version">{version.version || "dev"}</span>
      </div>

      <div className="dq-status-wrap">
        <span className={`dq-dot ${health === "ok" ? "ok" : "warn"}`} />
        <span className="dq-status-text">Connected - localhost:8080</span>
        <span className="dq-status-text dim">updated {updatedAt}</span>
      </div>
    </header>
  );
}

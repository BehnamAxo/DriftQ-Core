import { COMMON_TEXT, CONNECTION_STATUS_LABEL, HEADER_COPY, HEALTH_STATUS } from "../../../constants/ui";

function displayAddr(config) {
  const configured = (config?.addr || COMMON_TEXT.EMPTY).trim();
  if (!configured) {
    return window.location.host || COMMON_TEXT.UNKNOWN;
  }

  if (configured.startsWith(":") || configured.startsWith("0.0.0.0:") || configured.startsWith("[::]:")) {
    return window.location.host || configured;
  }

  return configured;
}

function connectionLabel(health) {
  return CONNECTION_STATUS_LABEL[health] || CONNECTION_STATUS_LABEL.DEFAULT;
}

export default function Header({ version, health, updatedAt, config }) {
  return (
    <header className="dq-header">
      <div className="dq-logo-wrap">
        <div className="dq-brand-copy">
          <div className="dq-logo">
            <span className="dq-logo-drift">{HEADER_COPY.BRAND_DRIFT}</span>
            <span className="dq-logo-q">{HEADER_COPY.BRAND_Q}</span>
          </div>
          <div className="dq-brand-meta">
            <span className="dq-sub">{HEADER_COPY.SUBTITLE}</span>
            <span className="dq-version">{version.version || COMMON_TEXT.DEV}</span>
          </div>
        </div>
      </div>

      <div className="dq-status-wrap">
        <span className={`dq-dot ${health === HEALTH_STATUS.OK ? "ok" : health === HEALTH_STATUS.DISCONNECTED ? "offline" : "warn"}`} />
        <span className="dq-status-text">{connectionLabel(health)} - {displayAddr(config)}</span>
        <span className="dq-status-text dim">{health === HEALTH_STATUS.OK ? HEADER_COPY.UPDATED : HEADER_COPY.LAST_UPDATE} {updatedAt}</span>
      </div>
    </header>
  );
}

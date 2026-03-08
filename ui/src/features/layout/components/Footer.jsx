import { APP_COPY, CONTROLS_COPY } from "../../../constants/ui";

export default function Footer({ tick }) {
  return (
    <footer className="dq-footer">
      <span>{APP_COPY.FOOTER_LABEL}</span>
      <span>{CONTROLS_COPY.tickLabel(tick)}</span>
    </footer>
  );
}

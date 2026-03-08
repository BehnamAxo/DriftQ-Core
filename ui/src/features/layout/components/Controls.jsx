import { COMMON_TEXT, CONTROLS_COPY } from "../../../constants/ui";

export default function Controls({ group, onGroupChange, onGroupBlur, onRefresh, loading, tick }) {
  return (
    <div className="dq-controls">
      <label>{CONTROLS_COPY.GROUP_LABEL}</label>
      <input
        value={group}
        onChange={(e) => onGroupChange(e.target.value)}
        onBlur={onGroupBlur}
        placeholder={CONTROLS_COPY.groupPlaceholder}
      />
      <button type="button" onClick={onRefresh}>
        {CONTROLS_COPY.REFRESH_BUTTON}
      </button>
      <span>{loading ? COMMON_TEXT.LOADING : CONTROLS_COPY.tickLabel(tick)}</span>
    </div>
  );
}

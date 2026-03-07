export default function Controls({ group, onGroupChange, onGroupBlur, onRefresh, loading, tick }) {
  return (
    <div className="dq-controls">
      <label>Group</label>
      <input
        value={group}
        onChange={(e) => onGroupChange(e.target.value)}
        onBlur={onGroupBlur}
        placeholder="bench"
      />
      <button type="button" onClick={onRefresh}>
        Refresh
      </button>
      <span>{loading ? "loading..." : `tick #${tick}`}</span>
    </div>
  );
}

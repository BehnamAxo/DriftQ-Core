import { metricsByName } from "../../../utils/metrics";
import { safeNum } from "../../../utils/number";

export function counterKey(labels, keys) {
  return keys.map((key) => labels?.[key] || "").join("\u0001");
}

export function counterMap(rows, name, keys) {
  const out = {};
  for (const row of metricsByName(rows, name)) {
    const key = counterKey(row.labels, keys);
    out[key] = (out[key] || 0) + safeNum(row.value);
  }
  return out;
}

export function pushCounterEvents(target, options) {
  const { current, previous, type, color, ts, parse, makeId, shouldInclude } = options;

  for (const [key, value] of Object.entries(current)) {
    const delta = safeNum(value) - safeNum(previous[key]);
    if (delta <= 0) {
      continue;
    }

    const payload = parse(key, delta);
    if (shouldInclude && !shouldInclude(payload, delta)) {
      continue;
    }

    target.push({
      id: makeId ? makeId(key, delta) : `${type}-${key}-${ts}`,
      type,
      color,
      ts,
      count: delta,
      ...payload
    });
  }
}

export function normalizeReason(reason) {
  const value = (reason || "").trim();
  if (!value) {
    return "";
  }
  if (value === "ack_timeout") {
    return "ack_timeout";
  }
  if (value.length > 32) {
    return `${value.slice(0, 29)}...`;
  }
  return value;
}

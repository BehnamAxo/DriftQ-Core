export function safeNum(v) {
  const n = Number(v);
  return Number.isFinite(n) ? n : 0;
}

export function fmt(v) {
  return safeNum(v).toLocaleString();
}

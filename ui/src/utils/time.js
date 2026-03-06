const CLOCK_OPTIONS = {
  hour12: false,
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit"
};

export function nowClock() {
  return new Date().toLocaleTimeString("en-US", CLOCK_OPTIONS);
}

export function formatClock(value) {
  if (!value) {
    return "-";
  }

  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) {
    return "-";
  }

  return date.toLocaleTimeString("en-US", CLOCK_OPTIONS);
}

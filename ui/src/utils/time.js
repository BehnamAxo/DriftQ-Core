import { COMMON_TEXT, TIME } from "../constants/ui";

const CLOCK_OPTIONS = {
  hour12: false,
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit"
};

export function nowClock() {
  return new Date().toLocaleTimeString(TIME.LOCALE, CLOCK_OPTIONS);
}

export function formatClock(value) {
  if (!value) {
    return COMMON_TEXT.DASH;
  }

  const date = value instanceof Date ? value : new Date(value);
  if (Number.isNaN(date.getTime())) {
    return COMMON_TEXT.DASH;
  }

  return date.toLocaleTimeString(TIME.LOCALE, CLOCK_OPTIONS);
}

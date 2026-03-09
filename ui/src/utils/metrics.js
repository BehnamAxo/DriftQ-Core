import { safeNum } from "./number";

export function parsePrometheus(text) {
  const out = [];
  for (const raw of text.split("\n")) {
    const line = raw.trim();
    if (!line || line.startsWith("#")) {
      continue;
    }

    const match = line.match(/^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})?\s+(-?(?:\d+(?:\.\d+)?|\.\d+)(?:[eE][+-]?\d+)?)$/);
    if (!match) {
      continue;
    }

    const [, name, labelsRaw = "", valueRaw] = match;
    const labels = {};
    const re = /([a-zA-Z_][a-zA-Z0-9_]*)="((?:\\.|[^"\\])*)"/g;
    let labelsMatch;

    while ((labelsMatch = re.exec(labelsRaw)) !== null) {
      labels[labelsMatch[1]] = labelsMatch[2].replace(/\\"/g, '"').replace(/\\\\/g, "\\");
    }

    out.push({ name, labels, value: safeNum(valueRaw) });
  }
  return out;
}

export function metricsByName(rows, name) {
  return rows.filter((r) => r.name === name);
}

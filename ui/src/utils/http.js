async function buildError(path, res) {
  let message = `${path} failed: ${res.status}`;

  try {
    const payload = await res.json();
    if (payload?.message) {
      message = payload.message;
    } else if (payload?.error) {
      message = payload.error;
    }
  } catch {
    // Ignore for now
  }

  return new Error(message);
}

export async function getJSON(path, signal) {
  const res = await fetch(path, { signal });

  if (!res.ok) {
    throw await buildError(path, res);
  }

  return res.json();
}

export async function getText(path, signal) {
  const res = await fetch(path, { signal });

  if (!res.ok) {
    throw await buildError(path, res);
  }

  return res.text();
}

export async function postJSON(path, body, signal) {
  const res = await fetch(path, {
    method: "POST",
    headers: {"Content-Type": "application/json"},
    body: JSON.stringify(body),
    signal
  });

  if (!res.ok) {
    throw await buildError(path, res);
  }

  return res.json();
}

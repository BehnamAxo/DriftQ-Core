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

  if (res.status === 204) {
    return null;
  }

  return res.json();
}

export async function readFirstNDJSON(path, { signal, timeoutMs = 4000 } = {}) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(new Error("consume timed out")), timeoutMs);

  const relayAbort = () => controller.abort(signal?.reason || new Error("request aborted"));
  if (signal) {
    if (signal.aborted) {
      relayAbort();
    } else {
      signal.addEventListener("abort", relayAbort, { once: true });
    }
  }

  try {
    const res = await fetch(path, { signal: controller.signal });
    if (!res.ok) {
      throw await buildError(path, res);
    }

    if (!res.body) {
      throw new Error("streaming body unavailable");
    }

    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = "";

    while (true) {
      const { value, done } = await reader.read();
      if (done) {
        break;
      }

      buffer += decoder.decode(value, { stream: true });
      const newlineIndex = buffer.indexOf("\n");
      if (newlineIndex >= 0) {
        const line = buffer.slice(0, newlineIndex).trim();
        if (!line) {
          buffer = buffer.slice(newlineIndex + 1);
          continue;
        }

        return JSON.parse(line);
      }
    }

    throw new Error("no message received");
  } catch (err) {
    if (controller.signal.aborted) {
      const reason = controller.signal.reason;
      if (reason instanceof Error && reason.message) {
        throw reason;
      }
      throw new Error("consume cancelled");
    }
    throw err;
  } finally {
    clearTimeout(timeout);
    if (signal) {
      signal.removeEventListener("abort", relayAbort);
    }
    controller.abort();
  }
}

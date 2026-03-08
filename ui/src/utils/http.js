import { COMMON_TEXT, HTTP } from "../constants/ui";

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
    method: HTTP.METHOD_POST,
    headers: { [HTTP.CONTENT_TYPE_HEADER]: HTTP.CONTENT_TYPE_JSON },
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

export async function streamNDJSON(path, { signal, onMessage } = {}) {
  const res = await fetch(path, { signal });
  if (!res.ok) {
    throw await buildError(path, res);
  }

  if (!res.body) {
    throw new Error(HTTP.STREAMING_BODY_UNAVAILABLE);
  }

  const reader = res.body.getReader();
  const decoder = new TextDecoder();
  let buffer = COMMON_TEXT.EMPTY;

  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) {
        break;
      }

      buffer += decoder.decode(value, { stream: true });

      while (true) {
        const newlineIndex = buffer.indexOf("\n");
        if (newlineIndex < 0) {
          break;
        }

        const line = buffer.slice(0, newlineIndex).trim();
        buffer = buffer.slice(newlineIndex + 1);
        if (!line) {
          continue;
        }

        onMessage?.(JSON.parse(line));
      }
    }

    const tail = buffer.trim();
    if (tail) {
      onMessage?.(JSON.parse(tail));
    }
  } catch (err) {
    if (signal?.aborted) {
      const reason = signal.reason;
      if (reason instanceof Error && reason.message) {
        throw reason;
      }
      throw new Error(HTTP.STREAM_CANCELLED);
    }
    throw err;
  } finally {
    reader.releaseLock();
  }
}

export async function readFirstNDJSON(path, { signal, timeoutMs = 4000 } = {}) {
  const controller = new AbortController();
  const timeout = setTimeout(() => controller.abort(new Error(HTTP.CONSUME_TIMED_OUT)), timeoutMs);

  const relayAbort = () => controller.abort(signal?.reason || new Error(HTTP.REQUEST_ABORTED));
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
      throw new Error(HTTP.STREAMING_BODY_UNAVAILABLE);
    }

    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buffer = COMMON_TEXT.EMPTY;

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

    throw new Error(HTTP.NO_MESSAGE_RECEIVED);
  } catch (err) {
    if (controller.signal.aborted) {
      const reason = controller.signal.reason;
      if (reason instanceof Error && reason.message) {
        throw reason;
      }
      throw new Error(HTTP.CONSUME_CANCELLED);
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

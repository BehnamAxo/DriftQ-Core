export async function getJSON(path, signal) {
  const res = await fetch(path, { signal });
  if (!res.ok) {
    throw new Error(`${path} failed: ${res.status}`);
  }

  return res.json();
}

export async function getText(path, signal) {
  const res = await fetch(path, { signal });
  if (!res.ok) {
    throw new Error(`${path} failed: ${res.status}`);
  }

  return res.text();
}

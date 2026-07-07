// Where the PakMedTrail API lives. Default matches the API .env (PORT=4000).
// You can change this at runtime from the gear menu in the top bar. The value
// is kept in localStorage so it survives reloads.

const DEFAULT_API_BASE = "http://localhost:4000";
const LS_KEY = "pmt_api_base";

export function getApiBase() {
  try {
    return localStorage.getItem(LS_KEY) || DEFAULT_API_BASE;
  } catch {
    return DEFAULT_API_BASE;
  }
}

export function setApiBase(url) {
  try {
    if (url) localStorage.setItem(LS_KEY, url.replace(/\/+$/, ""));
    else localStorage.removeItem(LS_KEY);
  } catch {
    // ignore storage failures
  }
}

export { DEFAULT_API_BASE };

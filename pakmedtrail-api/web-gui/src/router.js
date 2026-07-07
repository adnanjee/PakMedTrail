// Minimal hash router. Routes are registered as name -> render(params). The app
// reads location.hash like #/lots or #/lots/LOT123 and calls the matching page.

const routes = new Map();
let notFound = () => "<div class='content'>Not found</div>";
let onNavigate = () => {};
let started = false;

export function register(name, handler) {
  routes.set(name, handler);
}

export function setNotFound(handler) {
  notFound = handler;
}

export function setOnNavigate(handler) {
  onNavigate = handler;
}

export function go(path) {
  if (location.hash === "#" + path) resolve();
  else location.hash = path;
}

export function current() {
  const raw = location.hash.replace(/^#/, "") || "/dashboard";
  const parts = raw.split("/").filter(Boolean);
  const name = parts[0] || "dashboard";
  const params = parts.slice(1).map(decodeURIComponent);
  return { name, params, path: raw };
}

export function resolve() {
  const { name, params } = current();
  const handler = routes.get(name) || notFound;
  onNavigate(name);
  return handler(params);
}

export function start() {
  if (!started) {
    window.addEventListener("hashchange", resolve);
    started = true;
  }
  if (!location.hash) location.hash = "/dashboard";
}

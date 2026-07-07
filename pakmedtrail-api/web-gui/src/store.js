// Session state. The JWT and the user record are kept in localStorage so a
// reload keeps you signed in until the token expires (the server signs tokens
// for 8h). Role helpers drive which nav items and actions show.

const TOKEN_KEY = "pmt_token";
const USER_KEY = "pmt_user";

let token = null;
let user = null;

try {
  token = localStorage.getItem(TOKEN_KEY);
  const raw = localStorage.getItem(USER_KEY);
  user = raw ? JSON.parse(raw) : null;
} catch {
  token = null;
  user = null;
}

export function getToken() {
  return token;
}

export function getUser() {
  return user;
}

export function isLoggedIn() {
  return Boolean(token && user);
}

export function setSession(nextToken, nextUser) {
  token = nextToken;
  user = nextUser;
  try {
    localStorage.setItem(TOKEN_KEY, nextToken);
    localStorage.setItem(USER_KEY, JSON.stringify(nextUser));
  } catch {
    // ignore
  }
}

export function clearSession() {
  token = null;
  user = null;
  try {
    localStorage.removeItem(TOKEN_KEY);
    localStorage.removeItem(USER_KEY);
  } catch {
    // ignore
  }
}

export function role() {
  return user ? user.role : null;
}

export function isRole(...roles) {
  return user ? roles.includes(user.role) : false;
}

export function orgMSP() {
  return user ? user.org : null;
}

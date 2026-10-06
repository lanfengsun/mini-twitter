// Shared helpers for the k6 scenarios.
//
// Run k6 natively on the Mac (brew install k6): it then competes for the 8 physical cores with
// the Docker VM, which is noted as a caveat in the write-up.
//
// The seeder created sessions "loadtoken-0".."loadtoken-(N-1)" (users u1..uN; N = LOAD_SESSIONS,
// 2000 for `make seed-small`, 10000 for `make seed`) and "loadtoken-celeb1". Load tests skip the
// (bcrypt-expensive) login step by using those tokens. setup() probes how many exist, so a test
// never uses tokens that were not seeded (that would just measure 401s). Override with -e SESSIONS=n.
import http from 'k6/http';
import { Counter } from 'k6/metrics';

export const BASE = __ENV.BASE || 'http://localhost:8080';

// 429 is deliberate load shedding, not an error: count it separately and keep it out of
// http_req_failed so "error rate" means 5xx/timeouts/unexpected 4xx.
http.setResponseCallback(http.expectedStatuses({ min: 200, max: 299 }, 429));
export const shed429 = new Counter('shed_429');
export const accepted = new Counter('writes_accepted');

export function headers(token) {
  return { headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' } };
}

// Params for a request: auth headers plus an `op` tag, and a `name` tag so that URLs containing
// ids (/api/posts/123/like) collapse into one time series instead of one per post.
export function params(token, op, name) {
  const p = headers(token);
  p.tags = { op: op, name: name || op };
  return p;
}

export function tokenFor(i, n) { return `loadtoken-${i % n}`; }
export function randomToken(n) { return tokenFor(Math.floor(Math.random() * 1e9), n); }
export function pick(arr) { return arr[Math.floor(Math.random() * arr.length)]; }
export function randomUser(n) { return `u${1 + Math.floor(Math.random() * n)}`; }

export function track(res) {
  if (res.status === 429) shed429.add(1);
  else if (res.status === 202) accepted.add(1);
  return res;
}

// Tokens 0..N-1 are valid, so binary-search the largest N with loadtoken-(N-1) alive.
function discoverSessions(max) {
  let lo = 0, hi = max;
  while (lo < hi) {
    const mid = Math.ceil((lo + hi) / 2);
    const r = http.get(`${BASE}/api/me`, Object.assign(
      { responseCallback: http.expectedStatuses(200, 401), tags: { op: 'probe', name: 'probe' } },
      headers(`loadtoken-${mid - 1}`)));
    if (r.status === 200) lo = mid; else hi = mid - 1;
  }
  return lo;
}

// Call from setup(); returns how many seeded sessions exist.
export function setupSessions() {
  if (__ENV.SESSIONS) return parseInt(__ENV.SESSIONS);
  const n = discoverSessions(parseInt(__ENV.MAX_SESSIONS || '10000'));
  if (n === 0) {
    throw new Error(`no load-test sessions found at ${BASE}: is the stack up and seeded? (make seed-small / make seed)`);
  }
  console.log(`using ${n} seeded sessions (users u1..u${n})`);
  return n;
}

const phrases = ['shipping it', 'coffee first', 'p99 looks fine', 'who broke prod', 'cache hit rate is up',
                 'queue is draining', 'sharding is hard', 'load testing on a laptop', 'monday again'];
export function text() { return `${pick(phrases)} ${Math.random().toString(36).slice(2, 8)}`; }

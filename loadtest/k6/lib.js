// Shared helpers for the k6 scenarios.
//
// Run k6 natively on the Mac (brew install k6): it then competes for the 8 physical cores with
// the Docker VM, which is noted as a caveat in the write-up.
//
// `make seed` created sessions "loadtoken-0".."loadtoken-9999" (users u1..u10000) and
// "loadtoken-celeb1", so load tests skip the (bcrypt-expensive) login step.
import http from 'k6/http';
import { Counter } from 'k6/metrics';

export const BASE = __ENV.BASE || 'http://localhost:8080';
export const USERS = parseInt(__ENV.USERS || '500000');        // seeded users (for follow targets)
export const SESSIONS = parseInt(__ENV.SESSIONS || '10000');   // seeded sessions

// 429 is deliberate load shedding, not an error: count it separately and keep it out of
// http_req_failed so "error rate" means 5xx/timeouts/unexpected 4xx.
http.setResponseCallback(http.expectedStatuses({ min: 200, max: 299 }, 429));
export const shed429 = new Counter('shed_429');
export const accepted = new Counter('writes_accepted');

export function tokenFor(i) { return `loadtoken-${i % SESSIONS}`; }
export function headers(token) {
  return { headers: { 'Authorization': `Bearer ${token}`, 'Content-Type': 'application/json' } };
}
export function pick(arr) { return arr[Math.floor(Math.random() * arr.length)]; }

export function track(res) {
  if (res.status === 429) shed429.add(1);
  else if (res.status === 202) accepted.add(1);
  return res;
}

const phrases = ['shipping it', 'coffee first', 'p99 looks fine', 'who broke prod', 'cache hit rate is up',
                 'queue is draining', 'sharding is hard', 'load testing on a laptop', 'monday again'];
export function text() { return `${pick(phrases)} ${Math.random().toString(36).slice(2, 8)}`; }

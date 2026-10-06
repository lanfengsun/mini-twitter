// Sign-up and login storm. bcrypt (cost 10, ~50-60 ms of CPU per hash) makes this the
// first CPU saturation point of the whole system: expect throughput to plateau at roughly
// (CPU cores given to the API) / (hash time), long before Redis or Postgres notice.
//
//   k6 run -e SIGNUP_RATE=20 -e LOGIN_RATE=20 loadtest/k6/auth.js
import http from 'k6/http';
import { check } from 'k6';
import { BASE, SESSIONS } from './lib.js';

export const options = {
  scenarios: {
    signups: {
      executor: 'constant-arrival-rate', exec: 'signup',
      rate: parseInt(__ENV.SIGNUP_RATE || '20'), timeUnit: '1s', duration: __ENV.DURATION || '60s',
      preAllocatedVUs: 50, maxVUs: 1000,
    },
    logins: {
      executor: 'constant-arrival-rate', exec: 'login',
      rate: parseInt(__ENV.LOGIN_RATE || '20'), timeUnit: '1s', duration: __ENV.DURATION || '60s',
      preAllocatedVUs: 50, maxVUs: 1000,
    },
  },
  thresholds: { http_req_failed: ['rate<0.01'], http_req_duration: ['p(99)<500'] },
};

const json = { headers: { 'Content-Type': 'application/json' } };
const run = Date.now().toString(36);

export function signup() {
  const name = `k6_${run}_${__VU}_${__ITER}`.slice(0, 20);
  const res = http.post(`${BASE}/api/signup`, JSON.stringify({ username: name, password: 'password' }), Object.assign({ tags: { op: 'signup' } }, json));
  check(res, { 'signup 201': (r) => r.status === 201 });
}

export function login() {
  const name = `u${1 + Math.floor(Math.random() * SESSIONS)}`;
  const res = http.post(`${BASE}/api/login`, JSON.stringify({ username: name, password: 'password' }), Object.assign({ tags: { op: 'login' } }, json));
  check(res, { 'login 200': (r) => r.status === 200 });
}

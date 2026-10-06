// Finds the saturation point: the offered load climbs in steps while the Grafana dashboard
// shows where p99 latency, error rate or throughput bend.
//
//   k6 run -e STEP=500 -e STEPS=10 -e STEP_DURATION=60s loadtest/k6/ramp.js
//
// Saturation (as defined in the write-up) is the first step where ANY of these holds:
//   p99 > 500 ms,  error rate > 1%,  or achieved throughput stops following offered load.
// k6 prints the achieved iteration rate; compare it with the step target.
import http from 'k6/http';
import { check } from 'k6';
import { BASE, setupSessions, randomToken, randomUser, params, pick, track, text } from './lib.js';

const STEP = parseInt(__ENV.STEP || '500');
const STEPS = parseInt(__ENV.STEPS || '10');
const STEP_DURATION = __ENV.STEP_DURATION || '60s';

const stages = [];
for (let i = 1; i <= STEPS; i++) {
  stages.push({ target: STEP * i, duration: '10s' });         // ramp to the next level...
  stages.push({ target: STEP * i, duration: STEP_DURATION }); // ...and hold it
}

export const options = {
  scenarios: {
    ramp: {
      executor: 'ramping-arrival-rate',
      startRate: STEP, timeUnit: '1s', stages,
      preAllocatedVUs: 500, maxVUs: 8000,
    },
  },
  // not aborting on failure: the whole curve is the result
  thresholds: { http_req_failed: ['rate<0.01'], http_req_duration: ['p(99)<500'] },
};

export function setup() { return { n: setupSessions() }; }

const seen = [];

export default function (data) {
  const tok = randomToken(data.n);
  const r = Math.random();
  if (r < 0.75 || seen.length === 0) {
    const res = http.get(`${BASE}/api/feed`, params(tok, 'feed', 'GET /api/feed'));
    check(res, { 'feed 200': (x) => x.status === 200 });
    if (res.status === 200) for (const p of res.json('posts') || []) { seen.push(p.id); if (seen.length > 50) seen.shift(); }
  } else if (r < 0.90) {
    track(http.post(`${BASE}/api/posts`, JSON.stringify({ text: text() }), params(tok, 'post', 'POST /api/posts')));
  } else if (r < 0.97) {
    track(http.post(`${BASE}/api/posts/${pick(seen)}/like`, null, params(tok, 'like', 'POST /api/posts/:id/like')));
  } else {
    track(http.post(`${BASE}/api/follow/${randomUser(data.n)}`, null, params(tok, 'follow', 'POST /api/follow/:user')));
  }
}

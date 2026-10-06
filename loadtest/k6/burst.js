// Write burst: many users post at once. Shows the queue absorbing the spike (queue depth
// climbs, API latency stays flat, 429s appear if depth passes QUEUE_MAX_DEPTH) and the
// worker draining it afterwards (queue depth and feed-freshness lag on the dashboard).
//
//   k6 run -e BURST_RATE=5000 -e BURST_DURATION=60s loadtest/k6/burst.js
//
// Alongside the burst: a trickle of celebrity posts (no fan-out; merged at read time) and a
// steady reader, to show reads stay healthy while the write queue is deep.
import http from 'k6/http';
import { check } from 'k6';
import { BASE, tokenFor, headers, track, text } from './lib.js';

const BURST_RATE = parseInt(__ENV.BURST_RATE || '5000');
const BURST_DURATION = __ENV.BURST_DURATION || '60s';

export const options = {
  scenarios: {
    burst_posts: {
      executor: 'ramping-arrival-rate', exec: 'burst',
      startRate: 100, timeUnit: '1s',
      stages: [
        { target: BURST_RATE, duration: '10s' },
        { target: BURST_RATE, duration: BURST_DURATION },
        { target: 0, duration: '5s' },
      ],
      preAllocatedVUs: 500, maxVUs: 6000,
    },
    celeb_posts: {
      executor: 'constant-arrival-rate', exec: 'celeb',
      rate: 2, timeUnit: '1s', duration: '90s', preAllocatedVUs: 5,
    },
    steady_reads: {
      executor: 'constant-arrival-rate', exec: 'reader',
      rate: 200, timeUnit: '1s', duration: '90s', preAllocatedVUs: 50, maxVUs: 400,
    },
  },
  thresholds: {
    'http_req_failed': ['rate<0.01'],
    'http_req_duration{scenario:steady_reads}': ['p(99)<500'],
  },
};

export function burst() {
  track(http.post(`${BASE}/api/posts`, JSON.stringify({ text: text() }),
    Object.assign({ tags: { op: 'post' } }, headers(tokenFor(Math.floor(Math.random() * 1e9))))));
}

export function celeb() {
  track(http.post(`${BASE}/api/posts`, JSON.stringify({ text: 'celebrity update ' + text() }),
    Object.assign({ tags: { op: 'celeb_post' } }, headers('loadtoken-celeb1'))));
}

export function reader() {
  const res = http.get(`${BASE}/api/feed`, Object.assign({ tags: { op: 'feed' } }, headers(tokenFor(Math.floor(Math.random() * 1e9)))));
  check(res, { 'feed 200': (x) => x.status === 200 });
}

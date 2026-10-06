// Steady mixed workload at a fixed ARRIVAL RATE (open model), so a slow server cannot
// slow the offered load down (no coordinated omission).
//
//   k6 run -e RATE=1000 -e DURATION=3m loadtest/k6/mixed.js
//
// Mix: 70% read feed, 10% create post, 8% like, 5% follow, 4% reply, 3% read replies.
// Every seeded user follows the celebrity (celeb1), so feed reads exercise the
// merge-at-read path for celebrity posts, and the first read of each user is a cold rebuild.
import http from 'k6/http';
import { check } from 'k6';
import { BASE, setupSessions, randomToken, randomUser, params, pick, track, text } from './lib.js';

const RATE = parseInt(__ENV.RATE || '1000');

export const options = {
  scenarios: {
    mixed: {
      executor: 'constant-arrival-rate',
      rate: RATE, timeUnit: '1s', duration: __ENV.DURATION || '3m',
      preAllocatedVUs: Math.ceil(Math.max(100, RATE / 5)), maxVUs: Math.max(500, RATE * 2),
    },
  },
  thresholds: {
    http_req_failed: ['rate<0.01'],          // saturation criterion: error rate > 1%
    http_req_duration: ['p(99)<500'],        // saturation criterion: p99 > 500 ms
  },
};

export function setup() { return { n: setupSessions() }; }

// each VU remembers the last post ids it saw, so likes/replies target real posts
const seen = [];

export default function (data) {
  const tok = randomToken(data.n);
  const r = Math.random();

  if (r < 0.70 || seen.length === 0) {
    const res = http.get(`${BASE}/api/feed`, params(tok, 'feed', 'GET /api/feed'));
    check(res, { 'feed 200': (x) => x.status === 200 });
    if (res.status === 200) {
      for (const p of res.json('posts') || []) { seen.push(p.id); if (seen.length > 50) seen.shift(); }
    }
  } else if (r < 0.80) {
    track(http.post(`${BASE}/api/posts`, JSON.stringify({ text: text() }), params(tok, 'post', 'POST /api/posts')));
  } else if (r < 0.88) {
    track(http.post(`${BASE}/api/posts/${pick(seen)}/like`, null, params(tok, 'like', 'POST /api/posts/:id/like')));
  } else if (r < 0.93) {
    track(http.post(`${BASE}/api/follow/${randomUser(data.n)}`, null, params(tok, 'follow', 'POST /api/follow/:user')));
  } else if (r < 0.97) {
    track(http.post(`${BASE}/api/posts/${pick(seen)}/reply`, JSON.stringify({ text: text() }), params(tok, 'reply', 'POST /api/posts/:id/reply')));
  } else {
    http.get(`${BASE}/api/posts/${pick(seen)}/replies`, params(tok, 'replies', 'GET /api/posts/:id/replies'));
  }
}

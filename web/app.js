"use strict";

// Plain-JS client for the Mini Twitter API. All text is inserted with textContent, never innerHTML.
const $ = (id) => document.getElementById(id);
let token = null, me = null, nextCursor = null;

try { token = localStorage.getItem("mt_token"); me = JSON.parse(localStorage.getItem("mt_user") || "null"); } catch (_) {}

function toast(msg, isErr) {
  const t = $("toast");
  t.textContent = msg;
  t.className = "toast show" + (isErr ? " err" : "");
  clearTimeout(toast._t);
  toast._t = setTimeout(() => (t.className = "toast"), 2600);
}

async function api(method, path, body) {
  const headers = { "Content-Type": "application/json" };
  if (token) headers["Authorization"] = "Bearer " + token;
  const res = await fetch("/api" + path, { method, headers, body: body ? JSON.stringify(body) : undefined });
  let data = null;
  try { data = await res.json(); } catch (_) {}
  if (res.status === 401 && token && path !== "/login" && path !== "/signup") { setSession(null, null); }
  if (!res.ok) {
    const err = new Error((data && data.error) || "request failed (" + res.status + ")");
    err.status = res.status;
    err.requestId = res.headers.get("X-Request-Id");
    throw err;
  }
  return data;
}

function setSession(t, user) {
  token = t; me = user;
  try {
    if (t) { localStorage.setItem("mt_token", t); localStorage.setItem("mt_user", JSON.stringify(user)); }
    else { localStorage.removeItem("mt_token"); localStorage.removeItem("mt_user"); }
  } catch (_) {}
  render();
}

function fail(e) {
  // the request id lets you find the exact log line in Loki: {service="api"} | json | request_id="..."
  toast(e.message + (e.requestId ? "  (request " + e.requestId + ")" : ""), true);
}

function ago(iso) {
  const s = Math.max(0, (Date.now() - new Date(iso).getTime()) / 1000);
  if (s < 60) return Math.floor(s) + "s";
  if (s < 3600) return Math.floor(s / 60) + "m";
  if (s < 86400) return Math.floor(s / 3600) + "h";
  return Math.floor(s / 86400) + "d";
}

function el(tag, props, ...kids) {
  const n = document.createElement(tag);
  Object.assign(n, props || {});
  for (const k of kids) n.append(k);
  return n;
}

function postNode(p) {
  const likeBtn = el("button", { className: "ghost", textContent: "♥ " + p.likes });
  const replyBtn = el("button", { className: "ghost", textContent: "💬 " + p.replies });
  const box = el("div", { className: "replies hidden" });
  const node = el("div", { className: "card post" },
    el("div", { className: "head" },
      el("span", { className: "author", textContent: "@" + p.author }),
      el("span", { className: "muted", textContent: ago(p.created_at) })),
    el("div", { className: "text", textContent: p.text }),
    el("div", { className: "actions" }, likeBtn, replyBtn),
    box);

  let liked = false;
  likeBtn.onclick = async () => {
    if (liked) return;
    try { await api("POST", "/posts/" + p.id + "/like"); liked = true; p.likes++; likeBtn.textContent = "♥ " + p.likes; }
    catch (e) { fail(e); }
  };

  replyBtn.onclick = async () => {
    if (!box.classList.toggle("hidden")) await loadReplies(p, box, replyBtn);
  };
  return node;
}

async function loadReplies(p, box, replyBtn) {
  box.replaceChildren(el("div", { className: "muted", textContent: "loading..." }));
  try {
    const data = await api("GET", "/posts/" + p.id + "/replies");
    const input = el("input", { placeholder: "Reply...", maxLength: 280 });
    const send = el("button", { textContent: "Reply" });
    const list = el("div");
    for (const r of data.replies) list.append(replyNode(r));
    send.onclick = async () => {
      const text = input.value.trim();
      if (!text) return;
      try {
        const r = await api("POST", "/posts/" + p.id + "/reply", { text });
        list.prepend(replyNode({ author: me.username, text, created_at: r.created_at })); // optimistic
        input.value = ""; p.replies++; replyBtn.textContent = "💬 " + p.replies;
      } catch (e) { fail(e); }
    };
    box.replaceChildren(el("div", { className: "row" }, input, send), list);
  } catch (e) { box.replaceChildren(); fail(e); }
}

function replyNode(r) {
  return el("div", { className: "reply" },
    el("span", { className: "author", textContent: "@" + r.author + " " }),
    el("span", { className: "muted", textContent: ago(r.created_at) }),
    el("div", { textContent: r.text }));
}

async function loadFeed(more) {
  try {
    const q = more && nextCursor ? "?cursor=" + nextCursor : "";
    const data = await api("GET", "/feed" + q);
    const feed = $("feed");
    if (!more) feed.replaceChildren();
    for (const p of data.posts || []) feed.append(postNode(p));
    nextCursor = data.next_cursor || null;
    $("moreBtn").classList.toggle("hidden", !nextCursor);
    $("empty").classList.toggle("hidden", feed.children.length > 0);
  } catch (e) { fail(e); }
}

async function auth(kind) {
  try {
    const data = await api("POST", "/" + kind, { username: $("username").value, password: $("password").value });
    $("password").value = "";
    setSession(data.token, data.user);
  } catch (e) { fail(e); }
}

function render() {
  const on = !!token && !!me;
  $("auth").classList.toggle("hidden", on);
  $("app").classList.toggle("hidden", !on);
  $("who").classList.toggle("hidden", !on);
  if (on) { $("whoName").textContent = "@" + me.username; loadFeed(false); }
  else { $("feed").replaceChildren(); }
}

$("loginBtn").onclick = () => auth("login");
$("signupBtn").onclick = () => auth("signup");
$("password").addEventListener("keydown", (e) => { if (e.key === "Enter") auth("login"); });
$("logoutBtn").onclick = async () => {
  try { await api("POST", "/logout"); } catch (_) {}
  setSession(null, null);
};
$("refreshBtn").onclick = () => loadFeed(false);
$("moreBtn").onclick = () => loadFeed(true);
$("composer").addEventListener("input", () => ($("count").textContent = $("composer").value.length + " / 280"));

$("postBtn").onclick = async () => {
  const text = $("composer").value.trim();
  if (!text) return;
  $("postBtn").disabled = true;
  try {
    const r = await api("POST", "/posts", { text });
    // Optimistic render: the post is queued (202), not yet in the database or in followers' feeds,
    // but the author sees it immediately.
    $("feed").prepend(postNode({ id: r.id, author: me.username, text, likes: 0, replies: 0, created_at: r.created_at }));
    $("empty").classList.add("hidden");
    $("composer").value = ""; $("count").textContent = "0 / 280";
  } catch (e) { fail(e); }
  finally { $("postBtn").disabled = false; }
};

$("followBtn").onclick = async () => {
  const name = $("followName").value.trim().toLowerCase();
  if (!name) return;
  try {
    await api("POST", "/follow/" + encodeURIComponent(name));
    toast("Following @" + name + " (their posts appear after the next refresh)");
    $("followName").value = "";
  } catch (e) { fail(e); }
};

render();

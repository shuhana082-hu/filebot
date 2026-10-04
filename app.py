import os, re, json, html, time, hmac, hashlib, sqlite3, uuid, threading, mimetypes, datetime as dt
import requests
from flask import Flask, request, session, abort, Response
from werkzeug.security import generate_password_hash, check_password_hash

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", "change-this-secret")
app.config["SESSION_COOKIE_SAMESITE"] = "Lax"
ADMIN_PASS = os.environ.get("ADMIN_PASS", "admin123")
SECRET = hashlib.sha256(app.secret_key.encode()).hexdigest()[:32]
FB = bool(os.environ.get("FIREBASE_KEY"))  # set FIREBASE_KEY to keep data in Firestore (permanent); otherwise a local file is used
P = "fs_"
esc = lambda s: html.escape(str(s if s is not None else ""))
now = lambda: dt.datetime.now(dt.timezone.utc)

# ---------------- storage: Firestore (if FIREBASE_KEY) or local SQLite file ----------------
if FB:
    from google.cloud import firestore
    fs = firestore.Client.from_service_account_info(json.loads(os.environ["FIREBASE_KEY"]))

    def C(n): return fs.collection(P + n)
    def rows(n): return [dict(d.to_dict(), id=d.id) for d in C(n).stream()]
    def where(n, f, v): return [dict(d.to_dict(), id=d.id) for d in C(n).where(f, "==", v).stream()]
    def one(n, i):
        d = C(n).document(str(i)).get()
        return dict(d.to_dict(), id=d.id) if d.exists else None
    def save(n, i, data):
        if i:
            C(n).document(str(i)).set(data, merge=True)
            return str(i)
        ref = C(n).document()
        ref.set(data)
        return ref.id
    def drop(n, i): C(n).document(str(i)).delete()

    @firestore.transactional
    def _inc(tx, ref):
        v = (ref.get(transaction=tx).to_dict() or {}).get("n", 0) + 1
        tx.set(ref, {"n": v})
        return v
    def next_id(k): return _inc(fs.transaction(), C("meta").document(k))

    @firestore.transactional
    def _take(tx, pid):
        ds = list(C("codes").where("package_id", "==", pid).stream(transaction=tx))
        if not ds:
            return None
        d = min(ds, key=lambda x: x.get("seq"))
        tx.delete(d.reference)
        return d.get("code")
    def consume(pid): return _take(fs.transaction(), pid)

    def add_log(t, m): C("logs").add({"ts": now(), "type": t, "msg": m})
    def recent_logs(n):
        q = C("logs").order_by("ts", direction=firestore.Query.DESCENDING).limit(n).stream()
        return [(d.get("ts").strftime("%m-%d %H:%M:%S"), d.get("type"), d.get("msg")) for d in q]
    def trim_logs():
        for d in C("logs").order_by("ts", direction=firestore.Query.DESCENDING).offset(500).stream():
            d.reference.delete()
else:
    _dir = os.environ.get("DATA_DIR", os.path.dirname(os.path.abspath(__file__)))
    db = sqlite3.connect(os.path.join(_dir, "bot.db"), check_same_thread=False)
    db.execute("CREATE TABLE IF NOT EXISTS docs (col TEXT, id TEXT, data TEXT, PRIMARY KEY (col, id))")
    lk = threading.RLock()

    def rows(n):
        with lk:
            return [dict(json.loads(d), id=i) for i, d in db.execute("SELECT id, data FROM docs WHERE col=?", (P + n,)).fetchall()]
    def where(n, f, v): return [r for r in rows(n) if r.get(f) == v]
    def one(n, i):
        with lk:
            r = db.execute("SELECT data FROM docs WHERE col=? AND id=?", (P + n, str(i))).fetchone()
        return dict(json.loads(r[0]), id=str(i)) if r else None
    def save(n, i, data):
        i = str(i) if i else uuid.uuid4().hex[:10]
        with lk:
            old = one(n, i) or {}
            old.pop("id", None)
            old.update(data)
            db.execute("REPLACE INTO docs VALUES (?, ?, ?)", (P + n, i, json.dumps(old, default=str)))
            db.commit()
        return i
    def drop(n, i):
        with lk:
            db.execute("DELETE FROM docs WHERE col=? AND id=?", (P + n, str(i)))
            db.commit()
    def next_id(k):
        with lk:
            v = (one("meta", k) or {}).get("n", 0) + 1
            save("meta", k, {"n": v})
            return v
    def consume(pid):
        with lk:
            cs = where("codes", "package_id", pid)
            if not cs:
                return None
            c = min(cs, key=lambda x: x["seq"])
            drop("codes", c["id"])
            return c["code"]
    def add_log(t, m): save("logs", "%013d%s" % (time.time() * 1000, uuid.uuid4().hex[:3]), {"ts": now().strftime("%m-%d %H:%M:%S"), "type": t, "msg": m})
    def recent_logs(n): return [(r["ts"], r["type"], r["msg"]) for r in sorted(rows("logs"), key=lambda r: r["id"], reverse=True)[:n]]
    def trim_logs():
        for r in sorted(rows("logs"), key=lambda r: r["id"], reverse=True)[500:]:
            drop("logs", r["id"])

_cfg = {}
def cfg(k, d=""):
    if not _cfg:
        _cfg.update(one("config", "main") or {"_": 1})
    return _cfg.get(k, d)
def cfg_set(k, v):
    cfg("x")
    _cfg[k] = v
    save("config", "main", {k: v})

def log(msg, t="info"): add_log(t, str(msg)[:500])


# ---------------- products and stock ----------------
if FB:
    @firestore.transactional
    def _pop(tx, ref, unl):
        fl = (ref.get(transaction=tx).to_dict() or {}).get("files", [])
        if not fl:
            return None
        if not unl:
            tx.update(ref, {"files": fl[1:]})
        return fl[0]
    def pop_stock(pid, unl): return _pop(fs.transaction(), C("products").document(pid), unl)
else:
    def pop_stock(pid, unl):
        with lk:
            fl = (one("products", pid) or {}).get("files", [])
            if not fl:
                return None
            if not unl:
                save("products", pid, {"files": fl[1:]})
            return fl[0]

def packages(only_active=True):
    r = [p for p in rows("products") if p.get("active") or not only_active]
    return sorted(r, key=lambda p: p.get("name", ""))

# ---------------- files: stored on Telegram, only metadata is kept here ----------------
IMG = ("jpg", "jpeg", "png", "webp")
def add_file(fo, as_doc=False):
    name = os.path.basename(fo.filename or "file").replace(" ", "_")[:80]
    data = fo.read()
    if not data:
        raise ValueError("Empty file: " + name)
    if not cfg("bot_token") or not cfg("admin_id"):
        raise ValueError("Set Bot Token and Admin ID in Settings first (files are stored on Telegram)")
    photo = name.rsplit(".", 1)[-1].lower() in IMG and not as_doc
    try:
        r = requests.post("https://api.telegram.org/bot%s/%s" % (cfg("bot_token"), "sendPhoto" if photo else "sendDocument"), data={"chat_id": cfg("admin_id"), "caption": name},
                          files={"photo" if photo else "document": (name, data)}, timeout=180).json()
    except Exception as e:
        raise ValueError("%s: %s" % (name, e))
    if not r.get("ok"):
        raise ValueError("%s: %s (admin must press Start in the bot first; limits: photo 10 MB, file 50 MB)" % (name, r.get("description")))
    fid = r["result"]["photo"][-1]["file_id"] if photo else r["result"]["document"]["file_id"]
    return save("files", None, {"name": name, "kind": "photo" if photo else "doc", "fid": fid, "size": len(data), "ts": str(now())[:16]})

@app.route("/admin/f/<fid>")
def preview(fid):
    if not session.get("ok"):
        abort(401)
    r = one("files", fid)
    j = tg("getFile", file_id=r["fid"]) if r and r["kind"] == "photo" else {}
    if not j.get("ok"):
        abort(404)
    path = j["result"]["file_path"]
    img = requests.get("https://api.telegram.org/file/bot%s/%s" % (cfg("bot_token"), path), timeout=30)
    return Response(img.content, mimetype=mimetypes.guess_type(path)[0] or "image/jpeg", headers={"Cache-Control": "private, max-age=3600"})

# ---------------- bot logic ----------------
STATUS = {"pending": "পেন্ডিং", "completed": "সম্পন্ন", "rejected": "বাতিল"}
DEF_WELCOME = "স্বাগতম! এখান থেকে ডিজিটাল ফাইল কিনতে পারবেন। নিচের মেনু থেকে প্রোডাক্ট বেছে নিন।"

def get_session(chat): return one("sessions", chat) or {"step": "idle", "pid": None}
def set_session(chat, step, pid=None): save("sessions", chat, {"step": step, "pid": pid})

def pay_text():
    ns = rows("numbers")
    if not ns:
        return "এডমিন এখনো কোনো পেমেন্ট নাম্বার সেট করেননি।"
    return "\n".join("• %s (%s): <b>%s</b>" % (n["method"], n["type"], esc(n["number"])) for n in ns)

def order_lines(chat):
    os_ = sorted(where("orders", "chat_id", chat), key=lambda o: int(o["id"]), reverse=True)[:10]
    return "\n".join("#%s - %s - %s" % (o["id"], esc(o["product_name"]), STATUS.get(o["status"], o["status"])) for o in os_)

def help_text():
    L = ["/menu — সব প্রোডাক্টের মেনু", "/myorders — আপনার অর্ডার হিস্টোরি", "/help — এই লিস্ট"]
    L += ['/%s — "%s" প্রোডাক্ট দেখুন' % (p["command"], esc(p["name"])) for p in packages() if p.get("command")]
    L += ["/" + c["command"] for c in rows("commands") if c.get("active") and c["command"] != "start"]
    return "<b>সব কমান্ড</b>\n\n" + "\n".join(L)

def show_menu(chat, intro=""):
    ps = packages()
    pre = intro + "\n\n" if intro else ""
    if not ps:
        return send(chat, pre + "দুঃখিত, এই মুহূর্তে কোনো প্রোডাক্ট নেই।")
    kb = [[{"text": "%s - %g টাকা" % (p["name"], p["price"]), "callback_data": "p_" + p["id"]}] for p in ps]
    kb.append([{"text": "আমার অর্ডার", "callback_data": "orders"}, {"text": "পেমেন্ট নাম্বার", "callback_data": "pay"}])
    send(chat, pre + "<b>%s</b>\nপ্রোডাক্ট সিলেক্ট করুন:" % esc(cfg("shop_name", "File Store")), kb)

def show_product(chat, p):
    n = len(p.get("files", []))
    stock = "স্টকে আছে" if n and p.get("unlimited") else ("স্টক: %d টি" % n if n else "স্টক শেষ")
    cap = "<b>%s</b>\nমূল্য: %g টাকা\n%s\n\n%s" % (esc(p["name"]), p["price"], stock, esc(p.get("description", ""))[:700])
    kb = ([[{"text": "কিনুন", "callback_data": "buy_" + p["id"]}]] if n else []) + [[{"text": "মেনু", "callback_data": "menu"}]]
    ph = one("files", p["photo"]) if p.get("photo") else None
    if ph and tg("sendPhoto", chat_id=chat, photo=ph["fid"], caption=cap, parse_mode="HTML", reply_markup={"inline_keyboard": kb}).get("ok"):
        return
    send(chat, cap, kb)

def deliver(o):
    p = one("products", o["product_id"])
    if not p:
        return False
    unl = bool(p.get("unlimited"))
    fi = pop_stock(p["id"], unl)
    rec = one("files", fi) if fi else None
    if not rec:
        log("order #%s: no stock file for %s" % (o["id"], p["name"]), "err")
        return False
    r = tg("sendDocument", chat_id=o["chat_id"], document=rec["fid"], parse_mode="HTML",
           caption="অর্ডার #%s সম্পন্ন হয়েছে!\nপ্রোডাক্ট: %s\n\nধন্যবাদ আমাদের সাথে থাকার জন্য।" % (o["id"], esc(o["product_name"])))
    if not r.get("ok"):
        if not unl:
            save("products", p["id"], {"files": [fi] + (one("products", p["id"]) or {}).get("files", [])})
        log("order #%s: send failed: %s" % (o["id"], r.get("description")), "err")
        return False
    log("order #%s delivered" % o["id"], "in")
    return True

def approve(oid):
    o = one("orders", oid)
    if not o or o.get("status") != "pending":
        return "অর্ডার পেন্ডিং নেই।"
    if deliver(o):
        save("orders", oid, {"status": "completed"})
        return "অর্ডার #%s ডেলিভারি হয়েছে।" % oid
    return "অর্ডার #%s ডেলিভারি হয়নি (স্টক শেষ বা পাঠানো যায়নি)। Console দেখুন।" % oid

def reject(oid):
    o = one("orders", oid)
    if not o or o.get("status") != "pending":
        return "অর্ডার পেন্ডিং নেই।"
    save("orders", oid, {"status": "rejected"})
    send(o["chat_id"], "দুঃখিত, আপনার অর্ডার #%s বাতিল করা হয়েছে। সাহায্যের জন্য এডমিনের সাথে যোগাযোগ করুন।" % oid)
    return "অর্ডার #%s বাতিল করা হয়েছে।" % oid

def run_cmd(chat, c):
    t, msg = c["type"], c.get("message", "")
    if t == "text":
        return send(chat, msg or "ok")
    if t == "menu":
        return show_menu(chat, msg)
    if t == "order":
        p = one("products", c.get("pkg_id")) if c.get("pkg_id") else None
        return show_product(chat, p) if p and p.get("active") else send(chat, "এই প্রোডাক্টটি এখন পাওয়া যাচ্ছে না।")
    if t == "orders":
        return send(chat, (esc(msg) + "\n\n" if msg else "") + (order_lines(chat) or "আপনার কোনো অর্ডার নেই।"))
    if t == "paymentnumbers":
        return send(chat, (esc(msg) + "\n\n" if msg else "") + pay_text())

def on_message(m):
    chat, text = m["chat"]["id"], (m.get("text") or "").strip()
    fr = m.get("from", {})
    user = "@" + fr["username"] if fr.get("username") else fr.get("first_name", "User")
    log("msg (%s): %s" % (user, text), "in")
    name = text[1:].split(" ")[0].split("@")[0].lower() if text.startswith("/") else ""
    if name:
        for c in rows("commands"):
            if c.get("active") and c["command"] == name:
                return run_cmd(chat, c)
        if name == "start":
            set_session(chat, "idle")
            return show_menu(chat, esc(cfg("welcome") or DEF_WELCOME))
        if name in ("menu", "order"):
            return show_menu(chat)
        if name == "help":
            return send(chat, help_text())
        if name == "myorders":
            return send(chat, order_lines(chat) or "আপনার কোনো অর্ডার নেই।")
        for p in packages():
            if p.get("command") == name:
                return show_product(chat, p)
        return
    s = get_session(chat)
    if s["step"] != "awaiting_trx":
        return
    p = one("products", s["pid"]) if s.get("pid") else None
    if not p:
        set_session(chat, "idle")
        return send(chat, "সেশন শেষ হয়েছে, আবার /menu দিন।")
    if not re.fullmatch(r"[A-Za-z0-9]{6,15}", text):
        return send(chat, "সঠিক Transaction ID দিন (৬-১৫ ডিজিট/অক্ষর, স্পেস ছাড়া):")
    set_session(chat, "idle")
    oid = str(next_id("orderseq"))
    save("orders", oid, {"chat_id": chat, "username": user, "product_id": p["id"], "product_name": p["name"], "price": p["price"], "trx_id": text, "status": "pending", "created_at": str(now())[:19]})
    send(chat, "অর্ডার রিসিভ হয়েছে!\n\nঅর্ডার আইডি: #%s\nপ্রোডাক্ট: %s\nমূল্য: %g টাকা\nTransaction ID: %s\n\nপেমেন্ট যাচাই হলেই ফাইল পাঠিয়ে দেওয়া হবে।" % (oid, esc(p["name"]), p["price"], esc(text)))
    if cfg("admin_id"):
        send(cfg("admin_id"), "নতুন অর্ডার #%s\n\nইউজার: %s\nপ্রোডাক্ট: %s\nমূল্য: %g টাকা\nTransaction ID: %s\n\nপেমেন্ট চেক করে Approve চাপুন।" % (oid, esc(user), esc(p["name"]), p["price"], esc(text)),
             [[{"text": "Approve", "callback_data": "ap_" + oid}, {"text": "Reject", "callback_data": "rj_" + oid}]])

def on_cb(q):
    chat, data, cid = q["message"]["chat"]["id"], q.get("data", ""), q["id"]
    log("button: " + data, "in")
    if data[:3] in ("ap_", "rj_"):
        if str(q["from"]["id"]) != str(cfg("admin_id")):
            return tg("answerCallbackQuery", callback_query_id=cid, text="Not allowed")
        res = approve(data[3:]) if data[:2] == "ap" else reject(data[3:])
        tg("answerCallbackQuery", callback_query_id=cid, text=res[:150])
        return send(chat, esc(res))
    tg("answerCallbackQuery", callback_query_id=cid)
    if data == "menu":
        return show_menu(chat)
    if data == "orders":
        return send(chat, order_lines(chat) or "আপনার কোনো অর্ডার নেই।")
    if data == "pay":
        return send(chat, pay_text())
    p = one("products", data.split("_", 1)[1]) if data[:2] == "p_" or data[:4] == "buy_" else None
    if not p:
        return send(chat, "প্রোডাক্ট পাওয়া যায়নি।")
    if data[:2] == "p_":
        return show_product(chat, p)
    if not p.get("files"):
        return send(chat, "দুঃখিত, এই প্রোডাক্টের স্টক শেষ।")
    set_session(chat, "awaiting_trx", p["id"])
    send(chat, "<b>%s</b> - %g টাকা\n\nনিচের যেকোনো নাম্বারে <b>Send Money</b> করুন:\n\n%s\n\nপেমেন্ট করার পর আপনার <b>Transaction ID (TrxID)</b> এখানে টাইপ করে পাঠান।" % (esc(p["name"]), p["price"], pay_text()))

@app.route("/tg", methods=["POST"])
def webhook():
    if request.headers.get("X-Telegram-Bot-Api-Secret-Token") != SECRET:
        abort(403)
    u = request.get_json(force=True, silent=True) or {}
    try:
        if "message" in u:
            on_message(u["message"])
        elif "callback_query" in u:
            on_cb(u["callback_query"])
    except Exception as e:
        log("handler error: " + str(e), "err")
    return "ok"

@app.route("/")
@app.route("/ping")
def ping(): return "File Store Bot is running"

# ---------------- admin pages ----------------
NAV = [("dashboard", "Dashboard"), ("products", "Products"), ("files", "File Manager"), ("orders", "Orders"), ("numbers", "Numbers"), ("commands", "Commands"), ("settings", "Settings"), ("console", "Console")]
def kb(n): return "%.1f MB" % (n / 1048576) if n > 1048576 else "%d KB" % max(1, n // 1024)

def v_dashboard():
    o = rows("orders")
    st = [("Products", len(rows("products"))), ("Files", len(rows("files"))), ("Orders", len(o)), ("Pending", sum(1 for x in o if x.get("status") == "pending")), ("Storage", "Firestore" if FB else "Local file")]
    return "<h2>Dashboard</h2><div class=g>" + "".join("<div class=card><small>%s</small><h2>%s</h2></div>" % s for s in st) + "</div>"

def v_products():
    e = (one("products", request.args["edit"]) if request.args.get("edit") else None) or {}
    fl = rows("files")
    ph = sorted([x for x in fl if x["kind"] == "photo"], key=lambda x: x["name"])
    dc = sorted([x for x in fl if x["kind"] == "doc"], key=lambda x: x["name"])
    form = ('<form method=post enctype=multipart/form-data class=card>' + hid(action="save_product", pid=e.get("id", "")) + "<div class=g>" + fld("name", "Product Name", e.get("name", "")) + fld("price", "Price", e.get("price", ""), "number") +
            fld("command", "Command (optional, without /)", e.get("command", "")) + "</div>" + area("description", "Description", e.get("description", "")) + "<div class=g>" +
            sel("photo", "Photo from File Manager" + (" (empty = keep current)" if e.get("photo") else ""), [("", "-")] + [(x["id"], x["name"]) for x in ph]) +
            '<label>Or upload a new photo<input type=file name=new_photo accept="image/*"></label></div><div class=g><label>Stock files from File Manager (Ctrl / long-press to select many)<select name=stock multiple size=6>' +
            "".join('<option value="%s">%s</option>' % (x["id"], esc(x["name"])) for x in dc) + '</select></label><label>Or upload new stock files<input type=file name=new_files multiple></label></div>' +
            chk("unlimited", "Unlimited: send the first file to every buyer (file is not used up)", e.get("unlimited")) + chk("active", "Active", e.get("active", True)) + "<button class=b>Save</button></form>")
    cs = ""
    if e:
        cs = "<div class=card><b>Stock files (%d)</b>" % len(e.get("files", [])) + tbl(["File", ""], [[esc((one("files", i) or {}).get("name", "(deleted)")), act("remove_stock", "Remove", "b3", "Remove from stock?", pid=e["id"], fid=i)] for i in e.get("files", [])]) + "</div>"
    rs = [[esc(p["name"]), "%g" % p["price"], "unlimited" if p.get("unlimited") else len(p.get("files", [])), "Active" if p.get("active") else "Off",
           edit("products", p["id"]) + act("toggle_product", "On/Off", pid=p["id"]) + act("delete_product", "Delete", "b3", "Delete product?", pid=p["id"])] for p in packages(False)]
    return "<h2>Products</h2>" + form + cs + tbl(["Name", "Price", "Stock", "Status", ""], rs)

def h_products(f):
    a = f.get("action")
    if a == "save_product":
        pid = f.get("pid") or None
        old = (one("products", pid) or {}) if pid else {}
        d = {"name": f["name"].strip(), "price": float(f.get("price") or 0), "command": f.get("command", "").strip().lstrip("/").lower(),
             "description": f.get("description", "").strip(), "unlimited": "unlimited" in f, "active": "active" in f}
        try:
            ph, up = f.get("photo"), request.files.get("new_photo")
            if up and up.filename:
                ph = add_file(up)
            if ph:
                d["photo"] = ph
            ids = [i for i in f.getlist("stock") if i] + [add_file(x, True) for x in request.files.getlist("new_files") if x.filename]
        except ValueError as e:
            return "Not saved: " + str(e)
        d["files"] = old.get("files", []) + [i for i in ids if i not in old.get("files", [])]
        save("products", pid, d)
        return "Product saved."
    if a == "delete_product":
        drop("products", f["pid"])
        return "Product deleted."
    if a == "toggle_product":
        p = one("products", f["pid"])
        save("products", p["id"], {"active": not p.get("active")})
    if a == "remove_stock":
        p = one("products", f["pid"])
        save("products", p["id"], {"files": [i for i in p.get("files", []) if i != f["fid"]]})

def v_files():
    form = ('<form method=post enctype=multipart/form-data class=card>' + hid(action="upload") + '<label>Upload files (photos, zip, pdf, apk, anything up to 50 MB)<input type=file name=files multiple></label>' +
            chk("as_doc", "Send images as a file (original quality, not usable as product photo)", False) + "<button class=b>Upload</button></form>")
    rs = []
    for x in sorted(rows("files"), key=lambda x: x.get("ts", ""), reverse=True):
        th = '<img src="/admin/f/%s" style="height:44px;border-radius:8px">' % x["id"] if x["kind"] == "photo" else "file"
        rs.append([th, esc(x["name"]), "Photo" if x["kind"] == "photo" else "File", kb(x.get("size", 0)), x.get("ts", ""), act("delete_file", "Delete", "b3", "Delete file?", fid=x["id"])])
    return "<h2>File Manager</h2>" + form + tbl(["", "Name", "Type", "Size", "Uploaded", ""], rs)

def h_files(f):
    if f.get("action") == "upload":
        ok, bad = 0, []
        for x in request.files.getlist("files"):
            if x.filename:
                try:
                    add_file(x, "as_doc" in f)
                    ok += 1
                except ValueError as e:
                    bad.append(str(e))
        return "Uploaded %d file(s)." % ok + (" Failed: " + " | ".join(bad) if bad else "")
    if f.get("action") == "delete_file":
        drop("files", f["fid"])
        for p in rows("products"):
            save("products", p["id"], {"files": [i for i in p.get("files", []) if i != f["fid"]], "photo": "" if p.get("photo") == f["fid"] else p.get("photo", "")})
        return "File deleted."

def v_orders():
    flt = request.args.get("status", "all")
    os_ = [o for o in sorted(rows("orders"), key=lambda o: int(o["id"]), reverse=True) if flt == "all" or o.get("status") == flt][:100]
    links = " ".join('<a class=b2 href="/admin/orders?status=%s">%s</a>' % (s, s) for s in ("all", "pending", "completed", "rejected"))
    rs = [["#" + o["id"], esc(o.get("username")), esc(o.get("product_name")), "%g" % o.get("price", 0), esc(o.get("trx_id")), STATUS.get(o.get("status"), o.get("status")), o.get("created_at", ""),
           (act("approve_order", "Approve and send file", "b", order_id=o["id"]) + act("reject_order", "Reject", "b3", order_id=o["id"])) if o.get("status") == "pending" else ""] for o in os_]
    return "<h2>Orders</h2><p>" + links + "</p>" + tbl(["ID", "User", "Product", "Price", "TrxID", "Status", "Time", ""], rs)

def h_orders(f):
    if f.get("action") == "approve_order":
        return approve(f["order_id"])
    if f.get("action") == "reject_order":
        return reject(f["order_id"])

def v_settings():
    return "<h2>Settings</h2>" + box(fld("bot_token", "Bot Token", cfg("bot_token")) + fld("admin_id", "Admin Telegram ID (open the bot and press Start once)", cfg("admin_id")) + fld("shop_name", "Shop name", cfg("shop_name", "File Store")) +
                                     area("welcome", "Welcome message (/start)", cfg("welcome") or DEF_WELCOME, 4) + fld("new_password", "New admin password (empty = unchanged)", "", "password") + fld("new_password_confirm", "Confirm password", "", "password")) + \
        '<div class=card><b>Telegram webhook</b><p><small>Save the bot token first, then press the button once (again if your Render URL changes).</small></p>%s</div>' % act("webhook", "Set Webhook", "b")

def h_settings(f):
    if f.get("action") == "webhook":
        base = os.environ.get("RENDER_EXTERNAL_URL") or request.url_root.rstrip("/").replace("http://", "https://")
        r = tg("setWebhook", url=base + "/tg", secret_token=SECRET)
        return "Webhook: " + str(r.get("description") or r.get("ok"))
    for k in ("bot_token", "admin_id", "shop_name", "welcome"):
        cfg_set(k, f.get(k, "").strip())
    if f.get("new_password"):
        if f["new_password"] != f.get("new_password_confirm"):
            return "Settings saved, but the passwords did not match."
        cfg_set("admin_password_hash", generate_password_hash(f["new_password"]))
    return "Settings saved."

# ---------------- telegram ----------------
def tg(method, **p):
    t = cfg("bot_token")
    if not t:
        return {"ok": False, "description": "Bot token not set"}
    try:
        return requests.post("https://api.telegram.org/bot" + t + "/" + method, json=p, timeout=20).json()
    except Exception as e:
        return {"ok": False, "description": str(e)}

def send(chat, text, kb=None):
    p = {"chat_id": chat, "text": text, "parse_mode": "HTML"}
    if kb:
        p["reply_markup"] = {"inline_keyboard": kb}
    log("sent: " + str(chat), "out")
    return tg("sendMessage", **p)

CSS = """*{box-sizing:border-box}body{margin:0;background:#070a12;color:#e6ecf5;font-family:system-ui,'Noto Sans Bengali',sans-serif}
nav{display:flex;gap:6px;flex-wrap:wrap;padding:12px 16px;border-bottom:1px solid #1c2433;background:#0b1019;position:sticky;top:0}
nav a{color:#8b97a8;text-decoration:none;padding:8px 14px;border-radius:10px;transition:.2s}nav a:hover,nav a.on{background:#00e5ff22;color:#00e5ff}
main{max-width:980px;margin:auto;padding:18px}h2{margin:.3em 0}.g{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:12px}
.card{background:#0f1623;border:1px solid #1c2433;border-radius:14px;padding:16px;margin:12px 0;animation:u .4s}@keyframes u{from{opacity:0;transform:translateY(10px)}}
label{display:block;font-size:12px;color:#8b97a8;margin:8px 0}input,select,textarea{width:100%;margin-top:4px;padding:10px;border-radius:10px;border:1px solid #243049;background:#0a0f1a;color:#e6ecf5;font:inherit}
.ck{display:flex;gap:8px;align-items:center}.ck input{width:auto;margin:0}.b,.b2,.b3{border:0;border-radius:10px;padding:9px 16px;cursor:pointer;font:inherit;font-weight:600;background:#00e5ff;color:#001018;text-decoration:none;display:inline-block}
.b2{background:#1c2638;color:#e6ecf5}.b3{background:#3b1219;color:#fca5a5}.t{overflow:auto}table{width:100%;border-collapse:collapse;font-size:14px}
td,th{padding:9px;border-bottom:1px solid #1c2433;text-align:left;vertical-align:top}th{color:#8b97a8;font-weight:500}.m{background:#00e5ff22;border:1px solid #00e5ff55;padding:10px 14px;border-radius:10px;margin:12px 0}
small{color:#8b97a8}form.i{display:inline}"""

def layout(cur, body, msg=""):
    nav = "".join('<a href="/admin/%s" class="%s">%s</a>' % (k, "on" if k == cur else "", v) for k, v in NAV) + '<a href="/admin/logout" style="margin-left:auto">Logout</a>'
    return "<!doctype html><meta charset=utf-8><meta name=viewport content='width=device-width,initial-scale=1'><title>Admin</title><style>%s</style><nav>%s</nav><main>%s%s</main>" % (CSS, nav, '<div class=m>%s</div>' % esc(msg) if msg else "", body)

def fld(n, l, v="", t="text", ph=""): return '<label>%s<input name="%s" type="%s" value="%s" placeholder="%s"></label>' % (l, n, t, esc(v), ph)
def area(n, l, v="", r=4): return '<label>%s<textarea name="%s" rows="%d">%s</textarea></label>' % (l, n, r, esc(v))
def sel(n, l, opts, v=""): return '<label>%s<select name="%s">%s</select></label>' % (l, n, "".join('<option value="%s"%s>%s</option>' % (esc(a), " selected" if str(a) == str(v) else "", esc(b)) for a, b in opts))
def chk(n, l, v): return '<label class=ck><input type=checkbox name="%s"%s> %s</label>' % (n, " checked" if v else "", l)
def hid(**kw): return "".join('<input type=hidden name="%s" value="%s">' % (k, esc(v)) for k, v in kw.items())
def act(action, label, cls="b2", conf="", **kw):
    oc = ' onsubmit="return confirm(\'%s\')"' % conf if conf else ""
    return '<form method=post class=i%s>%s<button class=%s>%s</button></form>' % (oc, hid(action=action, **kw), cls, label)
def box(body, btn="Save"): return '<form method=post class=card>%s<button class=b>%s</button></form>' % (body, btn)
def tbl(head, body): return "<div class=t><table><tr>%s</tr>%s</table></div>" % ("".join("<th>%s</th>" % h for h in head), "".join("<tr>%s</tr>" % "".join("<td>%s</td>" % c for c in r) for r in body))
def edit(page, i): return '<a class=b2 href="/admin/%s?edit=%s">Edit</a>' % (page, i)

def v_numbers():
    e = (one("numbers", request.args["edit"]) if request.args.get("edit") else None) or {}
    form = box(hid(action="save_number", num_id=e.get("id", "")) + "<div class=g>" + sel("method", "Method", [(x, x) for x in ("bKash", "Nagad", "Rocket")], e.get("method")) +
               sel("type", "Type", [(x, x) for x in ("Personal", "Agent", "Merchant")], e.get("type")) + "</div>" + fld("number", "Number", e.get("number", ""), ph="01XXXXXXXXX"))
    return "<h2>Payment Numbers</h2>" + form + tbl(["Method", "Type", "Number", ""], [[n["method"], n["type"], esc(n["number"]), edit("numbers", n["id"]) + act("delete_number", "Delete", "b3", "Delete?", num_id=n["id"])] for n in rows("numbers")])

def h_numbers(f):
    if f.get("action") == "save_number":
        save("numbers", f.get("num_id") or None, {"method": f["method"], "type": f["type"], "number": f["number"].strip()})
        return "Number saved."
    if f.get("action") == "delete_number":
        drop("numbers", f["num_id"])

def v_commands():
    e = (one("commands", request.args["edit"]) if request.args.get("edit") else None) or {}
    types = [("text", "Text Reply"), ("menu", "Show Package Menu"), ("order", "Open Product"), ("orders", "My Orders History"), ("paymentnumbers", "Payment Numbers")]
    form = box(hid(action="save_command", cmd_id=e.get("id", "")) + "<div class=g>" + fld("command", "Command (without /)", e.get("command", ""), ph="support") + sel("type", "Type", types, e.get("type")) +
               sel("pkg_id", "Product (for Open Product)", [(p["id"], p["name"]) for p in packages(False)], e.get("pkg_id")) + "</div>" +
               area("message", "Message (reply text, or intro line for other types)", e.get("message", "")) + chk("active", "Active", e.get("active", True)))
    return "<h2>Commands</h2>" + form + tbl(["Command", "Type", "Message", "Status", ""], [["/" + esc(c["command"]), c["type"], esc(c.get("message", ""))[:60], "Active" if c.get("active") else "Off",
                                                                                      edit("commands", c["id"]) + act("delete_command", "Delete", "b3", "Delete?", cmd_id=c["id"])] for c in rows("commands")])

def h_commands(f):
    if f.get("action") == "save_command":
        t = f["type"]
        save("commands", f.get("cmd_id") or None, {"command": f["command"].strip().lstrip("/").lower(), "type": t, "message": f.get("message", "").strip(),
                                                    "pkg_id": f.get("pkg_id") if t == "order" else None, "active": "active" in f})
        return "Command saved."
    if f.get("action") == "delete_command":
        drop("commands", f["cmd_id"])

def v_console():
    return "<h2>Bot Console</h2>" + tbl(["Time (UTC)", "Type", "Message"], [[a, b, esc(c)] for a, b, c in recent_logs(100)])

PAGES = {"dashboard": (v_dashboard, None), "products": (v_products, h_products), "files": (v_files, h_files), "orders": (v_orders, h_orders), "numbers": (v_numbers, h_numbers),
         "commands": (v_commands, h_commands), "settings": (v_settings, h_settings), "console": (v_console, None)}

@app.route("/admin", methods=["GET", "POST"])
@app.route("/admin/<page>", methods=["GET", "POST"])
def admin(page="dashboard"):
    if page == "logout":
        session.clear()
        return "", 302, {"Location": "/admin"}
    if not session.get("ok"):
        err = ""
        if request.method == "POST":
            pw, h = request.form.get("password", ""), cfg("admin_password_hash")
            if check_password_hash(h, pw) if h else hmac.compare_digest(pw.encode(), ADMIN_PASS.encode()):
                session["ok"] = True
                return "", 302, {"Location": "/admin"}
            err = "Wrong password."
        return "<!doctype html><meta charset=utf-8><meta name=viewport content='width=device-width,initial-scale=1'><style>%s</style><main style='max-width:340px;margin-top:18vh'><form method=post class=card><h2>Admin Login</h2>%s<button class=b style='width:100%%'>Sign in</button><p style=color:#f87171>%s</p></form></main>" % (CSS, fld("password", "Password", "", "password"), err)
    if page not in PAGES:
        abort(404)
    msg = ""
    if request.method == "POST" and PAGES[page][1]:
        msg = PAGES[page][1](request.form) or ""
    return layout(page, PAGES[page][0](), msg)

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 5000)))

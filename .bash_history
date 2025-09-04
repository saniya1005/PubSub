    # query params: q (search), sort (one of listed), order (asc/desc), limit (int)
    q     = qparam("q", "")
    sort  = qparam("sort", "TransactionDateTime")
    order = qparam("order", "desc").lower()
    limit = int(qparam("limit", "50"))

    allowed_sort = {"ItemID","Location","Quantity","TransactionDateTime","TransactionNumber"}
    if sort not in allowed_sort:
        sort = "TransactionDateTime"
    if order not in {"asc","desc"}:
        order = "desc"

    where = []
    args  = []
    if q:
        # simple search across a few columns
        where.append("(ItemID LIKE ? OR Location LIKE ? OR TransactionNumber LIKE ?)")
        args += [f"%{q}%", f"%{q}%", f"%{q}%"]
    sql = "SELECT ItemID,Location,Quantity,TransactionDateTime,TransactionNumber,pubsub_message_id FROM messages"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += f" ORDER BY {sort} {order} LIMIT ?"
    args.append(limit)

    con = sqlite3.connect(DB_PATH)
    try:
        cur = con.execute(sql, args)
        cols = [d[0] for d in cur.description]
        data = rows_to_dicts(cur.fetchall(), cols)
        return jsonify(data)
    finally:
        con.close()

@app.post("/publish")
def publish():
    # accept JSON or form
    if request.is_json:
        body = request.get_json(force=True)
    else:
        body = request.form.to_dict()

    # fill required fields if missing
    msg = {
        "ItemID":              body.get("ItemID") or "0",
        "Location":            body.get("Location") or "ATL",
        "Quantity":            int(body.get("Quantity") or 1),
        "TransactionDateTime": body.get("TransactionDateTime") or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "TransactionNumber":   body.get("TransactionNumber") or f"TXN-{uuid.uuid4().hex[:12]}",
    }

    publisher = pubsub_v1.PublisherClient()
    topic     = publisher.topic_path(PROJECT_ID, TOPIC_ID)
    mid = publisher.publish(topic, json.dumps(msg).encode("utf-8"), source="flask_ui").result()
    return jsonify({"status":"ok","message_id":mid,"published":msg})

@app.get("/")
def index():
    # simple one-page UI: publish form + table with sort/filter
    html = """
<!doctype html>
<meta charset="utf-8">
<title>Messages Viewer</title>
<style>
body{font:14px/1.3 system-ui,Segoe UI,Arial;padding:20px;max-width:900px;margin:auto}
form, .toolbar{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:12px}
input, select, button{padding:6px 8px}
table{border-collapse:collapse;width:100%}
th, td{border:1px solid #ddd;padding:6px 8px}
th{cursor:pointer;background:#f6f6f6}
</style>
<h1>Pub/Sub Demo – Publisher & Messages</h1>

<h3>Publish a message</h3>
<form id="pubForm">
  <input name="ItemID" placeholder="ItemID" value="123">
  <input name="Location" placeholder="Location" value="Atlanta">
  <input name="Quantity" placeholder="Quantity" type="number" value="1" min="0">
  <input name="TransactionDateTime" placeholder="YYYY-MM-DD HH:MM:SS">
  <input name="TransactionNumber" placeholder="TransactionNumber (optional)">
  <button type="submit">Publish</button>
</form>
<pre id="pubResult"></pre>

<div class="toolbar">
  <input id="q" placeholder="Search (ItemID/Location/Txn)">
  <label>Sort
    <select id="sort">
      <option>TransactionDateTime</option>
      <option>Quantity</option>
      <option>ItemID</option>
      <option>Location</option>
      <option>TransactionNumber</option>
    </select>
  </label>
  <label>Order
    <select id="order">
      <option value="desc">desc</option>
      <option value="asc">asc</option>
    </select>
  </label>
  <label>Limit
    <select id="limit">
      <option>20</option><option selected>50</option><option>100</option>
    </select>
  </label>
  <button id="refresh">Refresh</button>
</div>

<table id="tbl">
  <thead>
    <tr>
      <th>ItemID</th><th>Location</th><th>Quantity</th><th>TransactionDateTime</th><th>TransactionNumber</th><th>pubsub_message_id</th>
    </tr>
  </thead>
  <tbody></tbody>
</table>

<script>
const $ = sel => document.querySelector(sel);
async function load() {
  const q = $('#q').value.trim();
  const params = new URLSearchParams({
    q, sort: $('#sort').value, order: $('#order').value, limit: $('#limit').value
  });
  const res = await fetch('/api/messages?'+params.toString());
  const data = await res.json();
  const tbody = $('#tbl tbody');
  tbody.innerHTML = '';
  for (const r of data) {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${r.ItemID??''}</td>
      <td>${r.Location??''}</td>
      <td>${r.Quantity??''}</td>
      <td>${r.TransactionDateTime??''}</td>
      <td>${r.TransactionNumber??''}</td>
      <td>${r.pubsub_message_id??''}</td>`;
    tbody.appendChild(tr);
  }
}
$('#refresh').onclick = load;
$('#pubForm').onsubmit = async (e) => {
  e.preventDefault();
  const fd = new FormData(e.target);
  const res = await fetch('/publish', { method:'POST', body: fd });
  const json = await res.json();
  $('#pubResult').textContent = JSON.stringify(json, null, 2);
  setTimeout(load, 500);
};
load();
</script>
"""
    return Response(html, mimetype="text/html")

if __name__ == "__main__":
    # For Cloud Shell preview
    app.run(host="0.0.0.0", port=8080, debug=False)
PY

cat > app.py <<'PY'
from flask import Flask, request, jsonify, render_template
from google.cloud import pubsub_v1
import sqlite3, json, uuid
from datetime import datetime

PROJECT_ID = "capstone-project-470501"
TOPIC_ID   = "KSU_Team_4"
DB_PATH    = "messages.db"

app = Flask(__name__)

def rows_to_dicts(rows, cols):
    return [dict(zip(cols, r)) for r in rows]

def qparam(name, default=None):
    return request.args.get(name, default)

@app.get("/api/messages")
def api_messages():
    q     = qparam("q", "")
    sort  = qparam("sort", "TransactionDateTime")
    order = (qparam("order", "desc") or "desc").lower()
    limit = int(qparam("limit", "50"))

    allowed_sort = {"ItemID","Location","Quantity","TransactionDateTime","TransactionNumber"}
    if sort not in allowed_sort:
        sort = "TransactionDateTime"
    if order not in {"asc","desc"}:
        order = "desc"

    where, args = [], []
    if q:
        where.append("(ItemID LIKE ? OR Location LIKE ? OR TransactionNumber LIKE ?)")
        args += [f"%{q}%", f"%{q}%", f"%{q}%"]

    sql = "SELECT ItemID,Location,Quantity,TransactionDateTime,TransactionNumber,pubsub_message_id FROM messages"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += f" ORDER BY {sort} {order} LIMIT ?"
    args.append(limit)

    con = sqlite3.connect(DB_PATH)
    try:
        cur = con.execute(sql, args)
        cols = [d[0] for d in cur.description]
        data = rows_to_dicts(cur.fetchall(), cols)
        return jsonify(data)
    finally:
        con.close()

@app.post("/publish")
def publish():
    body = request.get_json(silent=True) or request.form.to_dict()
    msg = {
        "ItemID":              body.get("ItemID") or "0",
        "Location":            body.get("Location") or "ATL",
        "Quantity":            int(body.get("Quantity") or 1),
        "TransactionDateTime": body.get("TransactionDateTime") or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "TransactionNumber":   body.get("TransactionNumber") or f"TXN-{uuid.uuid4().hex[:12]}",
    }
    publisher = pubsub_v1.PublisherClient()
    topic     = publisher.topic_path(PROJECT_ID, TOPIC_ID)
    mid = publisher.publish(topic, json.dumps(msg).encode("utf-8"), source="flask_ui").result()
    return jsonify({"status":"ok","message_id":mid,"published":msg})

@app.get("/")
def index():
    return render_template("index.html")

if __name__ == "__main__":
    # Cloud Shell preview uses port 8080
    app.run(host="0.0.0.0", port=8080, debug=False)
PY

cat > app.py <<'PY'
from flask import Flask, request, jsonify, render_template
from google.cloud import pubsub_v1
import sqlite3, json, uuid
from datetime import datetime

PROJECT_ID = "capstone-project-470501"
TOPIC_ID   = "KSU_Team_4"
DB_PATH    = "messages.db"

app = Flask(__name__)

def rows_to_dicts(rows, cols):
    return [dict(zip(cols, r)) for r in rows]

def qparam(name, default=None):
    return request.args.get(name, default)

@app.get("/api/messages")
def api_messages():
    q     = qparam("q", "")
    sort  = qparam("sort", "TransactionDateTime")
    order = (qparam("order", "desc") or "desc").lower()
    limit = int(qparam("limit", "50"))

    allowed_sort = {"ItemID","Location","Quantity","TransactionDateTime","TransactionNumber"}
    if sort not in allowed_sort:
        sort = "TransactionDateTime"
    if order not in {"asc","desc"}:
        order = "desc"

    where, args = [], []
    if q:
        where.append("(ItemID LIKE ? OR Location LIKE ? OR TransactionNumber LIKE ?)")
        args += [f"%{q}%", f"%{q}%", f"%{q}%"]

    sql = "SELECT ItemID,Location,Quantity,TransactionDateTime,TransactionNumber,pubsub_message_id FROM messages"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += f" ORDER BY {sort} {order} LIMIT ?"
    args.append(limit)

    con = sqlite3.connect(DB_PATH)
    try:
        cur = con.execute(sql, args)
        cols = [d[0] for d in cur.description]
        data = rows_to_dicts(cur.fetchall(), cols)
        return jsonify(data)
    finally:
        con.close()

@app.post("/publish")
def publish():
    body = request.get_json(silent=True) or request.form.to_dict()
    msg = {
        "ItemID":              body.get("ItemID") or "0",
        "Location":            body.get("Location") or "ATL",
        "Quantity":            int(body.get("Quantity") or 1),
        "TransactionDateTime": body.get("TransactionDateTime") or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "TransactionNumber":   body.get("TransactionNumber") or f"TXN-{uuid.uuid4().hex[:12]}",
    }
    publisher = pubsub_v1.PublisherClient()
    topic     = publisher.topic_path(PROJECT_ID, TOPIC_ID)
    mid = publisher.publish(topic, json.dumps(msg).encode("utf-8"), source="flask_ui").result()
    return jsonify({"status":"ok","message_id":mid,"published":msg})

@app.get("/")
def index():
    return render_template("index.html")

if __name__ == "__main__":
    # Cloud Shell preview uses port 8080
    app.run(host="0.0.0.0", port=8080, debug=False)
PY

PY
cat > app.py <<'PY'
from flask import Flask, request, jsonify, render_template
from google.cloud import pubsub_v1
import sqlite3, json, uuid
from datetime import datetime

PROJECT_ID = "capstone-project-470501"
TOPIC_ID   = "KSU_Team_4"
DB_PATH    = "messages.db"

app = Flask(__name__)

def rows_to_dicts(rows, cols):
    return [dict(zip(cols, r)) for r in rows]

def qparam(name, default=None):
    return request.args.get(name, default)

@app.get("/api/messages")
def api_messages():
    q     = qparam("q", "")
    sort  = qparam("sort", "TransactionDateTime")
    order = (qparam("order", "desc") or "desc").lower()
    limit = int(qparam("limit", "50"))

    allowed_sort = {"ItemID","Location","Quantity","TransactionDateTime","TransactionNumber"}
    if sort not in allowed_sort:
        sort = "TransactionDateTime"
    if order not in {"asc","desc"}:
        order = "desc"

    where, args = [], []
    if q:
        where.append("(ItemID LIKE ? OR Location LIKE ? OR TransactionNumber LIKE ?)")
        args += [f"%{q}%", f"%{q}%", f"%{q}%"]

    sql = "SELECT ItemID,Location,Quantity,TransactionDateTime,TransactionNumber,pubsub_message_id FROM messages"
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += f" ORDER BY {sort} {order} LIMIT ?"
    args.append(limit)

    con = sqlite3.connect(DB_PATH)
    try:
        cur = con.execute(sql, args)
        cols = [d[0] for d in cur.description]
        data = rows_to_dicts(cur.fetchall(), cols)
        return jsonify(data)
    finally:
        con.close()

@app.post("/publish")
def publish():
    body = request.get_json(silent=True) or request.form.to_dict()
    msg = {
        "ItemID":              body.get("ItemID") or "0",
        "Location":            body.get("Location") or "ATL",
        "Quantity":            int(body.get("Quantity") or 1),
        "TransactionDateTime": body.get("TransactionDateTime") or datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "TransactionNumber":   body.get("TransactionNumber") or f"TXN-{uuid.uuid4().hex[:12]}",
    }
    publisher = pubsub_v1.PublisherClient()
    topic     = publisher.topic_path(PROJECT_ID, TOPIC_ID)
    mid = publisher.publish(topic, json.dumps(msg).encode("utf-8"), source="flask_ui").result()
    return jsonify({"status":"ok","message_id":mid,"published":msg})

@app.get("/")
def index():
    return render_template("index.html")

if __name__ == "__main__":
    # Cloud Shell preview uses port 8080
    app.run(host="0.0.0.0", port=8080, debug=False)
PY

mkdir -p templates
cat > templates/index.html <<'HTML'
<!doctype html>
<meta charset="utf-8">
<title>Messages Viewer</title>
<style>
body{font:14px/1.4 system-ui,Segoe UI,Arial;padding:20px;max-width:900px;margin:auto}
form,.toolbar{display:flex;gap:8px;flex-wrap:wrap;margin-bottom:12px}
input,select,button{padding:6px 8px}
table{border-collapse:collapse;width:100%}
th,td{border:1px solid #ddd;padding:6px 8px}
th{background:#f6f6f6}
.dup{background:#ffe6e6}
</style>

<h1>Pub/Sub Demo – Publisher & Messages</h1>

<h3>Publish a message</h3>
<form id="pubForm">
  <input name="ItemID" placeholder="ItemID" value="123">
  <input name="Location" placeholder="Location" value="Atlanta">
  <input name="Quantity" placeholder="Quantity" type="number" value="1" min="0">
  <input name="TransactionDateTime" placeholder="YYYY-MM-DD HH:MM:SS">
  <input name="TransactionNumber" placeholder="TransactionNumber (optional)">
  <button type="submit">Publish</button>
</form>
<pre id="pubResult"></pre>

<div class="toolbar">
  <input id="q" placeholder="Search (ItemID/Location/Txn)">
  <label>Sort
    <select id="sort">
      <option>TransactionDateTime</option>
      <option>Quantity</option>
      <option>ItemID</option>
      <option>Location</option>
      <option>TransactionNumber</option>
    </select>
  </label>
  <label>Order
    <select id="order">
      <option value="desc" selected>desc</option>
      <option value="asc">asc</option>
    </select>
  </label>
  <label>Limit
    <select id="limit">
      <option>20</option><option selected>50</option><option>100</option>
    </select>
  </label>
  <button id="refresh">Refresh</button>
</div>

<table id="tbl">
  <thead>
    <tr>
      <th>ItemID</th><th>Location</th><th>Quantity</th>
      <th>TransactionDateTime</th><th>TransactionNumber</th><th>pubsub_message_id</th>
    </tr>
  </thead>
  <tbody></tbody>
</table>

<script>
const $ = s => document.querySelector(s);

async function load() {
  const params = new URLSearchParams({
    q: $('#q').value.trim(),
    sort: $('#sort').value,
    order: $('#order').value,
    limit: $('#limit').value
  });
  const res = await fetch('/api/messages?' + params.toString());
  const data = await res.json();

  // find duplicates by TransactionNumber (for highlighting)
  const seen = new Map();
  for (const row of data) {
    const k = row.TransactionNumber;
    seen.set(k, (seen.get(k) || 0) + 1);
  }

  const tbody = $('#tbl tbody');
  tbody.innerHTML = '';
  for (const r of data) {
    const tr = document.createElement('tr');
    if (r.TransactionNumber && seen.get(r.TransactionNumber) > 1) tr.classList.add('dup');
    tr.innerHTML = `
      <td>${r.ItemID ?? ''}</td>
      <td>${r.Location ?? ''}</td>
      <td>${r.Quantity ?? ''}</td>
      <td>${r.TransactionDateTime ?? ''}</td>
      <td>${r.TransactionNumber ?? ''}</td>
      <td>${r.pubsub_message_id ?? ''}</td>
    `;
    tbody.appendChild(tr);
  }
}

$('#refresh').onclick = load;

$('#pubForm').onsubmit = async (e) => {
  e.preventDefault();
  const fd = new FormData(e.target);
  const res = await fetch('/publish', { method:'POST', body: fd });
  const json = await res.json();
  $('#pubResult').textContent = JSON.stringify(json, null, 2);
  setTimeout(load, 400);
};

load();
</script>
HTML

# move into your project directory
cd ~
# check git is installed
git --version
# configure your identity (do this once)
git config --global user.name "Your Name"
git config --global user.email "your@email.com"
cd~

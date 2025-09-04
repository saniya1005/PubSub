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

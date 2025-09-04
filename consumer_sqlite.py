from google.cloud import pubsub_v1
from google.api_core.exceptions import NotFound
import json, sqlite3

PROJECT_ID = "capstone-project-470501"
TOPIC_ID = "KSU_Team_4"
SUBSCRIPTION_ID = "ksu_team_4_sub"
DB_PATH = "messages.db"

SCHEMA = """
CREATE TABLE IF NOT EXISTS messages (
  ItemID TEXT,
  Location TEXT,
  Quantity INTEGER,
  TransactionDateTime TEXT,
  TransactionNumber TEXT PRIMARY KEY,
  pubsub_message_id TEXT
);
"""

INSERT_SQL = """
INSERT OR IGNORE INTO messages
(ItemID, Location, Quantity, TransactionDateTime, TransactionNumber, pubsub_message_id)
VALUES (?, ?, ?, ?, ?, ?);
"""

def save(payload: dict, pubsub_message_id: str) -> int:
    conn = sqlite3.connect(DB_PATH)
    try:
        conn.execute(SCHEMA)
        cur = conn.execute(
            INSERT_SQL,
            (
                payload.get("ItemID"),
                payload.get("Location"),
                int(payload.get("Quantity")) if payload.get("Quantity") is not None else None,
                payload.get("TransactionDateTime"),
                payload.get("TransactionNumber"),
                pubsub_message_id,
            ),
        )
        conn.commit()
        return cur.rowcount  # 1 if inserted, 0 if duplicate ignored
    finally:
        conn.close()

def callback(message: pubsub_v1.subscriber.message.Message):
    data = message.data.decode("utf-8")
    try:
        payload = json.loads(data)
    except Exception:
        payload = {"raw": data}
    inserted = save(payload, message.message_id)
    print(("stored" if inserted == 1 else "duplicate_ignored"),
          "| txn:", payload.get("TransactionNumber"),
          "| pubsub_id:", message.message_id)
    message.ack()

if __name__ == "__main__":
    subscriber = pubsub_v1.SubscriberClient()
    publisher  = pubsub_v1.PublisherClient()

    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)
    sub_path   = subscriber.subscription_path(PROJECT_ID, SUBSCRIPTION_ID)

    try:
        publisher.get_topic(request={"topic": topic_path})
    except NotFound:
        raise SystemExit(f"Topic not found: {topic_path}")

    try:
        subscriber.get_subscription(request={"subscription": sub_path})
    except NotFound:
        subscriber.create_subscription(name=sub_path, topic=topic_path)

    print(f"Listening and saving to {DB_PATH} on subscription: {SUBSCRIPTION_ID}")
    future = subscriber.subscribe(sub_path, callback=callback)
    try:
        future.result()
    except KeyboardInterrupt:
        future.cancel()

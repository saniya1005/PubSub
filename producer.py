from google.cloud import pubsub_v1
import json, uuid
from datetime import datetime

PROJECT_ID = ""
TOPIC_ID   = ""

def main():
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(PROJECT_ID, TOPIC_ID)

    msg = {
        "ItemID": "123",
        "Location": "Atlanta",
        "Quantity": 10,
        "TransactionDateTime": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "TransactionNumber": f"TXN-{uuid.uuid4().hex[:12]}"
    }

    data = json.dumps(msg).encode("utf-8")
    message_id = publisher.publish(topic_path, data, source="producer.py", schema="v1").result()
    print("Published message ID:", message_id)

if __name__ == "__main__":
    main()

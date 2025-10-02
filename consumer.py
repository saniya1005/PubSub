from google.cloud import pubsub_v1
from google.api_core.exceptions import NotFound
import json

PROJECT_ID = ""
TOPIC_ID = ""
SUBSCRIPTION_ID = ""

def callback(message: pubsub_v1.subscriber.message.Message):
    try:
        payload = json.loads(message.data.decode("utf-8"))
    except Exception:
        payload = {"raw": message.data.decode("utf-8")}
    print("pubsub_message_id:", message.message_id, "| data:", payload)
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

    print(f"Listening on subscription: {SUBSCRIPTION_ID}")
    future = subscriber.subscribe(sub_path, callback=callback)
    try:
        future.result()
    except KeyboardInterrupt:
        future.cancel()
future = subscriber.subscribe(sub_path, callback=callback)

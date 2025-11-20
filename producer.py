from confluent_kafka import Producer
import uuid
import json

producer_config = {'bootstrap.servers':'localhost:9092'}

producer = Producer(producer_config)

order = {
    "order_id":str(uuid.uuid4()),
    "user":"test",
    "item":"saliami",
    "quantity":2
}

event_order = json.dumps(order).encode("utf-8")

def delivery_report(err, msg):
    if err:
        print(f"❌ Delivery failed: {err}")
    else:
        print(f"✅ Delivery succeeded: {msg.value().decode("uft-8")}") 


producer.produce(topic="orders", value=event_order, callback=delivery_report)

producer.flush()
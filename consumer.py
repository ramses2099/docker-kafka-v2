from confluent_kafka import Consumer
import json

consumer_config = {
    "bootstrap.servers":"localhost:9092",
    "group.id":"order-tracker",
    "auto.offset.reset":"earliest"
}

consumer = Consumer(consumer_config)

consumer.subscribe(["orders"])

print("🟢 Consumer is running and subscribed to orders")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"❌ Delivery failed: {msg.error()}")
            continue

        value = msg.value().decode('utf-8')
        order = json.loads(value)
        print(f"📦 Recived order: {order['quantity'], order['item']} from user: {order['user']}")
     
except KeyboardInterrupt as e:
    print("\n 🔴 Stopping consumer")
finally:
    consumer.close()
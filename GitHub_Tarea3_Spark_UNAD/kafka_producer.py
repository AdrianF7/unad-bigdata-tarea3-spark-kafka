#!/usr/bin/env python3
"""Produce datos ambientales SIMULADOS hacia el topic Kafka sensor_data."""
import time
import json
import random
from kafka import KafkaProducer


def generate_sensor_data():
    return {
        "sensor_id": random.randint(1, 10),
        "temperature": round(random.uniform(20, 30), 2),
        "humidity": round(random.uniform(30, 70), 2),
        "timestamp": int(time.time()),
    }


producer = KafkaProducer(
    bootstrap_servers=["localhost:9092"],
    value_serializer=lambda value: json.dumps(value).encode("utf-8"),
)

try:
    while True:
        sensor_data = generate_sensor_data()
        producer.send("sensor_data", value=sensor_data)
        print(f"Sent: {sensor_data}", flush=True)
        time.sleep(1)
except KeyboardInterrupt:
    print("Productor detenido por el usuario.")
finally:
    producer.flush()
    producer.close()

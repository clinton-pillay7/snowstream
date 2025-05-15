import json
import time
from confluent_kafka import Consumer, KafkaError
import snowflake.connector
from dotenv import load_dotenv
import os

load_dotenv()

# Kafka configuration
KAFKA_CONFIG = {
    'bootstrap.servers': 'localhost:19092',  # Redpanda broker address
    'group.id': 'snowflake-writer',
    'auto.offset.reset': 'earliest',
}

TOPIC_NAME = 'kafka_tut'  # Redpanda topic to listen to

# Snowflake configuration
SNOWFLAKE_CONFIG = {
    'user': os.getenv("SNOWFLAKEUN"),
    'password': os.getenv("SNOWFLAKEPASS"),
    'account': os.getenv("SNOWFLAKEACC"),
    'warehouse': 'COMPUTE_WH',
    'database': 'kafka_tutorial',
    'schema': 'PUBLIC',
}

TABLE_NAME = 'kafka'  # Target table

# Batch configuration
BATCH_SIZE = 1000
FLUSH_INTERVAL = 5  # seconds


def write_batch_to_snowflake(data_batch):
    if not data_batch:
        return

    try:
        conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
        cursor = conn.cursor()

        keys = data_batch[0].keys()
        columns = ', '.join(keys)
        placeholders = ', '.join(['%s'] * len(keys))
        values = [tuple(d[k] for k in keys) for d in data_batch]

        query = f"INSERT INTO {TABLE_NAME} ({columns}) VALUES ({placeholders})"
        cursor.executemany(query, values)

        print(f"✅ Inserted batch of {len(values)} records into Snowflake.")

    except Exception as e:
        print("❌ Snowflake batch insert failed:", e)
    finally:
        cursor.close()
        conn.close()


def consume_and_insert():
    consumer = Consumer(KAFKA_CONFIG)
    consumer.subscribe([TOPIC_NAME])
    print(f"🎧 Listening to Kafka topic: {TOPIC_NAME}")

    buffer = []
    last_flush = time.time()

    try:
        while True:
            msg = consumer.poll(1.0)
            now = time.time()

            if msg is None:
                pass
            elif msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF:
                    print("❌ Kafka error:", msg.error())
            else:
                try:
                    data = json.loads(msg.value().decode('utf-8'))

                    # Ensure each record has a unique ID
                    if 'id' not in data:
                        data['id'] = str(uuid.uuid4())

                    buffer.append(data)
                except json.JSONDecodeError:
                    print("❌ Failed to decode message:", msg.value())

            if len(buffer) >= BATCH_SIZE or (buffer and now - last_flush >= FLUSH_INTERVAL):
                write_batch_to_snowflake(buffer)
                buffer.clear()
                last_flush = now

    except KeyboardInterrupt:
        print("\n🛑 Gracefully stopping...")
    finally:
        if buffer:
            write_batch_to_snowflake(buffer)
        consumer.close()


if __name__ == '__main__':
    consume_and_insert()

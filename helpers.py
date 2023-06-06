import json
import random
import time
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic

# Initialize Kafka producer
producer = Producer({"bootstrap.servers": "localhost:19092"})

# Initialize Kafka admin client
admin_client = AdminClient({"bootstrap.servers": "localhost:19092"})

# Sensor IDs
sensor_ids = ['1234', '2345', '3456', '4567', '5678']

faulty_sensor = '3456'

# Temperature range
min_temp = 21.0
max_temp = 28.0

# Anomaly temperature range
min_anomaly_temp = 65.0
max_anomaly_temp = 85.0

# Anomaly probability (3% of the time, a sensor reading will be an anomaly)
anomaly_prob = 0.03

# Start time (current time)
start_time = time.time()

# Create the 'sensors' topic if it does not exist
topic = 'sensors'
topics = admin_client.list_topics().topics
if topic not in topics:
    new_topic = NewTopic(topic, num_partitions=1, replication_factor=1)
    admin_client.create_topics([new_topic])

while True:
    for sensor_id in sensor_ids:
        # Decide whether this reading is an anomaly
        is_anomaly = random.random() < anomaly_prob

        if is_anomaly and sensor_id == faulty_sensor:
            # Anomaly: choose a random temperature from the anomaly range
            temperature = random.uniform(min_anomaly_temp, max_anomaly_temp)
        else:
            # Normal reading: choose a random temperature from the normal range
            temperature = random.uniform(min_temp, max_temp)

        # Current timestamp
        ts = time.strftime('%Y-%m-%d %H:%M:%S', time.localtime(start_time))

        # Create the message
        message = {
            'sensor_id': sensor_id,
            'temp': temperature,
            'ts': ts
        }

        # Serialize the message to JSON
        message = json.dumps(message)

        # Send the message to the 'sensors' topic with sensor_id as the key
        producer.produce(topic, key=sensor_id, value=message)

    # Increase start_time by 1 second
    start_time += 1

    # Wait for 1 second
    time.sleep(1)

    # Flush the producer to make sure all messages are sent
    producer.flush()
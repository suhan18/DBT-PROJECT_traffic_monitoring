from kafka import KafkaProducer
import threading
import time
import random
from datetime import datetime
import csv
import json

# This is the Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# This is the topic name we are using to send the data to the Spark engine for processing
topic = "vehicle-data"


# Using 12 locations and 5 vehicle types (3 locations for each region North, South, East, and West)
locations = [
    "Indiranagar", "Whitefield", "Yelahanka", "Jayanagar",
    "Marathahalli", "Jakkur", "Hebbal", "JP Nagar",
    "Kengeri", "Rajajinagar", "Nagarbhavi", "Banashankari"
]
vehicle_types = ["bike", "car", "truck", "auto", "bus"]


data_list = []
data_count = 0 # Global variable to keep track of data count
lock = threading.Lock()

def generate_data():
    global data_count
    start_time = time.time()
    while time.time() - start_time < 20:  # run for 20 seconds
        location = random.choice(locations)
        vehicle = random.choice(vehicle_types)
        count = {
            "bike": random.randint(1, 11),
            "car": random.randint(1, 6),
            "truck": random.randint(1, 4),
            "auto": random.randint(1, 8),
            "bus": random.randint(1, 4)
        }[vehicle]

        data = {
            # "timestamp": datetime.now().strftime("%H:%M:%S"), # We are not using this
            "timestamp": datetime.now().isoformat(), # Using this as Spark needs iso format
            "location": location,
            "vehicle": vehicle,
            "count": count
        }

        # Kafka Producer sending it to Spark engine
        producer.send(topic, value=data)
        print(f"Sent to Spark engine: {data}")

        with lock:
            data_count += 1
            data_list.append(data)
        time.sleep(random.uniform(0.2, 0.5)) # We can reduce this to spawn even more data points at a much faster rate

# Using 4 threads to generate data concurrently
threads = []
for _ in range(4):
    t = threading.Thread(target=generate_data)
    t.start()
    threads.append(t)

for t in threads:
    t.join()


print(f"\nTotal data points generated so far: {data_count}\n")

# Write data to CSV
with open('producer_data_input.csv', 'w', newline='') as file:
    writer = csv.DictWriter(file, fieldnames=["timestamp", "location", "vehicle", "count"])
    writer.writeheader()
    writer.writerows(data_list)

print("\nData generation complete. Stored in 'producer_data_input.csv'")

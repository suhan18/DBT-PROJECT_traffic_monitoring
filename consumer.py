from kafka import KafkaConsumer
import json
import csv
import os
import mysql.connector
from mysql.connector import Error

# This is to store the output in a CSV file
csv_filename = "traffic_analysis_output.csv"
write_header = not os.path.exists(csv_filename)
csv_file = open(csv_filename, mode='w', newline='')
csv_writer = csv.writer(csv_file)

# Function to create MySQL database connection and use this to store the data
def create_db_connection():
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user="root",
            password="HashedStrong@6904",  # MySQL password
            database="dbt_project"
        )
        print("MySQL Database connection successful")
        return connection
    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
        return None

# Create database table if it doesn't exist
def create_table(connection, columns):
    try:
        cursor = connection.cursor()
        # Create a string of column definitions
        column_defs = ["Batch VARCHAR(255)"]
        for col in columns:
            if col != "batch_id":
                column_defs.append(f"`{col}` TEXT")
        
        create_table_query = f"""
        CREATE TABLE IF NOT EXISTS traffic_analysis (
            id INT AUTO_INCREMENT PRIMARY KEY,
            {', '.join(column_defs)}
        )
        """
        cursor.execute(create_table_query)
        connection.commit()
        print("Table created or already exists")
    except Error as e:
        print(f"Error creating table: {e}")

# Function to insert data into MySQL
def insert_data(connection, data, batch_id, columns):
    try:
        cursor = connection.cursor()
        db_columns = ["Batch"] + [f"`{col}`" for col in columns if col != "batch_id"]
        placeholders = ["%s"] * len(db_columns)
        
        # Build values list in the same order as columns
        values = [batch_id] + [data.get(col, "") for col in columns if col != "batch_id"]
        
        insert_query = f"""
        INSERT INTO traffic_analysis ({', '.join(db_columns)})
        VALUES ({', '.join(placeholders)})
        """
        cursor.execute(insert_query, values)
        connection.commit()
        print(f"Row inserted into MySQL database for batch #{batch_id}")
    except Error as e:
        print(f"Error inserting data: {e}")

# This is the topic to get the data from Spark
get_data_from_spark = 'traffic-analysis-output'


# This is to consume messages from the Kafka topic
# auto_offest_reset = 'earliest' : So that it reads from the beginning of the topic
# enable_auto_commit = False : Disable auto commit so that it gets all data and behaves as if all data is new which is true in our case
# group_id = None : No group id because we don't want any state being stored and we want to read all data fresh

consumer = KafkaConsumer(
    get_data_from_spark,
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest', 
    enable_auto_commit=False,
    group_id=None,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Waiting for traffic analysis messages...\n")

# Database connection
db_connection = create_db_connection()
table_created = False

for message in consumer:
    data = message.value
    batch_id = data.get("batch_id", "Unknown")

    print(f"Batch #{batch_id}")
    print("Traffic Data Received:")
    for key, value in data.items():
        print(f"  {key}: {value}")

    # Define columns - all keys in the message
    columns = list(data.keys())

    # Write CSV header if needed
    if write_header:
        csv_writer.writerow(['Batch'] + [key for key in columns if key != "batch_id"])
        write_header = False
    
    # Write CSV data
    csv_writer.writerow([batch_id] + [data.get(key, "") for key in columns if key != "batch_id"])
    csv_file.flush()
    
    # Create table if not already created
    if db_connection and not table_created:
        create_table(db_connection, columns)
        table_created = True
    
    # Insert data into MySQL
    if db_connection:
        insert_data(db_connection, data, batch_id, columns)

    print("-" * 50)

# Cleanup
csv_file.close()
if db_connection and db_connection.is_connected():
    db_connection.close()
    print("MySQL connection closed")
from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import json
import spark_sql as ss

json_path = "/home/naya/anomaly/files_json/scd_raw.json"
renamed_json_path = "/home/naya/anomaly/files_json/scd_raw_read.json"

host = 'cnt7-naya-cdh63'
port = '9092'
bootstrap_servers = f'{host}:{port}' 
topic = 'get_sealing_raw_data'
group_id = 'prepare_predict_HDFS'
enable_auto_commit = True
auto_commit_interval_ms = 5000
auto_offset_reset = 'earliest'
value_deserializer = lambda x: x.decode('utf-8')

# Create the Kafka consumer
consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers, group_id=group_id,
                         enable_auto_commit=enable_auto_commit,
                         auto_commit_interval_ms=auto_commit_interval_ms,
                         auto_offset_reset=auto_offset_reset,
                         value_deserializer=value_deserializer)

# Accumulate messages into a list
messages = []

# Read and process messages from the Kafka topic
for message in consumer:
    value = message.value
    # Extract the file size from the message
    if value.startswith("file_size:"):
        file_size = int(value.split(":")[1])
        print(f"File size: {file_size}")
    else:
        messages.append(value)
        print(f"Received message: {value}")

    # Process the accumulated messages when the desired file size is reached
    if len(messages) == file_size:
        full_json = ''.join(messages)

        # Split the full JSON string into individual records
        record_strings = full_json.split('\n')

        # Parse and process each individual JSON record
        for record_string in record_strings:
            try:
                record = json.loads(record_string)
                # Process the record as needed
                print(record)
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON record: {record_string}")

        # Clear the accumulated messages
        messages = []
# refine it
df = ss.spark_refine()


# 
# Close the Kafka consumer
consumer.close()

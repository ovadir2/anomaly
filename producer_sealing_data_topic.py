import json
import pandas as pd
from kafka import KafkaProducer
from datetime import datetime
from sql import fetch_sealing_data
import os

json_path = "/home/naya/anomaly/files_json/scd_raw.json"
renamed_json_path = "/home/naya/anomaly/files_json/scd_raw_read.json"

host = 'cnt7-naya-cdh63'
port = '9092'
bootstrap_servers = f'{host}:{port}' 
topic = 'kafka-tst-01'

def trigger_fetch_and_produce():
    current_time = datetime.now()
    year = current_time.year
    quarter = None
    month = None
    yearweek = current_time.isocalendar()[1]
    weekday = current_time.weekday()
    configs_id = 917
    
    if os.path.isfile(json_path):
        scd_raw = pd.read_json(json_path)
        print(f"Reading from existing file, {len(scd_raw)}")
        os.rename(json_path, renamed_json_path)
    else:
        print(year, quarter, month, yearweek, weekday, configs_id)
        scd_raw = fetch_sealing_data(year, quarter, month, yearweek, weekday, configs_id)
        print(f"Fetching from new file, {len(scd_raw)}")
    
    # Convert DataFrame to JSON string
    scd_raw_json = scd_raw.to_json(orient='records')

    # Initialize Kafka producer
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

    # Produce the sealing data to Kafka
    producer.send(topic, value=scd_raw_json.encode('utf-8'))

    # Flush and close the Kafka producer
    producer.flush()
    producer.close()

# Trigger the fetch and produce function
trigger_fetch_and_produce()

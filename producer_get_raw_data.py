import json
import pandas as pd
from kafka import KafkaProducer
from datetime import datetime
from sql import fetch_sealing_data
import os

json_path = "/home/naya/anomaly/files_json/scd_raw.json"
json_path = "/home/naya/anomaly/files_json/scd_raw.json"
renamed_json_path = "/home/naya/anomaly/files_json/scd_raw_read.json"

host = 'cnt7-naya-cdh63'
port = '9092'
bootstrap_servers = f'{host}:{port}' 
topic = 'get_sealing_raw_data'

def trigger_fetch_and_produce():
     
    file_size=0
    if os.path.isfile(json_path):
         scd_raw = pd.read_json(json_path)
         file_size = os.path.getsize(json_path)
         print(f"Reading from existing file, {len(scd_raw)}")
         file_size = os.path.getsize(json_path)
         os.rename(json_path, renamed_json_path)
    else:
        current_time = datetime.now()
        year = current_time.year
        quarter = None
        month = None
        yearweek = current_time.isocalendar()[1] -1 #need 1 week before for prediction
        weekday = 4
        configs_id = 917

        print(year, quarter, month, yearweek, weekday, configs_id)
        scd_raw = fetch_sealing_data(year, quarter, month, yearweek, weekday, configs_id)
        print(f"Fetching from new file")
            
    file_size = len(scd_raw)
    print(f"File size: {file_size}")
    
    # # Convert DataFrame to json string
    # scd_raw_json = scd_raw.to_json(orient='records')

    # Initialize Kafka producer
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)
    if file_size != 0:
        producer.send(topic, value=f'file_size:{file_size}'.encode('utf-8'))
    # Iterate over each record and produce it to Kafka
    for _, record in scd_raw.iterrows():
        # Convert record to json string
        record_json = record.to_json()
        # Produce the record to Kafka
        producer.send(topic, value=record_json.encode('utf-8'))
        # Flush and close the Kafka producer
        producer.flush()
    producer.close()

# Trigger the fetch and produce function
trigger_fetch_and_produce()

#import json
#import pandas as pd
from kafka import KafkaProducer
from datetime import datetime
from sql import fetch_sealing_data
import argparse

json_path = "/home/naya/anomaly/files_json/scd_raw.json"
json_path = "/home/naya/anomaly/files_json/scd_raw.json"
renamed_json_path = "/home/naya/anomaly/files_json/scd_raw_read.json"


def trigger_fetch_and_produce(history_value= 'No'):
    print("history:", history_value)
    host = 'cnt7-naya-cdh63'
    port = '9092'
    topic = 'get_sealing_raw_data'
    bootstrap_servers = f'{host}:{port}' 
    
    if  history_value.lower() == 'no':
        current_time = datetime.now()
        year = current_time.year
        quarter = None
        month = None
        yearweek = current_time.isocalendar()[1] #need 1 week before for prediction
        weekday = None
        configs_id = 917
    else:
        current_time = datetime.now()
        year = 2022
        quarter = None
        month = None
        yearweek = None
        weekday = None
        configs_id = 917
        
    file_size=0
   
    print(year, quarter, month, yearweek, weekday, configs_id)
    scd_raw = fetch_sealing_data(year, quarter, month, yearweek, weekday, configs_id)
    print(f"Fetching from new file")
            
    file_size = len(scd_raw) 
    print(f"File size: {file_size}")
        
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


if __name__ == "__main__":
    # Create an argument parser
    parser = argparse.ArgumentParser()
    # Add the --history option
    parser.add_argument("--history", type=str, default='No', help="Specify the history value")
    # Parse the command line arguments
    args = parser.parse_args()

    # Trigger the fetch and produce function with the parsed history value
    trigger_fetch_and_produce(history_value=args.history)
import json
import pandas as pd
from kafka import KafkaProducer
from datetime import datetime
from sql import fetch_sealing_data

host = 'cnt7-naya-cdh63'
port = '9092'
bootstrap_servers = f'{host}:{port}' 
topic = 'sealing_data_topic'

def trigger_fetch_and_produce():
    # Update time parameters with actual time
    current_time = datetime.now()
    year = current_time.year
    quarter = None
    month = None
    yearweek = current_time.isocalendar()[1]
    weekday = current_time.weekday()
    configs_id = 917

    # Fetch sealing data
    scd_raw = fetch_sealing_data(year, quarter, month, yearweek, weekday, configs_id)

    # Convert DataFrame to JSON string
    scd_raw_json = scd_raw.to_json(orient='records')

    # Initialize Kafka producer
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

    # Produce the sealing data to Kafka
    producer.send(topic, value=scd_raw_json.encode('utf-8'))

    # Close the Kafka producer
    producer.close()

# Trigger the fetch and produce function
trigger_fetch_and_produce()

from kafka import KafkaConsumer

host = 'cnt7-naya-cdh63'
port = '9092'
bootstrap_servers = f'{host}:{port}' 
topic = 'get_sealing_raw_data'
group_id = 'prepare_anomaly'
enable_auto_commit = True
auto_commit_interval_ms = 5000
auto_offset_reset = 'earliest'
value_deserializer = lambda x: x  # Return bytes without decoding

# Create the Kafka consumer
consumer = KafkaConsumer(
    topic,
    bootstrap_servers=bootstrap_servers,
    group_id=group_id,
    enable_auto_commit=enable_auto_commit,
    auto_commit_interval_ms=auto_commit_interval_ms,
    auto_offset_reset=auto_offset_reset,
    value_deserializer=value_deserializer
)


# Continuously poll for records until there are no more
while True:
    records = consumer.poll(timeout_ms=1000)  # Poll for records with a timeout of 1 second
    
    if records:
        for tp, records_batch in records.items():
            for record in records_batch:
                # Process the record
                print(record.value)
    else:
        # No more records to fetch, break the loop
        break

# Close the Kafka consumer
consumer.close()

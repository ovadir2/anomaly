from kafka import KafkaConsumer

host = 'cnt7-naya-cdh63'
port = '9092'
bootstrap_servers = f'{host}:{port}' 
topic = 'get_sealing_raw_data'
group_id = 'consumer_group2'
enable_auto_commit=True,
auto_commit_interval_ms=5000,
auto_offset_reset='earliest',
value_deserializer=lambda x: x.decode('utf-8')

# Create the Kafka consumer
consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers, group_id=group_id)

# Read and print messages from the Kafka topic
i=0
for message in consumer:
    i=i+1
    print(f"Received message: {message.value}, {i}")

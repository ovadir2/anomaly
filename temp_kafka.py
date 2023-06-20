from kafka import KafkaConsumer

host = 'cnt7-naya-cdh63'
port = '9092'
bootstrap_servers = f'{host}:{port}' 
topic = 'kafka-tst-01'
group_id = 'consumer_group2'

# Create the Kafka consumer
consumer = KafkaConsumer(topic, bootstrap_servers=bootstrap_servers, group_id=group_id)

# Read and print messages from the Kafka topic
for message in consumer:
    print(f"Received message: {message.value}")

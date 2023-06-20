from kafka import KafkaConsumer, KafkaProducer
import json
import pandas as pd

# Kafka consumer configuration
kafka_bootstrap_servers = 'cnt7-naya-cdh63:9092'
kafka_topic = 'sealing_data_topic'
consumer_group_id = 'sealing_data_consumer_group'

# Kafka producer configuration
bootstrap_servers = 'cnt7-naya-cdh63:9092'
topic = 'sealing_data_topic'

def spark_sql_operation(data):
    # Perform Spark SQL operations on the consumed data
    # ...

    # Return the processed data
    return processed_data

def consume_and_process_data():
    # Create a Kafka consumer instance
    consumer = KafkaConsumer(
        kafka_topic,
        bootstrap_servers=kafka_bootstrap_servers,
        group_id=consumer_group_id
    )

    # Initialize Kafka producer
    producer = KafkaProducer(bootstrap_servers=bootstrap_servers)

    try:
        # Consume data from Kafka topic
        for message in consumer:
            # Deserialize the consumed message
            consumed_data = json.loads(message.value.decode('utf-8'))

            # Perform Spark SQL operations on the consumed data
            processed_data = spark_sql_operation(consumed_data)

            # Convert processed data to JSON string
            processed_data_json = processed_data.to_json(orient='records')

            # Produce the processed data to Kafka
            producer.send(topic, value=processed_data_json.encode('utf-8'))

    except Exception as e:
        print(f"An error occurred: {str(e)}")

    finally:
        # Close the Kafka consumer and producer
        consumer.close()
        producer.close()

# Trigger the data consumption and processing
consume_and_process_data()

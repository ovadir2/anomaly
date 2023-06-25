from kafka import KafkaConsumer
import json
from pyspark.sql import SparkSession
import os

host = 'cnt7-naya-cdh63'
port = '9092'
bootstrap_servers = f'{host}:{port}' 
topic = 'get_sealing_raw_data'
group_id = 'prepare_predict_HDFS' #'consumer_group2'
enable_auto_commit=True,
auto_commit_interval_ms=5000,
auto_offset_reset='earliest',
value_deserializer=lambda x: x.decode('utf-8')

local_path_refine_output = "/home/naya/anomaly/files_json/scd_refine.json"



def process_json_record(record):
    try:
        # Convert column names to lowercase
        record = record.toDF(*(c.lower() for c in record.columns))

        # Trim whitespace from string columns
        record = record.select(*(F.col(c).cast("string").trim().alias(c) if c in record.columns else F.col(c) for c in record.columns))

        # Delete rows where 'pass_failed' column is not 'Pass'
        record = record.filter(record['pass_failed'] == 'Pass')
        
        # Trim whitespace from the 'pass_failed' column
        record = record.withColumn('pass_failed', F.trim(F.col('pass_failed')))

        # Drop rows with null values in 'domecasegap' and 'stitcharea' columns
        record = record.dropna(subset=['domecasegap', 'stitcharea'])

        # Convert 'test_time_min' column to duration in seconds
        record = record.withColumn('test_time_sec', F.col('test_time_min').cast('float') * 60)

        # Convert 'date' column to datetime if it's not already
        record = record.withColumn('date', F.col('date').cast(TimestampType()))

        # Select the desired columns to keep
        columns_to_keep = ['week', 'batchid', 'tp_cell_name', 'blister_id', 'domecasegap', 'domecasegap_limit',
                           'domecasegap_spc', 'stitcharea', 'stitcharea_limit', 'stitcharea_spc', 'minstitchwidth',
                           'bodytypeid', 'dometypeid', 'leaktest', 'laserpower', 'lotnumber', 'test_time_sec',
                           'date', 'error_code_number', 'pass_failed']

        record = record.select(*columns_to_keep)
        record.printSchema()

        return record
    except Exception as e:
        print(f"An error occurred while processing JSON record: {str(e)}")
        return None

def process_record(record):
    try:
        # Create a SparkSession
        spark = SparkSession.builder.getOrCreate()

        if os.path.exists(local_path_refine_output):
            # Read the existing file as a DataFrame
            read_record = spark.read.json(local_path_refine_output)
            
            # Append the refined DataFrame to the existing file
            combined_record = record.union(read_record)
            if not combined_record.isEmpty():
                combined_record.write.json(local_path_refine_output, mode="overwrite")
        else:
            # Save the refined DataFrame as a new JSON file
            if not record.isEmpty():
                record.write.json(local_path_refine_output)  # Append the DataFrame to the destination file
    except Exception as e:
        # Handle any errors that occur during record processing
        print(f"Error processing record: {record}. Error: {str(e)}")

def consume_from_kafka(topic, bootstrap_servers, group_id, value_deserializer, auto_offset_reset='earliest'):
    try:
        # Create the Kafka consumer
        consumer = KafkaConsumer(
            topic,
            bootstrap_servers=bootstrap_servers,
            group_id=group_id,
            enable_auto_commit=True,
            auto_commit_interval_ms=5000,
            auto_offset_reset=auto_offset_reset,
            value_deserializer=value_deserializer
        )

        # Accumulate messages into a list
        messages = []
        file_size = 0

        # Continuously consume messages from Kafka
        for message in consumer:
            # Decode the message value
            message_value = message.value

            # Check if the message contains file size information
            if message_value.startswith('file_size:'):
                file_size = int(message_value.split(':')[1])
                print(f"Received file size: {file_size}")
            else:
                # Process the regular record message
                record_json = message_value
                print(f"Received record: {record_json}")
                  # Process the record
                refined_record = process_json_record(record_json)
                print(f"writing refine record:")
                if refined_record:
                    process_record(refined_record)


        # Close the Kafka consumer
        consumer.close()
  
  
    except Exception as e:
        print(f"An error occurred while consuming from Kafka: {str(e)}")

# Set up Kafka consumer and process messages
consume_from_kafka(topic, bootstrap_servers, group_id, value_deserializer)

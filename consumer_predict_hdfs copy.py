from kafka import KafkaConsumer
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import TimestampType
import json
import os

json_path = "/home/naya/anomaly/files_json/scd_raw.json"
renamed_json_path = "/home/naya/anomaly/files_json/scd_raw_read.json"
local_path_refine_output = "/home/naya/anomaly/files_json/scd_refine.json"

host = 'cnt7-naya-cdh63'
port = '9092'
bootstrap_servers = f'{host}:{port}'
topic = 'get_sealing_raw_data'
group_id = 'prepare_predict_HDFS'
enable_auto_commit = True
auto_commit_interval_ms = 5000
auto_offset_reset = 'earliest'
value_deserializer = lambda x: x.decode('utf-8')


def process_json_record(record):
    try:
        print('#######################################################################################################################################')
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


def process_kafka_messages(messages):
    try:
        # Concatenate the accumulated messages into a single JSON string
        full_json = ''.join(messages)

        # Split the full JSON string into individual records
        record_strings = full_json.split('\n')

        # Create a SparkSession
        spark = SparkSession.builder.appName("KafkaMessageProcessor").getOrCreate()

        # Process each individual JSON record
        for record_string in record_strings:
            try:
                record = json.loads(record_string)
                df = spark.createDataFrame([record])  # Convert JSON record to DataFrame

                # Process the record
                processed_df = process_json_record(df)
                if processed_df is not None:
                    print(f"processed_df: OK")
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON record: {record_string}")
            except Exception as e:
                print(f"An error occurred while processing JSON record: {str(e)}")
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
                #record.show(5)

        # Stop the SparkSession
        spark.stop()
    except Exception as e:
        print(f"An error occurred while processing Kafka messages: {str(e)}")


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

        # Read and process messages from the Kafka topic
        for message in consumer:
            value = message.value

            # Extract the file size from the message
            if value.startswith("file_size:"):
                file_size = int(value.split(":")[1])
                print(f"File size: {file_size}")
            else:
                messages.append(value)
                  # print(f"Received message: {value}")
            # Process the accumulated messages when the desired file size is reached
            #if file_size and len(messages) == file_size:
                process_kafka_messages(messages)
                # Clear the accumulated messages
                messages = []

        # Close the Kafka consumer
        consumer.close()
    except Exception as e:
        print(f"An error occurred while consuming from Kafka: {str(e)}")


# Set up Kafka consumer and process messages
consume_from_kafka(topic, bootstrap_servers, group_id, value_deserializer)

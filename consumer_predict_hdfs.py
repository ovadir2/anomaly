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
        processed_df = record.toDF(*(c.lower() for c in record.columns))

        # Trim whitespace from string columns
        processed_df = processed_df.select(*(F.col(c).cast("string").trim().alias(c) if c in processed_df.columns else F.col(c) for c in processed_df.columns))

        # Delete rows where 'pass_failed' column is not 'Pass'
        processed_df = processed_df.filter(processed_df['pass_failed'] == 'Pass')
        
        # Trim whitespace from the 'pass_failed' column
        processed_df = processed_df.withColumn('pass_failed', F.trim(F.col('pass_failed')))

        # Drop rows with null values in 'domecasegap' and 'stitcharea' columns
        processed_df = processed_df.dropna(subset=['domecasegap', 'stitcharea'])

        # Convert 'test_time_min' column to duration in seconds
        processed_df = processed_df.withColumn('test_time_sec', F.col('test_time_min').cast('float') * 60)

        # Convert 'date' column to datetime if it's not already
        processed_df = processed_df.withColumn('date', F.col('date').cast(TimestampType()))

        # Select the desired columns to keep
        columns_to_keep = ['week', 'batchid', 'tp_cell_name', 'blister_id', 'domecasegap', 'domecasegap_limit',
                           'domecasegap_spc', 'stitcharea', 'stitcharea_limit', 'stitcharea_spc', 'minstitchwidth',
                           'bodytypeid', 'dometypeid', 'leaktest', 'laserpower', 'lotnumber', 'test_time_sec',
                           'date', 'error_code_number', 'pass_failed']

        processed_df = processed_df.select(*columns_to_keep)
        return processed_df
    except Exception as e:
        print(f"An error occurred while processing json record: {str(e)}")
        return None


import traceback

def process_kafka_messages(messages):
    try:
        # Concatenate the accumulated messages into a single json string
        full_json = ''.join(messages)

        # Split the full json string into individual records
        record_strings = full_json.split('\n')

        # Create a SparkSession
        spark = SparkSession.builder.appName("KafkaMessageProcessor").getOrCreate()

        # Process each individual json record
        for record_string in record_strings:
            try:
                record = json.loads(record_string)
                df = spark.createDataFrame([record])  # Convert json record to DataFrame
                print('#######################################################################################################################################')
                # Process the record
                processed_df = process_json_record(df)
                if processed_df is not None:
                    print(f"processed_df: OK")
                    if os.path.exists(local_path_refine_output):
                        # Read the existing file as a DataFrame
                        read_df = spark.read.json(local_path_refine_output)

                        # Append the refined DataFrame to the existing file
                        combined_df = df.union(read_df)
                        if not combined_df.isEmpty():
                            combined_df.write.json(local_path_refine_output, mode="overwrite")
                    else:
                        # Save the refined DataFrame as a new json file
                        if not df.isEmpty():
                            df.write.json(local_path_refine_output)  # Append the DataFrame to the destination file
                    processed_df.show(5)
            except json.JSONDecodeError as e:
                print(f"Error parsing JSON record: {record_string}")
                traceback.print_exc()  # Print the full traceback information
            except Exception as e:
                print(f"An error occurred while processing JSON record: {str(e)}")
                traceback.print_exc()  # Print the full traceback information

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
                processed_messages = process_kafka_messages(messages)
                if processed_messages is not None:
                    messages.extend(processed_messages)                        
                    if len(messages) != file_size:                                        messages.append(value)
                                    #print(f"Received message: {value}")
            # Process the accumulated messages when the desired file size is reached
                                # Clear the accumulated messages
                       # else:
                            #messages = []

        # Close the Kafka consumer
        consumer.close()
    except Exception as e:
        print(f"An error occurred while consuming from Kafka: {str(e)}")


# Set up Kafka consumer and process messages
consume_from_kafka(topic, bootstrap_servers, group_id, value_deserializer)

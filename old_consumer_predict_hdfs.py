import traceback
from kafka import KafkaConsumer
import os
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext
from pyspark.sql.functions import col, trim, udf
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, DateType

# Kafka configuration
host = 'cnt7-naya-cdh63'
port = '9092'
bootstrap_servers = f'{host}:{port}' 
topic = 'get_sealing_raw_data'
group_id = 'prepare_predict_HDFS'
value_deserializer = lambda x: x.decode('utf-8')
auto_offset_reset = 'earliest'

# Output file path
local_path_refine_output = "/home/naya/anomaly/files_json/scd_refine.json"

def process_json_record(record, spark):
    try:
        # Convert the JSON record to a DataFrame
        df = spark.read.json(spark.sparkContext.parallelize([record]))
        df = df.toDF(*(c.lower() for c in df.columns))

        # Trim whitespace from string columns
        trim_udf = udf(lambda x: x.strip() if x is not None and isinstance(x, str) else x, StringType())
        df = df.select(*(trim_udf(col(c)).alias(c) if c in df.columns else col(c) for c in df.columns))

        # Apply Spark SQL transformations on the DataFrame
        df.createOrReplaceTempView("record_table")

        query = """
        SELECT week, batchid, tp_cell_name, blister_id, domecasegap, domecasegap_limit,
               domecasegap_spc, stitcharea, stitcharea_limit, stitcharea_spc, minstitchwidth,
               bodytypeid, dometypeid, leaktest, laserpower, lotnumber, test_time_min * 60 AS test_time_sec,
               date, error_code_number, pass_failed
        FROM record_table
        WHERE pass_failed = 'Pass'
          AND domecasegap IS NOT NULL
          AND stitcharea IS NOT NULL
        """

        refined_record = spark.sql(query)
        return refined_record

    except Exception as e:
        print(f"An error occurred while processing the JSON record: {str(e)}")
        traceback.print_exc()  # Print the traceback for detailed error information
        return None

def get_refined_record_schema():
    """
    Returns the schema for the refined records.
    """
    return StructType([
        StructField("week", IntegerType(), nullable=True),
        StructField("batchid", IntegerType(), nullable=True),
        StructField("tp_cell_name", StringType(), nullable=True),
        StructField("blister_id", StringType(), nullable=True),
        StructField("domecasegap", StringType(), nullable=True),
        StructField("domecasegap_limit", DoubleType(), nullable=True),
        StructField("domecasegap_spc", DoubleType(), nullable=True),
        StructField("stitcharea", DoubleType(), nullable=True),
        StructField("stitcharea_limit", DoubleType(), nullable=True),
        StructField("stitcharea_spc", DoubleType(), nullable=True),
        StructField("minstitchwidth", DoubleType(), nullable=True),
        StructField("bodytypeid", StringType(), nullable=True),
        StructField("dometypeid", StringType(), nullable=True),
        StructField("leaktest", StringType(), nullable=True),
        StructField("laserpower", DoubleType(), nullable=True),
        StructField("lotnumber", StringType(), nullable=True),
        StructField("test_time_sec", DoubleType(), nullable=True),
        StructField("date", DateType(), nullable=True),
        StructField("error_code_number", StringType(), nullable=True),
        StructField("pass_failed", StringType(), nullable=True)
    ])

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

        # Create a SparkSession
        conf = SparkConf().set("spark.debug.maxToStringFields", "100")

        # Create SparkContext
        sc = SparkContext(conf=conf)

        # Create SparkSession using the SparkContext
        spark = SparkSession.builder.config(conf=conf).getOrCreate()

        # Create an empty DataFrame to hold the refined records
        refined_records = spark.createDataFrame([], schema=get_refined_record_schema())

        # Process messages from Kafka
        # Consume and discard existing messages
        consumer.poll()
        file_size = 0
        message_count = 0  # Counter for processed messages
        for message in consumer:
            message_value = message.value
            if message_value.startswith('file_size:'):
                file_size = int(message_value.split(':')[1])
                print(f"Received file size: {file_size}")
            else:
                if file_size > 0:
                    print(message_count, file_size)
                    if message_count == file_size:
                        message_count = 0
                        file_size = 0
                    else:
                        message_count += 1
                        # Decode the message value
                        message_value = message.value
                        
                        # Process the record
                        refined_record = process_json_record(message_value, spark)
                        if not refined_record.rdd.isEmpty():
                            print(f"refined_record #, {message_count}")
                            # Append the refined record to the DataFrame
                            refined_records = refined_records.union(refined_record)
                        
        # Perform the first aggregation
        grouped_scd = refined_records.groupby('week').agg({
            'domecasegap': ['max', 'min', 'mean', 'std'],
            'stitcharea': ['max', 'min', 'mean', 'std']
        })

        # Rename the columns for clarity
        grouped_scd.columns = [
            'maximum_domecasegap', 'minimum_domecasegap',\
            'domecasegap_week_mean', 'domecasegap_week_stddev', \
            'maximum_stitcharea', 'minimum_stitcharea',\
            'stitcharea_week_mean', 'stitcharea_week_stddev'
        ]

        # Perform the second aggregation using scd_refine DataFrame
        scd_weeks_raws = {}
        scd_weeks_raws['stitcharea_week_mean'] = refined_records.groupby('week')['stitcharea'].mean()
        scd_weeks_raws['stitcharea_week_stddev'] = refined_records.groupby('week')['stitcharea'].std()
        scd_weeks_raws['domecasegap_week_mean'] = refined_records.groupby('week')['domecasegap'].mean()
        scd_weeks_raws['domecasegap_week_stddev'] = refined_records.groupby('week')['domecasegap'].std()
        scd_weeks_raws['maximum_domecasegap'] = refined_records.groupby('week')['domecasegap'].max()
        scd_weeks_raws['minimum_domecasegap'] = refined_records.groupby('week')['domecasegap'].min()
        scd_weeks_raws['maximum_stitcharea'] = refined_records.groupby('week')['stitcharea'].max()
        scd_weeks_raws['minimum_stitcharea'] = refined_records.groupby('week')['stitcharea'].min()

        # Print the result
        for key, value in scd_weeks_raws.items():
            value.show()

        # Stop the SparkSession and SparkContext
        spark.stop()
        sc.stop()

    except Exception as e:
        print(f"An error occurred while consuming from Kafka: {str(e)}")
        traceback.print_exc()  # Print the traceback for detailed error information



# Set up Kafka consumer and process messages
os.system('sudo pkill -9 -f SparkSubmit')
consume_from_kafka(topic, bootstrap_servers, group_id, value_deserializer, auto_offset_reset)

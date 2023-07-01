from kafka import KafkaConsumer
from pyspark.sql import SparkSession

host = 'cnt7-naya-cdh63'
port = '9092'
bootstrap_servers = f'{host}:{port}' 
topic = 'get_sealing_raw_data'
group_id = 'prepare_predict_HDFS'  # 'consumer_group2'
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

# Create SparkSession
spark = SparkSession.builder.getOrCreate()

# Collect all messages into a list
messages = []
file_size = 0
message_count = 0

for message in consumer:
    message_value = message.value.decode('utf-8')  # Decode the message value
    if message_value.startswith('file_size:'):
        file_size = int(message_value.split(':')[1])
        print(f"Received file size: {file_size}")
    else:
        messages.append(message_value)
        message_count += 1

    if file_size > 0 and message_count == file_size:
        break

    # Print the running number on the same position
    print(f"{message_count}", end='\r')





# Create SparkSession
spark = SparkSession.builder.getOrCreate()

def SealingCell_data_refinning(df):
    # Rename the duplicate column
    df = df.withColumnRenamed('BatchID', '_BatchID')
    
    # Generate a unique file name using the UNIX timestamp
    df = df.toDF(*(c.lower() for c in df.columns))
    df = df.withColumn("pass_failed", df["pass_failed"].cast("string"))
    df = df.withColumn("test_time_min", df["test_time_min"].cast("string"))
    df = df.withColumn("date", df["date"].cast("string"))
    
    # Delete all records not marked as "Pass"
    df = df.filter(df['pass_failed'] == 'Pass')
    
    # Drop rows with missing values in 'domecasegap' and 'stitcharea' columns
    df = df.dropna(subset=['domecasegap', 'stitcharea'])
    
    # Calculate the test time in seconds
    df = df.withColumn("test_time_sec", df['test_time_min'].substr(1, 2).cast("integer") * 60 * 60 + df['test_time_min'].substr(4, 2).cast("integer") * 60)
    
    # Convert the 'date' column to datetime if it's not already
    df = df.withColumn("date", df['date'].cast("date"))
    
    # Select the desired columns
    columns_to_keep = ['week', 'batchid', 'tp_cell_name', 'blister_id', 'domecasegap', 'domecasegap_limit', 'domecasegap_spc', 'stitcharea', 'stitcharea_limit', 'stitcharea_spc', 'minstitchwidth', 'bodytypeid', 'dometypeid', 'leaktest', 'laserpower', 'lotnumber', 'test_time_sec', 'date', 'error_code_number', 'pass_failed']
    df = df.select(*columns_to_keep)
    
    # Cast the 'batchid' column to integer
    df = df.withColumn("batchid", df["batchid"].cast("integer"))
    
    df_refine = df
    
    # Remove unwanted columns
    remove_col = ['blister_id', 'date', 'domecasegap_limit', 'domecasegap_spc', 'stitcharea_limit', 'stitcharea_spc', 'leaktest', 'laserpower', 'minstitchwidth']
    df_anomaly = df.drop(*remove_col)
    
    # Convert categorical columns to numerical codes
    for col in ['pass_failed', 'dometypeid', 'bodytypeid', 'error_code_number', 'lotnumber']:
        df_anomaly = df_anomaly.withColumn(col, df_anomaly[col].cast("integer"))
    
    # Handle missing values by replacing them with a specific value (e.g., -999)
    df_anomaly = df_anomaly.fillna(-999)
    
    return df_refine , df_anomaly

# Read the input DataFrame from Kafka
df = spark.read.format("kafka").option("kafka.bootstrap.servers", "cnt7-naya-cdh63:9092").option("subscribe", "get_sealing_raw_data").load()

# Convert the value column from binary to string
df = df.withColumn("value", df["value"].cast("string"))

# Split the value column into individual columns
df = df.selectExpr("split(value, ',') as value")

# Expand the array of values into separate columns
df = df.selectExpr("value[0] as BatchID", "value[1] as week", "value[2] as tp_cell_name", "value[3] as Blister_ID", "value[4] as DomeCaseGap", "value[5] as DomeCaseGap_Limit", "value[6] as DomeCaseGap_SPC", "value[7] as StitchArea", "value[8] as StitchArea_Limit", "value[9] as StitchArea_SPC", "value[10] as MinStitchWidth", "value[11] as BodyTypeID", "value[12] as DomeTypeID", "value[13] as LeakTest", "value[14] as LaserPower", "value[15] as LotNumber", "value[16] as Test_Time_Min", "value[17] as Date", "value[18] as ErrorCodeNumber", "value[19] as Pass_Failed")

# Apply data refinement function
df_refine, df_anomaly = SealingCell_data_refinning(df)

# Close the Kafka consumer
consumer.close()

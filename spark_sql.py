from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType, StringType, StructType, StructField

# Local paths
local_path_refine_output = "/home/naya/anomaly/files_parquet/scd_refine.parquet"
local_path_anomaly_output = "/home/naya/anomaly/files_parquet/scd_anomaly.parquet"
local_path_weeks_raws_output = "/home/naya/anomaly/files_parquet/scd_weeks_raws.parquet"

def spark_refine(df):
    try:
        # Create a SparkSession
        spark = SparkSession.builder.appName("Local_write_and_query").getOrCreate()

        # Define the schema based on the provided column names and types
        schema = StructType([
            StructField("_BatchID", IntegerType(), True),
            StructField("BatchName", StringType(), True),
            StructField("BatchPath", StringType(), True),
            StructField("ConfigID", IntegerType(), True),
            StructField("Shift", StringType(), True),
            StructField("Cell_Name", StringType(), True),
            StructField("TestTypeID", IntegerType(), True),
            StructField("LastInsertedRow", StringType(), True),
            StructField("StartTime", StringType(), True),
            StructField("Heads", StringType(), True),
            StructField("BatteryTypeID", IntegerType(), True),
            StructField("BodyTypeID", IntegerType(), True),
            StructField("DomeTypeID", IntegerType(), True),
            StructField("PCATypeID", IntegerType(), True),
            StructField("PCBTypeID", IntegerType(), True),
            StructField("PlacementContractorID", IntegerType(), True),
            StructField("EmployeID", IntegerType(), True),
            StructField("OpticsTypeID", IntegerType(), True),
            StructField("Versions", StringType(), True),
            StructField("LotNumber", StringType(), True),
            StructField("SubLot", StringType(), True),
            StructField("TestID", IntegerType(), True),
            StructField("Capsule_Number", IntegerType(), True),
            StructField("Initial_Capsule_Id_HEX", StringType(), True),
            StructField("Capsule_Id_HEX", StringType(), True),
            StructField("Test_Counter", IntegerType(), True),
            StructField("ERROR_Code_Number", IntegerType(), True),
            StructField("Capsule_Id", StringType(), True),
            StructField("PASS_FAILED", IntegerType(), True),
            StructField("Start_Time", StringType(), True),
            StructField("End_time", StringType(), True),
            StructField("Test_Time_min", StringType(), True),
            StructField("Date", StringType(), True),
            StructField("History", StringType(), True),
            StructField("Remark", StringType(), True),
            StructField("Blister_Id", IntegerType(), True),
            StructField("Fail_Tray", StringType(), True),
            StructField("Fail_capsule_position", IntegerType(), True),
            StructField("ERROR_Code_Description", StringType(), True),
            StructField("WTC", StringType(), True),
            StructField("CQC_num", StringType(), True),
            StructField("Capsule_Counter", IntegerType(), True),
            StructField("TP_Cell_Name", StringType(), True),
            StructField("DomeCaseGap", StringType(), True),
            StructField("LaserPower", StringType(), True),
            StructField("StitchArea", StringType(), True),
            StructField("MinStitchWidth", StringType(), True),
            StructField("LeakTest", StringType(), True),
            StructField("year", IntegerType(), True),
            StructField("quarter", IntegerType(), True),
            StructField("week", IntegerType(), True),
            StructField("month", IntegerType(), True),
            StructField("dayweek", IntegerType(), True),
            StructField("DomeCaseGap_spc_orig", StringType(), True),
            StructField("LaserPower_spc_orig", StringType(), True),
            StructField("MinStitchWidth_spc_orig", StringType(), True),
            StructField("StitchArea_spc_orig", StringType(), True),
            StructField("LeakTest_spc_orig", StringType(), True),
            StructField("DomeCaseGap_spc", StringType(), True),
            StructField("LaserPower_spc", StringType(), True),
            StructField("MinStitchWidth_spc", StringType(), True),
            StructField("StitchArea_spc", StringType(), True),
            StructField("LeakTest_spc", StringType(), True),
            StructField("BatchId", IntegerType(), True),
            StructField("DomeCaseGap_limit", StringType(), True),
            StructField("LaserPower_limit", StringType(), True),
            StructField("MinStitchWidth_limit", StringType(), True),
            StructField("StitchArea_limit", StringType(), True),
            StructField("LeakTest_limit", StringType(), True)
        ])

        # Convert Pandas DataFrame to Spark DataFrame using the defined schema
        spark_df = spark.createDataFrame(df, schema=schema)

        # Rename BatchID column to _BatchID
        spark_df = spark_df.withColumnRenamed("BatchID", "_BatchID")

        # Generate a unique file name using the UNIX timestamp
        spark_df = spark_df.withColumn("timestamp", F.unix_timestamp())

        # Convert column names to lowercase
        spark_df = spark_df.toDF(*[col.lower() for col in spark_df.columns])

        # Trim whitespace from string columns
        spark_df = spark_df.withColumn("pass_failed", F.trim(spark_df["pass_failed"]))

        # Filter out records where pass_failed is not "Pass"
        spark_df = spark_df.filter(spark_df["pass_failed"] == "Pass")

        # Convert columns to the appropriate types
        spark_df = spark_df.withColumn("batchid", spark_df["_batchid"].cast(IntegerType()))
        spark_df = spark_df.withColumn("bodytypeid", spark_df["bodytypeid"].cast(IntegerType()))
        spark_df = spark_df.withColumn("dometypeid", spark_df["dometypeid"].cast(IntegerType()))
        spark_df = spark_df.withColumn("error_code_number", spark_df["error_code_number"].cast(IntegerType()))
        spark_df = spark_df.withColumn("pass_failed", F.lit(1))

        # Select columns to keep
        columns_to_keep = [
            "batchid", "tp_cell_name", "domecasegap", "stitcharea", "bodytypeid", "dometypeid",
            "lotnumber", "test_time_sec", "error_code_number", "pass_failed"
        ]
        spark_df = spark_df.select(columns_to_keep)

        # Drop unnecessary columns
        remove_cols = ["_batchid", "timestamp"]
        spark_df = spark_df.drop(*remove_cols)

        # Save refined DataFrame as Parquet file locally
        spark_df.write.parquet(local_path_refine_output, mode="overwrite")

        # Drop unnecessary columns
        remove_col = [
            'blister_id', 'date', 'domecasegap_limit', 'domecasegap_spc', 'stitcharea_limit', 'stitcharea_spc',
            'leaktest', 'laserpower', 'minstitchwidth', '_batchid'
        ]
        spark_df = spark_df.drop(*remove_col)

        # Handle missing values by replacing them with a specific value (-999)
        spark_df = spark_df.fillna(-999)

        # Save anomaly DataFrame as Parquet file locally
        spark_df.write.parquet(local_path_anomaly_output, mode="overwrite")

        # Query and save grouped DataFrame
        query_week_grouped = """
            SELECT week,
                COUNT(*) AS count_of_records,
                MAX(domecasegap) AS maximum_domecasegap,
                MIN(domecasegap) AS minimum_domecasegap,
                MIN(stitcharea) AS minimum_stitcharea,
                AVG(domecasegap) AS week_mean,
                STDDEV(domecasegap) AS week_stddev,
                AVG(stitcharea) AS stitcharea_week_mean,
                STDDEV(stitcharea) AS stitcharea_week_stddev
            FROM raw_table
            GROUP BY week
        """
        grouped_df = spark.sql(query_week_grouped)
        grouped_df.write.parquet(local_path_weeks_raws_output, mode="overwrite")

        spark.stop()

        return True
    except Exception as e:
        print(f"An error occurred at local_to_refine_hdfs.py: {str(e)}")
        return False

if __name__ == '__main__':
    try:
        # Read raw DataFrame from Parquet file using PyArrow
        scd_raw = pq.read_table('/home/naya/anomaly/anomaly/sealing_anomaly/files_parquet/scd_raw.parquet').to_pandas()
        print(scd_raw.head())
        success = spark_refine(scd_raw)
        if success:
            print("Files are ready")
        else:
            print("ERRORS FOUND!")
    except Exception as error:
        print(f"Error in anomaly_analysis.py: {error}")

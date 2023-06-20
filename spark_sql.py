from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Local paths
local_path_refine_output = "/home/naya/anomaly/files_json/scd_refine.json"
local_path_anomaly_output = "/home/naya/anomaly/files_json/scd_anomaly.json"
local_path_weeks_raws_output = "/home/naya/anomaly/files_json/scd_weeks_raws.json"

def spark_refine(df):
    try:
        # Create a SparkSession
        spark = SparkSession.builder.appName("Local_write_and_query").getOrCreate()

        # Convert Pandas DataFrame to Spark DataFrame
        spark_df = spark.createDataFrame(df)

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

        # Save refined DataFrame as JSON file locally
        spark_df.write.json(local_path_refine_output)

        spark.stop()

        return True
    except Exception as e:
        print(f"An error occurred in spark_refine: {str(e)}")
        return False


def spark_anomaly(df):
    try:
        # Create a SparkSession
        spark = SparkSession.builder.appName("Local_write_and_query").getOrCreate()

        # Convert Pandas DataFrame to Spark DataFrame
        spark_df = spark.createDataFrame(df)

        # Drop unnecessary columns
        remove_col = [
            'blister_id', 'date', 'domecasegap_limit', 'domecasegap_spc', 'stitcharea_limit', 'stitcharea_spc',
            'leaktest', 'laserpower', 'minstitchwidth', '_batchid'
        ]
        spark_df = spark_df.drop(*remove_col)

        # Handle missing values by replacing them with a specific value (-999)
        spark_df = spark_df.fillna(-999)

        # Save anomaly DataFrame as JSON file locally
        spark_df.write.json(local_path_anomaly_output)

        spark.stop()

        return True
    except Exception as e:
        print(f"An error occurred in spark_anomaly: {str(e)}")
        return False


def spark_weekly_rows(df):
    try:
        # Create a SparkSession
        spark = SparkSession.builder.appName("Local_write_and_query").getOrCreate()

        # Convert Pandas DataFrame to Spark DataFrame
        spark_df = spark.createDataFrame(df)

        # Query and save grouped DataFrame
        grouped_df = spark_df.groupby("week").agg(
            F.count("*").alias("count_of_records"),
            F.max("domecasegap").alias("maximum_domecasegap"),
            F.min("domecasegap").alias("minimum_domecasegap"),
            F.min("stitcharea").alias("minimum_stitcharea"),
            F.avg("domecasegap").alias("week_mean"),
            F.stddev("domecasegap").alias("week_stddev"),
            F.avg("stitcharea").alias("stitcharea_week_mean"),
            F.stddev("stitcharea").alias("stitcharea_week_stddev")
        )

        # Save grouped DataFrame as JSON file locally
        grouped_df.write.json(local_path_weeks_raws_output)

        spark.stop()

        return True
    except Exception as e:
        print(f"An error occurred in spark_weekly_rows: {str(e)}")
        return False


if __name__ == '__main__':
    try:
        # Read raw DataFrame from JSON file
        scd_raw = spark.read.json('/home/naya/anomaly/files_json/scd_raw.json').toPandas()
        print(scd_raw.head())

        success_refine = spark_refine(scd_raw)
        success_anomaly = spark_anomaly(scd_raw)
        success_weekly_rows = spark_weekly_rows(scd_raw)

        if success_refine and success_anomaly and success_weekly_rows:
            print("Files are ready")
        else:
            print("ERRORS FOUND!")
    except Exception as error:
        print(f"Error in anomaly_analysis.py: {error}")

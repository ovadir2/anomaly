from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType
import pyarrow as pa

# Local paths
json_path = "/home/naya/anomaly/files_json/scd_raw.json"
# local_path_refine_output = "file://home/naya/anomaly/files_json/scd_refine.json"
# local_path_anomaly_output = "file://home/naya/anomaly/files_json/scd_anomaly.json"
# local_path_weeks_raws_output = "file://home/naya/anomaly/files_json/scd_weeks_raws.json"


fs = pa.hdfs.HadoopFileSystem(
    host='Cnt7-naya-cdh63',
    port=8020,
    user='hdfs',
    kerb_ticket=None,
    extra_conf=None)

def spark_refine(df):
    try:
       # Create a SparkSession
        spark = SparkSession.builder.appName("SCD_Refining").getOrCreate()

        # Convert pandas DataFrame to Spark DataFrame
        spark_df = spark.createDataFrame(df)

        # Generate a unique file name using the UNIX timestamp
        spark_df = spark_df.toDF(*[col.lower() for col in spark_df.columns])
        spark_df = spark_df.withColumn("pass_failed", F.trim(spark_df["pass_failed"]))

        # Filter out records where pass_failed is not "Pass"
        spark_df = spark_df.filter(spark_df["pass_failed"] == "Pass")

        # Drop rows with missing values in domecasegap and stitcharea columns
        spark_df = spark_df.dropna(subset=["domecasegap"])
        spark_df = spark_df.dropna(subset=["stitcharea"])

        # Convert test_time_min column to test_time_sec
        spark_df = spark_df.withColumn("test_time_sec", F.expr("hour(to_timestamp(test_time_min, 'HH:mm')) * 3600 + minute(to_timestamp(test_time_min, 'HH:mm')) * 60"))

        # Convert date column to datetime type
        spark_df = spark_df.withColumn("date", F.to_date(spark_df["date"], "dd/MM/yyyy"))

        # Select columns to keep
        columns_to_keep = [
            "week", "batchid", "tp_cell_name", "blister_id", "domecasegap", "domecasegap_limit", "domecasegap_spc",
            "stitcharea", "stitcharea_limit", "stitcharea_spc", "minstitchwidth", "bodytypeid", "dometypeid",
            "leaktest", "laserpower", "lotnumber", "test_time_sec", "date", "error_code_number", "pass_failed"
        ]
        spark_df = spark_df.select(columns_to_keep)

        # Save refined DataFrame as JSON file locally
        #spark_df.write.json(local_path_refine_output)
        # Append the DataFrame to the destination file
        #spark_df.write.mode('append').csv('/user/hive/warehouse/scd_raw_db/scd_raw.csv')

        spark.stop()

        return spark_df
    
    except Exception as e:
        print(f"An error occurred in spark_refine: {str(e)}")
        return None

# def spark_anomaly(df):
#     try:
#         # Create a SparkSession
#         spark = SparkSession.builder.appName("Local_write_and_query").getOrCreate()

#         # Convert Pandas DataFrame to Spark DataFrame
#         spark_df = spark.createDataFrame(df)

#         # Drop unnecessary columns
#         remove_cols = [
#             'blister_id', 'date', 'domecasegap_limit', 'domecasegap_spc', 'stitcharea_limit', 'stitcharea_spc',
#             'leaktest', 'laserpower', 'minstitchwidth', '_batchid'
#         ]
#         spark_df = spark_df.drop(*remove_cols)

#         # Handle missing values by replacing them with a specific value (-999)
#         spark_df = spark_df.fillna(-999)

#         # Save anomaly DataFrame as JSON file locally
#         spark_df.write.json(local_path_anomaly_output)

#         spark.stop()

#         return spark_df
#     except Exception as e:
#         print(f"An error occurred in spark_anomaly: {str(e)}")
#         return None

# def spark_weekly_rows(df):
#     try:
#         # Create a SparkSession
#         spark = SparkSession.builder.appName("Local_write_and_query").getOrCreate()

#         # Convert Pandas DataFrame to Spark DataFrame
#         spark_df = spark.createDataFrame(df)

#         # Group by week and count the number of rows
#         spark_df = spark_df.withColumn("date", F.to_date(spark_df["date"]))
#         spark_df = spark_df.withColumn("week", F.weekofyear(spark_df["date"]))
#         spark_df = spark_df.groupBy("week").count()

#         # Save weekly rows DataFrame as JSON file locally
#         spark_df.write.json(local_path_weeks_raws_output)

#         spark.stop()

#         return spark_df
#     except Exception as e:
#         print(f"An error occurred in spark_weekly_rows: {str(e)}")
#         return None

if __name__ == "__main__":
    import pandas as pd
    try:
        df= pd.DataFrame()
        # Refine the data
        refined_df = spark_refine(df)
       # Read JSON file into Pandas DataFrame
        df = pd.read_json(json_path)
        fs.chmod('hdfs://Cnt7-naya-cdh63:8020/user/naya/anomaly', 775)

        # Change access permissions for a file in the staging directory in order
        # to allow for naya user to get this file only
        fs.chown('hdfs://Cnt7-naya-cdh63:8020/user/naya/anomaly', owner='naya', group='naya')

        if refined_df is not None:
            print("Data refinement completed successfully!")
            #refined_df.write.format("csv").option("header", "true").mode("overwrite").save('hdfs://Cnt7-naya-cdh63:8020/user/naya/anomaly/refined_df.csv')
            refined_df.write.format("json").mode("append").save('hdfs://Cnt7-naya-cdh63:8020/user/naya/anomaly/refined_df.json')
        else:
            print("Data refinement failed!")

        # # Perform anomaly detection
        # anomaly_df = spark_anomaly(refined_df)

        # if anomaly_df is not None:
        #     print("Anomaly detection completed successfully!")
        #     anomaly_df.write.format("csv").option("header", "true").mode("overwrite").save('/home/naya/anomaly/files_csv/anomaly_df.csv')

        # else:
        #     print("Anomaly detection failed!")

        # # Generate weekly rows report
        # weekly_rows_df = spark_weekly_rows(anomaly_df)

        # if weekly_rows_df is not None:
        #     print("Weekly rows report generated successfully!")
        #     weekly_rows_df.write.format("csv").option("header", "true").mode("overwrite").save('/home/naya/anomaly/files_csv/scd_weeks_raws.csv')

        # else:
        #     print("Failed to generate weekly rows report!")

        # # print("Processing completed.")
    except Exception as e:
        print(f"ERROR FOUND!: {str(e)}")

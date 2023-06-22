from pyspark.sql import SparkSession
import os
import pyarrow as pa

fs = pa.hdfs.HadoopFileSystem(
    host='Cnt7-naya-cdh63',
    port=8020,
    user='hdfs',
    kerb_ticket=None,
    extra_conf=None)
def create_path():
    #Note: will delete only if exist /tmp/sqoop/staging
    if fs.exists('hdfs://Cnt7-naya-cdh63:8020/user/naya/anomaly'):
        fs.rm('hdfs://Cnt7-naya-cdh63:8020/user/naya/anomaly', recursive=True)


    #First we use mkdir() to create a staging area in HDFS under /user/naya/anomaly.
    fs.mkdir('hdfs://Cnt7-naya-cdh63:8020/user/naya/anomaly', create_parents=True)

    #Next, we use upload() to copy all *.sql files created locally to the staging area.
    return True

def copy_local_to_hdfs(extension_='.json'): #.'.json' , '.csv' ,...
    local_path = '/home/naya/anomaly/files_csv'
    extension = extension_
    files = [f for f in os.listdir(local_path) if f.endswith(extension)]
    print(files)

    #,'rb'
    for f_name in sql_files:
        with open(local_path + f_name,'rb') as f:
            dst = f'hdfs://Cnt7-naya-cdh63:8020/tmp/sqoop/staging/{f_name}'
            print('uploading', f_name, 'to', dst)
            fs.upload(dst, f)



if __name__ == '__main__':
    success = False
    success = create_path()
    if success:

    # Finally, using ls() we verify the content of the folder
    sql_dir_files = fs.ls('hdfs://Cnt7-naya-cdh63:8020/tmp/sqoop/staging', detail=False)
    print(sql_dir_files)
    return True








# Create a SparkSession
spark = SparkSession.builder.getOrCreate()





# Read the source file as a DataFrame
source_df = spark.read.csv('/home/naya/anomaly/anomaly/sealing_anomaly/hdfs/scd_raw.csv', header=True)

# Append the DataFrame to the destination file
source_df.write.mode('append').csv('/user/hive/warehouse/scd_raw_db/scd_raw.csv')

# Stop the SparkSession
spark.stop()

from kafka import KafkaConsumer
import pandas as pd
import json
import seaborn as sns
import pyarrow.hdfs as hdfs
import os

# Kafka configuration
host = 'cnt7-naya-cdh63'
port = '9092'
bootstrap_servers = f'{host}:{port}'
topic = 'get_sealing_raw_data'
group_id = 'prepare_predict_HDFS'
enable_auto_commit = True
auto_commit_interval_ms = 5000
auto_offset_reset = 'earliest'
value_deserializer = lambda x: x  # Return bytes without decoding


def sealing_cell_data_refining(json_messages):
    df = pd.DataFrame(json_messages)
    
    df = df.applymap(lambda x: x.strip() if isinstance(x, str) else x)
    # Lowercase all column names
    df.columns = df.columns.str.lower()   
    df = df[df['pass_failed'] == 'Pass']

    df.dropna(subset=['domecasegap'], inplace=True)
    df.dropna(subset=['stitcharea'], inplace=True)
    time = pd.to_datetime(df['test_time_min'], format='%H:%M')
    # Calculate duration in minutes and seconds
    in_minutes = time.dt.hour * 60 + time.dt.minute
    df['test_time_sec'] = in_minutes * 60    # Calculate the required statistics
    df['date'] = pd.to_datetime(df['date'], errors='coerce')
    
    columns_to_keep = ['week', 'year', 'batchid', 'tp_cell_name', 'blister_id', 'domecasegap', 'domecasegap_limit','domecasegap_spc',\
                    'stitcharea','stitcharea_limit','stitcharea_spc', \
                    'minstitchwidth', 'bodytypeid', 'dometypeid', 'leaktest', 'laserpower', 'lotnumber',\
                    'test_time_sec', 'date', 'error_code_number', 'pass_failed']

    df = df[columns_to_keep]
    scd_refine = df
    df['batchid'] = df['batchid'].astype(int)
    

    remove_col = ['blister_id','date','year','domecasegap_limit','domecasegap_spc','stitcharea_limit','stitcharea_spc','leaktest','laserpower','minstitchwidth']
    scd_anomaly = df.drop(columns = remove_col) 
    for col in ['pass_failed','dometypeid', 'bodytypeid','error_code_number','lotnumber']:
        scd_anomaly[col] = scd_anomaly[col].astype('category').cat.codes

    remaim_col = scd_anomaly.columns

    # Handle missing values by replacing them with a specific value (e.g., -999)
    scd_anomaly = scd_anomaly.fillna(-999)

    
    return scd_anomaly, scd_refine

def scd_weeks_group(scd_refine):
    scd_weeks_raws = scd_refine.groupby('week').agg({
        'domecasegap': ['max', 'min', 'mean', 'std'],
        'stitcharea': ['max', 'min', 'mean', 'std'],
    })

    # Rename the columns for clarity
    scd_weeks_raws.columns = [
        'maximum_domecasegap', 'minimum_domecasegap',\
        'domecasegap_week_mean', 'domecasegap_week_stddev', \
        'maximum_stitcharea', 'minimum_stitcharea',\
        'stitcharea_week_mean', 'stitcharea_week_stddev'
    ]
    # Add the 'week' column
    scd_weeks_raws['week'] = scd_weeks_raws.index
    # Perform the second aggregation using scd_refine DataFrame
    
    scd_weeks_raws['stitcharea_week_mean'] = scd_refine.groupby('week')['stitcharea'].mean()
    scd_weeks_raws['stitcharea_week_stddev'] = scd_refine.groupby('week')['stitcharea'].std()
    scd_weeks_raws['domecasegap_week_mean'] = scd_refine.groupby('week')['domecasegap'].mean()
    scd_weeks_raws['domecasegap_week_stddev'] = scd_refine.groupby('week')['domecasegap'].std()
    scd_weeks_raws['maximum_domecasegap'] = scd_refine.groupby('week')['domecasegap'].max()
    scd_weeks_raws['minimum_domecasegap'] = scd_refine.groupby('week')['domecasegap'].min()
    scd_weeks_raws['maximum_stitcharea'] = scd_refine.groupby('week')['stitcharea'].max()
    scd_weeks_raws['minimum_stitcharea'] = scd_refine.groupby('week')['stitcharea'].min()

    return(scd_weeks_raws)

def write_appended_hdfs(fname, df):
    fs = hdfs.HadoopFileSystem(
        host='Cnt7-naya-cdh63',
        port=8020,
        user='hdfs',
        kerb_ticket=None,
        extra_conf=None
    )

    path = 'hdfs://Cnt7-naya-cdh63:8020/user/naya/'
    directory = path + 'anomaly'
    file_path = directory + '/' + fname + '.json'

    if not fs.exists(directory):
        fs.mkdir(directory)

    # Check if file exists
    if fs.exists(file_path):
        with fs.open(file_path, 'rb') as f:
            existing_data = f.read().decode('utf-8')
        
        # Convert existing data to DataFrame
        if existing_data:
            existing_df = pd.read_json(existing_data)
        else:
            existing_df = pd.DataFrame()

        # Append new data to existing DataFrame
        combined_df = existing_df.append(df, ignore_index=True)
        json_data = combined_df.to_json()
    else:
        json_data = df.to_json()

    with fs.open(file_path, 'wb') as f:
        f.write(json_data.encode('utf-8'))

def write_hdfs(fname, df):
    fs = hdfs.HadoopFileSystem(
        host='Cnt7-naya-cdh63',
        port=8020,
        user='hdfs',
        kerb_ticket=None,
        extra_conf=None
    )

    path = 'hdfs://Cnt7-naya-cdh63:8020/user/naya/'

    try:
        fs.mkdir(path + 'anomaly')
    except FileExistsError:
        pass  # Directory already exists, no need to create it

    file_path = path + 'anomaly' + '/' + fname + '.json'
    with fs.open(file_path, 'wb') as f:
        json_bytes = df.to_json().encode('utf-8')
        f.write(json_bytes)


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

# Collect all messages into a list
messages = []
file_size = 0
message_count = 0
#consumer.poll()

for message in consumer:
    message_value = message.value.decode('utf-8')  # Decode the message value
    if message_value.startswith('file_size:'):
        file_size = int(message_value.split(':')[1]) 
        print(f"Received file size: {file_size}")
    else:
        messages.append(message_value)
        message_count += 1

    if file_size > 0 and message_count == file_size:
        # Convert the messages list to JSON objects
        json_messages = [json.loads(msg) for msg in messages]

        # Apply data refining function
        scd_anomaly, scd_refine = sealing_cell_data_refining(json_messages)
        write_hdfs('scd_refine',scd_refine)

        scd_weeks_raws = scd_weeks_group(scd_refine)
        
        write_appended_hdfs('scd_weeks_raws', scd_weeks_raws)
        
        if __name__ == '__main__':
                    print("scd_anomaly:")
                    print(scd_anomaly)
                    print("scd_refine:")
                    print(scd_refine)   
                    print("scd_weeks_raws:")
                    print(scd_weeks_raws)   
                    
                    
                    break

      # Print the running number on the same position
    else:
        print(f"{message_count}", end='\r')

# Close the Kafka consumer
consumer.close()

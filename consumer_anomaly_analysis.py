from kafka import KafkaConsumer
import pandas as pd
import json
from sklearn.ensemble import IsolationForest
import numpy as np
import seaborn as sns
import pyarrow.hdfs as hdfs

# Kafka configuration
host = 'cnt7-naya-cdh63'
port = '9092'
bootstrap_servers = f'{host}:{port}'
topic = 'get_sealing_raw_data'
group_id = 'prepare_anomaly'
enable_auto_commit = True
auto_commit_interval_ms = 5000
auto_offset_reset = 'earliest'
value_deserializer = lambda x: x  # Return bytes without decoding




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
    
    columns_to_keep = ['week', 'year','batchid', 'tp_cell_name', 'blister_id', 'domecasegap', 'domecasegap_limit','domecasegap_spc',\
                    'stitcharea','stitcharea_limit','stitcharea_spc', \
                    'minstitchwidth', 'bodytypeid', 'dometypeid', 'leaktest', 'laserpower', 'lotnumber',\
                    'test_time_sec', 'date', 'error_code_number', 'pass_failed']

    df = df[columns_to_keep]
    scd_refine = df
    df['batchid'] = df['batchid'].astype(int)
    
    remove_col = ['blister_id','date','domecasegap_limit','domecasegap_spc','stitcharea_limit','stitcharea_spc','leaktest','laserpower','minstitchwidth']
    scd_anomaly = df.drop(columns = remove_col) 
    for col in ['pass_failed','dometypeid', 'bodytypeid','error_code_number','lotnumber']:
        scd_anomaly[col] = scd_anomaly[col].astype('category').cat.codes
    remaim_col = scd_anomaly.columns
    # Handle missing values by replacing them with a specific value (e.g., -999)
    scd_anomaly = scd_anomaly.fillna(-999)
    return scd_anomaly, scd_refine
 
def check_anomalies(scd_anomaly, contamination=0.05, n_estimators=100):
    # Adjust the contamination value and number of estimators
    isolation_forest = IsolationForest(contamination=contamination, n_estimators=n_estimators)
    # Fit the model to the dat
    isolation_forest.fit(scd_anomaly)

    # Predict the anomalies in the data
    scd_anomaly['anomaly'] = isolation_forest.predict(scd_anomaly)
    return scd_anomaly
       
def spc_trend(df, feature, hi_limit=None, lo_limit=None, hi_value=None, lo_value=None):
    df = df.copy()  # Create a copy of the DataFrame to avoid modifying the original data
    window_size = 10
    sigma = 2

    # Compute moving average and standard deviation
    df['MA'] = df[feature].rolling(window=window_size, min_periods=1).mean()
    df['STD'] = df[feature].rolling(window=window_size, min_periods=1).std()

    # Define SPC limits based on moving average and standard deviation
    df['SPC_Lower'] = df['MA'] - sigma * df['STD']
    df['SPC_Upper'] = df['MA'] + sigma * df['STD']

    lo_spc = df['SPC_Lower'].mean()
    hi_spc = df['SPC_Upper'].mean()

    # Find spc-alarm
    if lo_value < lo_spc and hi_value > hi_spc:
        trend = "Within SPC limits"
        alarm = f"0:{trend}"
    elif lo_value * 1.1 >= lo_spc or hi_value * 0.9 <= hi_spc:
        trend = "Approaching SPC limits"
        alarm = f"1:{trend}"
    elif lo_value > lo_spc:
        trend = "Below SPC limits"
        alarm = f"2:{trend}"
    elif hi_value > hi_spc:
        trend = "Above SPC limits"
        alarm = f"3:{trend}"
    elif hi_value > hi_limit or lo_value < lo_limit:
        trend = "Above limits"
        alarm = f"4:{trend}"
    else:
        trend = "Unknown"
        alarm = f"5:{trend}"

    df['alarm'] = alarm

    return df

def read_hdfs(file_path):
    fs = hdfs.HadoopFileSystem(
        host='Cnt7-naya-cdh63',
        port=8020,
        user='hdfs',
        kerb_ticket=None,
        extra_conf=None
    )

    if fs.exists(file_path):
        with fs.open(file_path, 'rb') as f:
            json_bytes = f.read()
            json_str = json_bytes.decode('utf-8')
            df = pd.read_json(json_str)
            return df
    else:
        return pd.DataFrame()


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
#consumer1.poll()

for message in consumer:
    message_value = message.value.decode('utf-8')  # Decode the message value
    if message_value.startswith('file_size:'):
        file_size = int(message_value.split(':')[1]) 
        print(f"Received file size: {file_size}")
    else:
        messages.append(message_value)
        message_count += 1

    if file_size > 0 and message_count == file_size:
        file_size = 0
        message_count = 0
        # Convert the messages list to JSON objects
        json_messages = [json.loads(msg) for msg in messages]

        # Apply data refining function
        scd_anomaly, scd_refine = sealing_cell_data_refining(json_messages)
        #write_hdfs('scd_refine',scd_refine)

        scd_anomaly_check= check_anomalies(scd_anomaly, contamination=0.05, n_estimators=100);
        write_hdfs('scd_anomaly_check',scd_anomaly_check)

        scd_only_anomaly = scd_anomaly[scd_anomaly_check['anomaly'] == -1]

        features = ["domecasegap", "stitcharea"]
        scd_refine_first_row = scd_refine.iloc[0]  # Get the first row of scd_refine

        for feature in features:
            
            # Extract the limit values from the first row of scd_refine
            limit_values = scd_refine_first_row[f'{feature}_limit']
            if limit_values:
                lo_limit, hi_limit = limit_values.split(':')
                lo_limit = float(lo_limit) if lo_limit else float('-inf')
                hi_limit = float(hi_limit) if hi_limit else float('inf')

                # Set the value range for the feature
                lo_value = scd_only_anomaly[feature].min()
                hi_value = scd_only_anomaly[feature].max()

                # Display the trend plot
                scd_only_anomaly_trend = spc_trend(scd_only_anomaly, feature, hi_limit, lo_limit, hi_value, lo_value)
                
                read_scd_only_anomaly_trend_path = 'hdfs://Cnt7-naya-cdh63:8020/user/naya/anomaly/scd_only_anomaly_trend.json'
                read_scd_only_anomaly_trend = read_hdfs(read_scd_only_anomaly_trend_path)
                if not read_scd_only_anomaly_trend.empty:
                    read_scd_only_anomaly_trend['year'] = read_scd_only_anomaly_trend['year'].astype(int)
                    read_scd_only_anomaly_trend['week'] = read_scd_only_anomaly_trend['week'].astype(int)
                    # Check if year and week already exist before updating
                    if any((entry['year'] == row['year'] and entry['week'] == row['week']) for entry in scd_only_anomaly_trend.to_dict('records') for row in read_scd_only_anomaly_trend.to_dict('records')):
                        print("Year and week combination already exists. Skipping write operation.")
                    else:
                        # Check if year and week already exist before updating
                        print("Year and week combination new. write to HDFS")
                        write_hdfs('scd_only_anomaly_trend',scd_only_anomaly_trend)
                else:
                    write_hdfs('scd_only_anomaly_trend',scd_only_anomaly_trend)
  
        if __name__ == '__main__':
            print(scd_anomaly)
            print(scd_refine)  
            print(scd_anomaly_check)  
            print(scd_only_anomaly)  
            print(scd_only_anomaly_trend) 
             #break

    else:
        print(f"{message_count}", end='\r')
# Close the Kafka consumer
consumer.close()


from kafka import KafkaConsumer
import pandas as pd
import jsonschema
import json


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
            
            columns_to_keep = ['week','batchid', 'tp_cell_name', 'blister_id', 'domecasegap', 'domecasegap_limit','domecasegap_spc',\
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
         # Select a single message from the messages list
        # json_message= json.loads(messages[0])

        # # Generate the schema
        # schema = jsonschema.Draft7Validator(json_message).schema

        # # Print the schema
        # print(json.dumps(schema, indent=4))


        # Convert the PyArrow Table to a PyArrow RecordBatch
        # record_batch = table.to_batches()[0]

        # # Perform data refining operations on the RecordBatch
                    

        # Convert the messages list to JSON objects
        json_messages = [json.loads(msg) for msg in messages]

        # Apply data refining function
        scd_anomaly, scd_refine = sealing_cell_data_refining(json_messages)


        if __name__ == '__main__':
                    # Print the anomaly 
                    print(scd_anomaly)
                    # Print the refined schema
                    print(scd_refine)    # Print the running number on the same position
                    
                    break

      # Print the running number on the same position
    else:
        print(f"{message_count}", end='\r')

# Close the Kafka consumer
consumer.close()

import seaborn as sns
import pyarrow.hdfs as hdfs
import pandas as pd
import litelearn
import pickle
import os
import argparse
from datetime import datetime


def read_hdfs(file_path):
    fs = hdfs.HadoopFileSystem(
        host='Cnt7-naya-cdh63',
        port=8020,
        user='hdfs',
        kerb_ticket=None,
        extra_conf=None
    )

    with fs.open(file_path, 'rb') as f:
        json_bytes = f.read()
        json_str = json_bytes.decode('utf-8')
        df = pd.read_json(json_str)
    return df

def prediction_train(scd_weeks_raws):
        print("Trainning the model")
        ref=0.2*len(scd_weeks_raws)
        validation = scd_weeks_raws.sample(int(ref))
        scd_weeks_train=scd_weeks_raws.drop(validation.index)
        print('test', len(validation))
        train = scd_weeks_train
        print('train:',len(scd_weeks_train))
        pred_MA_stitcharea = litelearn.regress_df(train, 'stitcharea_week_mean') 
        pred_STD_stitcharea = litelearn.regress_df(train, 'stitcharea_week_stddev') 
        return pred_MA_stitcharea, pred_STD_stitcharea

def model_evaluation_plot(pred_MA_stitcharea, pred_STD_stitcharea):
    pred_STD_stitcharea.get_evaluation()
    pred_MA_stitcharea.display_residuals()
    pred_MA_stitcharea.display_shap()
    pred_STD_stitcharea.display_shap()
    
def train_valdation(pred_MA_stitcharea, pred_STD_stitcharea):
    print("Validation... checking ")
    pred_ref_STD_stitcharea = pred_STD_stitcharea.predict(validation)
    pred_ref_MA_stitcharea = pred_MA_stitcharea.predict(validation)
    validation= validation.assign(pred_STD_stitchareae=pred_ref_STD_stitcharea).assign(pred_MA_stitchareae=pred_ref_MA_stitcharea)
    print(validation)

def save_the_model(path_to_models, pred_MA_stitcharea, pred_STD_stitcharea):
    print("New model saving...")
    fs = hdfs.HadoopFileSystem(
        host='Cnt7-naya-cdh63',
        port=8020,
        user='hdfs',
        kerb_ticket=None,
        extra_conf=None
    )
    
    if not fs.exists(path_to_models):
        fs.mkdir(path_to_models)
    
    with fs.open(os.path.join(path_to_models, 'pred_MA_stitcharea.bin'), 'wb') as f1:
        pickle.dump(pred_MA_stitcharea, f1)
    with fs.open(os.path.join(path_to_models, 'pred_STD_stitcharea.bin'), 'wb') as f2:
        pickle.dump(pred_STD_stitcharea, f2)
       
    return True

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
    else:
        existing_df = pd.DataFrame()

    # Concatenate the existing DataFrame with the new DataFrame
    appended_df = pd.concat([existing_df, df], ignore_index=True)

    # Write the appended DataFrame to HDFS
    with fs.open(file_path, 'wb') as f:
        f.write(appended_df.to_json().encode('utf-8'))
    
def triger_alarm_table(use_pred_MA_stitcharea, use_pred_STD_stitcharea,MIN_PRED_RECORD_value=20 , NEXT_TRAIN_QTY_value=100):
    scd_refine_path = 'hdfs://Cnt7-naya-cdh63:8020/user/naya/anomaly/scd_refine.json'
    scd_weeks_raws_path = 'hdfs://Cnt7-naya-cdh63:8020/user/naya/anomaly/scd_weeks_raws.json'
    scd_only_anomaly_trend_path = 'hdfs://Cnt7-naya-cdh63:8020/user/naya/anomaly/scd_only_anomaly_trend.json'

    #year_value=print(datetime.now().year) 
    scd_refine = read_hdfs(scd_refine_path)
    scd_weeks_raws = read_hdfs(scd_weeks_raws_path)
    scd_only_anomaly_trend = read_hdfs(scd_only_anomaly_trend_path)
    #assuming weekly new data row is appended to this data file 
    week_record = scd_weeks_raws.tail(1)
    year_value = scd_refine['year'].values[0]+1 if week_record['week'].index[0] < 52 else  scd_refine['year'].values[0]
    current_week_value = scd_refine['week'].values[0] 
    next_week_value = current_week_value + 1 if current_week_value < 52 else 1
    spc_lower_limit = scd_only_anomaly_trend[scd_only_anomaly_trend['week']==current_week_value]['SPC_Lower'].mean()
    spc_upper_limit = scd_only_anomaly_trend[scd_only_anomaly_trend['week']==current_week_value]['SPC_Upper'].mean()
    df_row = pd.DataFrame({
        'week': current_week_value,
        'pred_year': year_value,
        'pred_week': next_week_value,
        'pred_stitcharea_week_mean': use_pred_MA_stitcharea.predict(week_record),
        'pred_stitcharea_week_stddev': use_pred_STD_stitcharea.predict(week_record),
        'spc_lower_limit': spc_lower_limit,
        'spc_upper_limit': spc_upper_limit,
        'predicted_stitcharea_calculate': use_pred_MA_stitcharea.predict(week_record) + 2 * use_pred_STD_stitcharea.predict(week_record),
        'alarm_pre_stitcharea': 0,
        're-train_required': 0,
        'additional_recorrds_needed': 0,
        'next_retraining_and_assigned': NEXT_TRAIN_QTY_value,
        'minimum_train_records_qty': MIN_PRED_RECORD_value
    })
    predicted_stitcharea = df_row['predicted_stitcharea_calculate'].values[0]
    spc_lower_limit = df_row['spc_lower_limit'].values[0]
    spc_upper_limit = df_row['spc_upper_limit'].values[0]
    
    if len(scd_weeks_raws) > df_row['minimum_train_records_qty'].iloc[0]:
        if len(scd_weeks_raws) < df_row['next_retraining_and_assigned'].iloc[0]:
            # Compare the predicted stitch area to the SPC limits
                is_alarm = (predicted_stitcharea < spc_lower_limit) | (predicted_stitcharea > spc_upper_limit)
                if is_alarm.any():
                    msg =f"Alarm: predicted_stitcharea {predicted_stitcharea:03f} is out of SPC limits {spc_lower_limit:03f}: {spc_upper_limit:03f}"
                    df_row['alarm_pre_stitcharea'] = 1
                else:
                    msg =f"No Alarm: predicted_stitcharea {predicted_stitcharea} is within SPC limits {spc_lower_limit}: {spc_upper_limit}"
                    df_row['alarm_pre_stitcharea'] = 0
                df_row['re-train_required'] = 0
        else:
            msg ="Need to re-train the data....."
            df_row['re-train_required'] = 1
    else:
        msg="Need to have more data for prediction model training...."
        df_row['additional_recorrds_needed'] = 1
        msg=('Need to have more data for prediction model training....')
    return msg,df_row
        
    
def read_model(path):
    print("Using existing model...")
    fs = hdfs.HadoopFileSystem(
        host='Cnt7-naya-cdh63',
        port=8020,
        user='hdfs',
        kerb_ticket=None,
        extra_conf=None
    )
    
    try:
        fs.mkdir(path)
    except FileExistsError:
        pass  # Directory already exists, no need to create it
   
    with fs.open(path+'pred_MA_stitcharea.bin', 'rb') as f2:
       use_pred_MA_stitcharea = pickle.load(f2)
    with fs.open(path + 'pred_STD_stitcharea.bin', 'rb') as f2:
        use_pred_STD_stitcharea = pickle.load(f2)
    
    return use_pred_MA_stitcharea, use_pred_STD_stitcharea


 
if __name__ == "__main__":
    triger_alarm_table_path = 'hdfs://Cnt7-naya-cdh63:8020/user/naya/anomaly/triger_alarm_table.json'
    # Create an argument parser
    parser = argparse.ArgumentParser()
    # Add the --history option
    parser.add_argument("--MIN_PRED_RECORD", type=int, default=20, help="Specify value that decide if trainer is valid")
    parser.add_argument("--NEXT_TRAIN_QTY", type=int, default=100, help="Specify the value, when to activate the tranner model")
    # Parse the command line arguments
    args = parser.parse_args() 
 
    MIN_PRED_RECORD_value = args.MIN_PRED_RECORD
    NEXT_TRAIN_QTY_value = args.NEXT_TRAIN_QTY
    
    path = 'hdfs://cnt7-naya-cdh63:8020/user/naya/anomaly/'
    scd_weeks_raws_file_path = (path + 'scd_weeks_raws.json')

 
    scd_weeks_raws = read_hdfs(scd_weeks_raws_file_path)
    print('MIN_PRED_RECORD = ', MIN_PRED_RECORD_value)
    print('NEXT_TRAIN_QTY = ', NEXT_TRAIN_QTY_value)
     
    if len(scd_weeks_raws) >= MIN_PRED_RECORD_value:
        if len(scd_weeks_raws) >= NEXT_TRAIN_QTY_value:
            pred_MA_stitcharea, pred_STD_stitcharea = prediction_train(scd_weeks_raws)
            save_the_model(path,pred_MA_stitcharea, pred_STD_stitcharea)
        else:
            pred_MA_stitcharea, pred_STD_stitcharea = read_model(path)
            msg,df_row = triger_alarm_table(pred_MA_stitcharea, pred_STD_stitcharea,MIN_PRED_RECORD_value,NEXT_TRAIN_QTY_value)
            print(msg)
            write_appended_hdfs('triger_alarm_table',df_row)
 
    else:
        print("not enought records to train....")
    

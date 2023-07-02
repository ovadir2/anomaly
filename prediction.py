import seaborn as sns
import pyarrow.hdfs as hdfs
import pandas as pd
import litelearn
import pickle
import os
import argparse


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
    pred_ref_STD_stitcharea = pred_STD_stitcharea.predict(validation)
    pred_ref_MA_stitcharea = pred_MA_stitcharea.predict(validation)
    validation= validation.assign(pred_STD_stitchareae=pred_ref_STD_stitcharea).assign(pred_MA_stitchareae=pred_ref_MA_stitcharea)
    print(validation)

def save_the_model(path_to_models, pred_MA_stitcharea, pred_STD_stitcharea):
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

def read_model(path_to_models):
    
    fs = hdfs.HadoopFileSystem(
        host='Cnt7-naya-cdh63',
        port=8020,
        user='hdfs',
        kerb_ticket=None,
        extra_conf=None
    )
    
    try:
        fs.mkdir(path_to_models)
    except FileExistsError:
        pass  # Directory already exists, no need to create it
   
    with open(path_to_models+ '/'+ 'pred_MA_stitcharea.bin', 'rb') as f2:
       use_pred_MA_stitcharea = pickle.load(f2)
    with open(path_to_models + '/'+ 'pred_STD_stitcharea.bin', 'rb') as f2:
        use_pred_STD_stitcharea = pickle.load(f2)
    
    return use_pred_MA_stitcharea, use_pred_STD_stitcharea



if __name__ == "__main__":
    # Create an argument parser
    parser = argparse.ArgumentParser()
    # Add the --history option
    parser.add_argument("--MIN_PRED_RECORD ", type=int, default=20, help="Specify value that decide if trainer is valid")
    parser.add_argument("--NEXT_TRAIN_QTY ", type=int, default=100, help="Specify the value, when to activate the tranner model")
    # Parse the command line arguments
    args = parser.parse_args() 
 
    MIN_PRED_RECORD_value = args.MIN_PRED_RECORD
    NEXT_TRAIN_QTY_value = args.NEXT_TRAIN_QTY
 
    scd_weeks_raws_file_path = ('hdfs:///user/naya/anomaly/scd_weeks_raws.json')
    stitcharea_MA_STD_models = ('hdfs:///user/naya/anomaly')

 
    scd_weeks_raws = read_hdfs(scd_weeks_raws_file_path)
    print('MIN_PRED_RECORD = ', MIN_PRED_RECORD_value)
    print('NEXT_TRAIN_QTY = ', NEXT_TRAIN_QTY_value)
     
    if len(scd_weeks_raws) >= MIN_PRED_RECORD_value:
        if len(scd_weeks_raws) >= NEXT_TRAIN_QTY_value:
            pred_MA_stitcharea, pred_STD_stitcharea = prediction_train(scd_weeks_raws)
            save_the_model(stitcharea_MA_STD_models,pred_MA_stitcharea, pred_STD_stitcharea)
        else:
            pred_MA_stitcharea, pred_STD_stitcharea = read_model(stitcharea_MA_STD_models)  
    else:
        print("not enought records to train....")
    

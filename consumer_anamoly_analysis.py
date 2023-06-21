#import streamlit as st
from sklearn.ensemble import IsolationForest
import pyarrow.parquet as pq
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import os
import math

def spark_anomaly(df):
    try:
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

        return True
    except Exception as e:
        print(f"An error occurred in spark_anomaly: {str(e)}")
        return False
    
    
def check_anomalies(scd_anomaly, contamination=0.05, n_estimators=100):
    # Adjust the contamination value and number of estimators
    isolation_forest = IsolationForest(contamination=contamination, n_estimators=n_estimators)
    # Fit the model to the data
    isolation_forest.fit(scd_anomaly)

    # Predict the anomalies in the data
    scd_anomaly['anomaly'] = isolation_forest.predict(scd_anomaly)

    display(scd_anomaly[scd_anomaly['anomaly'] == -1])
    return scd_anomaly
def display_scd_anomaly(scd_anomaly):
    lotnumbers = scd_anomaly['lotnumber'].unique()

    # Filter out lotnumbers with less than 800 items
    lotnumbers_filtered = [lotnumber for lotnumber in lotnumbers if len(scd_anomaly[scd_anomaly['lotnumber'] == lotnumber]) >= 800]

    # Limit the number of subplots to a maximum of 10
    num_subplots = min(len(lotnumbers_filtered), 10)

    # Set the spacing between the subplots
    fig, axes = plt.subplots(num_subplots, 1, figsize=(8, num_subplots * 7))

    # Create separate scatter plots for each lotnumber
    for i, lotnumber in enumerate(lotnumbers_filtered):
        if i < num_subplots:
            subset = scd_anomaly[scd_anomaly['lotnumber'] == lotnumber]
            ax = np.atleast_1d(axes)[i]  # Use np.atleast_1d() to handle single subplot case
            unique_anomalies = subset['anomaly'].unique()  # Get unique anomaly values
            palette = ['black', 'orange'][:len(unique_anomalies)]  # Adjust the palette based on the number of unique anomalies
            sns.scatterplot(data=subset, x='stitcharea', y='domecasegap', hue='anomaly', palette=palette, alpha=0.5, ax=ax)
            ax.set_title(f"Scatter Plot for Lot Number: {lotnumber}", color='blue', fontsize=16)
            ax.set_ylabel('Dome Case Gap')
            ax.set_xlabel('Stitch Area')
            plt.subplots_adjust(hspace=0.5)

    return fig
def plot_spc_trend(df, feature,  hi_limit=None, lo_limit=None, hi_value=None, lo_nalue=None):
    df = df.copy()  # Create a copy of the DataFrame to avoid modifying the original data
    figure_size=(14, 6)
    window_size=10
    sigma=2
    
    # Compute moving average and standard deviation
    df['MA'] = df[feature].rolling(window=window_size, min_periods=1).mean()
    df['STD'] = df[feature].rolling(window=window_size, min_periods=1).std()

    # Define SPC limits based on moving average and standard deviation
    df['SPC_Lower'] = df['MA'] - sigma * df['STD']
    df['SPC_Upper'] = df['MA'] + sigma * df['STD']

    # Plot the data and SPC limits
    plt.figure(figsize=figure_size)
    plt.plot(df.index, df[feature], 'b-', label=feature)
    plt.plot(df.index, df['MA'], 'r-', label=f'Moving Average ({window_size} days)')
    plt.plot(df.index, df['SPC_Lower'], 'g--', label=f'SPC Lower Limit ({sigma} sigma)')
    plt.plot(df.index, df['SPC_Upper'], 'g--', label=f'SPC Upper Limit ({sigma} sigma)')

    lo_spc = df['SPC_Lower'].mean()
    hi_spc = df['SPC_Upper'].mean()
    # Add upper limit lines based on hi_avg, lo_avg, hi_spc, and lo_spc values
    if hi_value is not None:
        plt.axhline(y=hi_value, color='orange', linestyle='--', label='Hi')
    if lo_value is not None:
        plt.axhline(y=lo_value, color='purple', linestyle='--', label='Lo')
    if hi_spc is not None:
        plt.axhline(y=hi_spc, color='red', linestyle='--', label='Hi SPC')
    if lo_spc is not None:
        plt.axhline(y=lo_spc, color='blue', linestyle='--', label='Lo SPC')
        
    #find spc-alarm  
    if lo_value < lo_spc and hi_value> hi_spc:
        trend = "Within SPC limits"
        alarm = f"0:{trend}"
    elif lo_value *1.1 >= lo_spc or  hi_value *0.9 <= hi_spc:
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


    plt.xlabel('Index')
    plt.ylabel(feature)
    plt.title(f'SPC Trend for {feature}')
    plt.legend()
    plt.show()
    
    return df
if __name__ == '__main__':

    parquet_path = "/home/naya/anomaly/files_parquet/scd_anomaly.parquet"
    scd_anomaly_check= check_anomalies(scd_anomaly, contamination=0.05, n_estimators=100);
    scd_anomaly_check.to_parquet('./files_parquet/scd_anomaly_check.parquet', engine='pyarrow')
    scd_only_anomaly = scd_anomaly[scd_anomaly_check['anomaly'] == -1]
    scd_only_anomaly.to_parquet('./files_parquet/scd_only_anomaly.parquet', engine='pyarrow')


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
            scd_only_anomaly_trend = plot_spc_trend(scd_only_anomaly, feature, hi_limit, lo_limit, hi_value, lo_value)

    display(scd_only_anomaly_trend)

    # Display the anomaly plot
    fig_anomaly = display_scd_anomaly(scd_anomaly)
    fig_anomaly.show()

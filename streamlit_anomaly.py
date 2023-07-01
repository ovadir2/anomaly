import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np
import pyarrow as pa
import pyarrow.hdfs as hdfs
import time
import datetime


def spc_trend(df, feature, hi_limit, lo_limit, hi_value, lo_value):
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

def plot_spc_trend(result_df, feature, hi_limit, lo_limit):
    plt.figure(figsize=(14, 6))
    plt.plot(result_df.index, result_df[feature], 'b-', label=feature)
    plt.plot(result_df.index, result_df['MA'], 'r-', label=f'Moving Average (10 days)')
    plt.plot(result_df.index, result_df['SPC_Lower'], 'g--', label='SPC Lower Limit (2 sigma)')
    plt.plot(result_df.index, result_df['SPC_Upper'], 'g--', label='SPC Upper Limit (2 sigma)')

    # Add upper limit lines based on hi_limit, lo_limit, hi_spc, and lo_spc values
    lo_spc = result_df['SPC_Lower'].mean()
    hi_spc = result_df['SPC_Upper'].mean()

    if hi_limit is not None:
        plt.axhline(y=hi_limit, color='orange', linestyle='--', label='Hi Limit')
    if lo_limit is not None:
        plt.axhline(y=lo_limit, color='purple', linestyle='--', label='Lo Limit')
    if hi_spc is not None:
        plt.axhline(y=hi_spc, color='red', linestyle='--', label='Hi SPC')
    if lo_spc is not None:
        plt.axhline(y=lo_spc, color='blue', linestyle='--', label='Lo SPC')

    plt.xlabel('Index')
    plt.ylabel(feature)
    plt.title(f'SPC Trend for {feature}')
    plt.legend()

def filter_data(df, week=None, batchid=None, tp_cell_name=None):
    filtered_df = df.copy()
    
    if week:
        filtered_df = filtered_df[filtered_df['week'] == week]
    if batchid:
        filtered_df = filtered_df[filtered_df['batchid'] == batchid]
    if tp_cell_name:
        filtered_df = filtered_df[filtered_df['tp_cell_name'] == tp_cell_name]
    
    return filtered_df

def highlight_alarm(row):
    alarm_value = int(row['alarm'].split(':')[0])
    if alarm_value != 0:
        return ['background-color: red'] * len(row.index)
    return [''] * len(row.index)

def display_scd_anomaly(scd_anomaly):
    lotnumbers = scd_anomaly['lotnumber'].unique()

    # Filter out lotnumbers with less than 800 items
    lotnumbers_filtered = [lotnumber for lotnumber in lotnumbers if len(scd_anomaly[scd_anomaly['lotnumber'] == lotnumber]) >= 800]

    # Define the columns for anomaly plotting
    columns_to_plot = ['stitcharea', 'domecasegap']

    num_subplots = len(columns_to_plot)
    
    # Create a figure and axes for subplots
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

def filter_data(df, week=None, batchid=None, tp_cell_name=None):
    filtered_df = df.copy()
    
    if week:
        filtered_df = filtered_df[filtered_df['week'] == week]
    if batchid:
        filtered_df = filtered_df[filtered_df['batchid'] == batchid]
    if tp_cell_name:
        filtered_df = filtered_df[filtered_df['tp_cell_name'] == tp_cell_name]
    
    return filtered_df

def main():
    st.title("SPC Trend Plot")

    # Get user input for the file paths
    spc_file_path = st.text_input("SPC File Path:", value='hdfs:///user/naya/anomaly/scd_only_anomaly_trend.json')
    scd_file_path = st.text_input("SCD File Path:", value='hdfs:///user/naya/anomaly/scd_anomaly_check.json')
    scd_refine_path = st.text_input("SCD Refine File Path:", value='hdfs:///user/naya/anomaly/scd_refine.json')

    # Load the DataFrames (initial load or reload if changes occur)
    spc_df = read_hdfs(spc_file_path)
    scd_df = read_hdfs(scd_file_path)
    scd_refine_df = read_hdfs(scd_refine_path)

    # Get the distinct values for operator selection
    week_values = scd_df['week'].unique().tolist()
    batchid_values = scd_df['batchid'].unique().tolist()
    tp_cell_name_values = scd_df['tp_cell_name'].unique().tolist()

    # Operator selection
    selected_week = st.selectbox("Select Week", week_values)
    selected_batchid = st.selectbox("Select Batch ID", batchid_values)
    selected_tp_cell_name = st.selectbox("Select TP Cell Name", tp_cell_name_values)

    # Check if all operator fields are selected
    if selected_week and selected_batchid and selected_tp_cell_name:
        # Filter the DataFrames based on operator selection
        filtered_spc_df = spc_df[(spc_df['week'] == selected_week) & (spc_df['batchid'] == selected_batchid) & (spc_df['tp_cell_name'] == selected_tp_cell_name)]
        filtered_scd_df = scd_df[(scd_df['week'] == selected_week) & (scd_df['batchid'] == selected_batchid) & (scd_df['tp_cell_name'] == selected_tp_cell_name)]
        filtered_scd_refine_df = scd_refine_df[(scd_refine_df['week'] == selected_week) & (scd_refine_df['batchid'] == selected_batchid) & (scd_refine_df['tp_cell_name'] == selected_tp_cell_name)]

        features = ["domecasegap", "stitcharea"]

        for feature in features:
            # Extract the limit values from the first row of scd_refine
            limit_values = filtered_scd_refine_df[f'{feature}_limit'].values[0]

            try:
                lo_limit, hi_limit = map(float, limit_values.split(':')) if limit_values else (float('-inf'), float('inf'))
            except ValueError:
                lo_limit, hi_limit = float('-inf'), float('inf')

            # Set the value range for the feature
            lo_value = filtered_scd_df[feature].min()
            hi_value = filtered_scd_df[feature].max()

            # Call the spc_trend function
            result_df = spc_trend(filtered_spc_df, feature, hi_limit, lo_limit, hi_value, lo_value)

            # Plot the SPC Trend only if the filtered DataFrame has data
            if not result_df.empty:
                # Plot the SPC Trend
                plot_spc_trend(result_df, feature, hi_limit, lo_limit)

                # Display the SPC Trend plot
                st.subheader(f"SPC Trend Plot - {feature}")
                st.pyplot(plt)

        # Call the display_scd_anomaly function only if the filtered DataFrame has data
        if not filtered_scd_df.empty:
            result_scd_anomaly_df = display_scd_anomaly(filtered_scd_df)

            # Display the SCD Anomaly plot
            st.subheader("SCD Anomaly Scatter Plot")
            st.pyplot(result_scd_anomaly_df)

    # Refresh the page every 10 seconds
    st.experimental_rerun_everything(seconds=10)

if __name__ == '__main__':
    main()

import streamlit as st
import pandas as pd
import seaborn as sns
import numpy as np
import pyarrow as pa
import pyarrow.hdfs as hdfs
import matplotlib.pyplot as plt
import mplcursors
import base64

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


def plot_spc_trend(df, feature, hi_limit=None, lo_limit=None, hi_value=None, lo_value=None):
    df = df.copy()  # Create a copy of the DataFrame to avoid modifying the original data
    window_size = 10
    sigma = 2

    # Compute moving average and standard deviation
    df['MA'] = df[feature].rolling(window=window_size, min_periods=1).mean()
    df['STD'] = df[feature].rolling(window=window_size, min_periods=1).std()

    # Define SPC limits based on moving average and standard deviation
    df['SPC_Lower'] = df['MA'] - sigma * df['STD']
    df['SPC_Upper'] = df['MA'] + sigma * df['STD']

    # Create a matplotlib figure
    fig, ax = plt.subplots(figsize=(min(len(df) / 100, 14), 6))  # Adjust the scaling factor and maximum width as needed

    # Plot the data and SPC limits
    ax.plot(df.index, df[feature], 'b-', label=feature)
    ax.plot(df.index, df['MA'], 'r-', label=f'Moving Average ({window_size} days)')
    ax.plot(df.index, df['SPC_Lower'], 'g--', label=f'SPC Lower Limit ({sigma} sigma)')
    ax.plot(df.index, df['SPC_Upper'], 'g--', label=f'SPC Upper Limit ({sigma} sigma)')

    lo_spc = df['SPC_Lower'].mean()
    hi_spc = df['SPC_Upper'].mean()

    # Add upper limit lines based on hi_avg, lo_avg, hi_spc, and lo_spc values
    if hi_value is not None:
        ax.axhline(y=hi_value, color='orange', linestyle='--', label='Hi')
    if lo_value is not None:
        ax.axhline(y=lo_value, color='purple', linestyle='--', label='Lo')
    if hi_spc is not None:
        ax.axhline(y=hi_spc, color='red', linestyle='--', label='Hi SPC')
    if lo_spc is not None:
        ax.axhline(y=lo_spc, color='blue', linestyle='--', label='Lo SPC')

    trend_counts = {
        "Within SPC limits": 0,
        "Approaching SPC limits": 0,
        "Below SPC limits": 0,
        "Above SPC limits": 0,
        "Above limits": 0,
        "Unknown": 0
    }

    for index, row in df.iterrows():
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

    df.at[index, 'alarm'] = alarm
    trend_counts[alarm.split(':')[1]] += 1

    # Render the figure
    st.pyplot(fig)

    # Aggregation visualization for 'trend' values
    trend_labels = list(trend_counts.keys())
    trend_values = list(trend_counts.values())

    # Create the bar chart
    fig, ax = plt.subplots(figsize=(3, 3))
    ax.bar(trend_labels, trend_values)

    # Set the font size of the x-axis and y-axis labels
    ax.set_xlabel('', fontsize=2)
    ax.set_ylabel('Occurrences', fontsize=8)

    # Set the font size of the title
    ax.set_title('Anamalies counting', fontsize=8)

    # Rotate x-axis labels for better visibility
    plt.xticks(rotation=45,fontsize=8 )

    # Display the chart using Streamlit
    st.pyplot(fig)

    return df


def display_one_scd_anomaly(scd_anomaly):
    fig, ax = plt.subplots(figsize=(8, 6))

    scatter = sns.scatterplot(data=scd_anomaly, x='stitcharea', y='domecasegap', hue='anomaly', palette=['black', 'orange'], alpha=0.5, ax=ax)
    ax.set_title("Scatter Plot for Selected Lot Number", color='blue', fontsize=16)
    ax.set_ylabel('Dome Case Gap')
    ax.set_xlabel('Stitch Area')

    cursor = mplcursors.cursor(scatter)

    @cursor.connect("add")
    def on_hover(sel):
        index = sel.target.index
        point = scd_anomaly.iloc[index]
        sel.annotation.set_text(str(point))

    st.pyplot(fig)

def display_scd_anomaly(scd_anomaly):
    lotnumbers = scd_anomaly['lotnumber'].unique()

    # Filter out lotnumbers with less than 800 items
    len_lot = len(scd_anomaly)
    st.text(f'Selected lot size is: {len_lot}')
    lotnumbers_filtered = [lotnumber for lotnumber in lotnumbers if len(scd_anomaly[scd_anomaly['lotnumber'] == lotnumber]) >= 800]

    # Limit the number of subplots to a maximum of 10
    num_subplots =  min(len(lotnumbers_filtered), 10)

    # Set the spacing between the subplots
    fig, axes = plt.subplots(num_subplots, 1, figsize=(8, num_subplots * 7))

    # Create separate scatter plots for each lotnumber
    for i, lotnumber in enumerate(lotnumbers_filtered):
       if i < num_subplots:
        subset = scd_anomaly[scd_anomaly['lotnumber'] == lotnumber]
        subset = scd_anomaly
        ax = np.atleast_1d(axes)[i] # use np.atleast_1d() to handle single subplot case
        unique_anomalies = subset['anomaly'].unique()  # Get unique anomaly values
        palette = ['black', 'orange'][:len(unique_anomalies)]  # Adjust the palette based on the number of unique anomalies
        sns.scatterplot(data=subset, x='stitcharea', y='domecasegap', hue='anomaly', palette=palette, alpha=0.5, ax=ax)
        ax.set_title("Scatter Plot for selected Lot Number", color='blue', fontsize=16)
        ax.set_ylabel('Dome Case Gap')
        ax.set_xlabel('Stitch Area')
        plt.subplots_adjust(hspace=0.5)

        # Render the figure using Streamlit
        st.pyplot(fig)

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

def triger_alarm_table(df_row):
    predicted_stitcharea = df_row['predicted_stitcharea_calculate']
    spc_lower_limit = df_row['spc_lower_limit']
    spc_upper_limit = df_row['spc_upper_limit']
    
    if len(scd_weeks_raws) > df_row['minimum_train_records_qty']:
        if len(scd_weeks_raws) < df_row['next_retraining_and_assigned']:
            # Compare the predicted stitch area to the SPC limits
                is_alarm = (predicted_stitcharea < spc_lower_limit) | (predicted_stitcharea > spc_upper_limit)
                if is_alarm.any():
                    msg =f"Alarm: predicted_stitcharea {predicted_stitcharea:03f} is out of SPC limits {spc_lower_limit:03f}:{spc_upper_limit:03f}"
                    df_row['alarm_pre_stitcharea'] = 1
                else:
                    msg =f"No Alarm: predicted_stitcharea {predicted_stitcharea:03f} is within SPC limits {spc_lower_limit:03f}:{spc_upper_limit:03f}"
                    df_row['alarm_pre_stitcharea'] = 0
                df_row['re-train_required'] = 0
        else:
            msg ="Need to re-train the data....."
            df_row['re-train_required'] = 1
    else:
        msg="Need to have more data for prediction model training...."
        df_row['additional_recorrds_needed'] = 1
        msg=('Need to have more data for prediction model training....')
    return(msg)
        
# Read the image file
with open('/home/naya/anomaly/architecture.PNG', 'rb') as file:
    image_data = file.read()

# Encode the image data to base64
encoded_image = base64.b64encode(image_data).decode('utf-8')
markdown_str = f"![DataFlow](data:image/png;base64,{encoded_image})"

# Define the long markdown text
long_markdown = """
##### Data engineer : Ovadia Ronen
## Final Project 
#### anomalies measurments trends detector    
###### Buisness case : critical to paitient, paramenters monitoring 

###### Determining Anomaly Level:

- Anomaly level will be determined by the business, ensure clear criteria are defined for what constitutes an anomaly.
- Consider incorporating domain expertise and historical data analysis to set appropriate thresholds for anomaly detection.
- Define the acceptable range or limits for each sensor parameter and identify when values approach or exceed those limits.

###### Tracking Values Close to Limits:

- Implement monitoring mechanisms to track sensor values that are approaching the predefined limits.
- Continuously compare the current sensor values to the limit thresholds and generate alerts or notifications when values are close to the limits.
- Include historical context and trends to identify if values are consistently moving towards the limits over time.

###### Prediction Model for Future trends:

- Train a prediction model using historical and aggregated data to forecast future values of sensor parameters.
- Use the prediction model to estimate if the future values may exceed the predefined limits.
- Incorporate the prediction results into the anomaly detection process to enhance proactive identification of potential anomalies.

###### Visualization and Alerting:

- Design the user interface (UI) to visualize sensor data, historical trends, and predicted values.
- Highlight data points and trends that are approaching or exceeding the predefined limits.
- Implement alerting mechanisms within the UI to notify users when anomalies or potential limit breaches are detected.

"""

## Create an expander widget
expander = st.expander("ReadMe..")

# Add the markdown content inside the expander
with expander:
    st.markdown(long_markdown)
    st.markdown('### Data flow')
    st.markdown(markdown_str, unsafe_allow_html=True)

    
    
# Streamlit app
st.markdown('### anomalies trends detector')

# Get user input for the file paths
spc_file_path = 'hdfs:///user/naya/anomaly/scd_only_anomaly_trend.json'
scd_file_path = 'hdfs:///user/naya/anomaly/scd_anomaly_check.json'
scd_refine_path = 'hdfs:///user/naya/anomaly/scd_refine.json'
triger_alarm_table_path = 'hdfs:///user/naya/anomaly/triger_alarm_table.json'
scd_weeks_raws_path = 'hdfs:///user/naya/anomaly/scd_weeks_raws.json'

# Load the DataFrames (initial load or reload if changes occur)
scd_only_anomaly = read_hdfs(spc_file_path)
scd_anomaly = read_hdfs(scd_file_path)
scd_refine = read_hdfs(scd_refine_path)
triger_alarm_tbl = read_hdfs(triger_alarm_table_path)
scd_weeks_raws = read_hdfs(scd_weeks_raws_path)

features = ["domecasegap", "stitcharea"]
#scd_refine_first_row = scd_refine.iloc[0]  # Get the first row of scd_refine
# Display the anomaly trends 
for feature in features:
    # Set the value range for the feature
    lo_value = scd_only_anomaly[feature].min()
    hi_value = scd_only_anomaly[feature].max()

 
    # Calculate the mean values for lo_limit and hi_limit
    lo_limit_mean = scd_refine[f'{feature}_limit'].apply(lambda x: float(x.split(':')[0].strip()) if x.split(':')[0].strip() else 0).mean()
    hi_limit_mean = scd_refine[f'{feature}_limit'].apply(lambda x: float(x.split(':')[1].strip()) if x.split(':')[1].strip() else 0).mean()  
 
    # Display the trend plot using mean values as limits and the 'lotnumber' column as the index
    scd_only_anomaly_trend = plot_spc_trend(scd_only_anomaly, feature, hi_limit_mean, lo_limit_mean, hi_value, lo_value)
# Display the anomaly plot
default_tp_cell_name = [11.0, 12.0]
selected_lotnumber = st.selectbox('Show anomalies for the selected production Lot ID', [lotnumber for lotnumber in scd_anomaly['lotnumber'].unique() if len(scd_anomaly[scd_anomaly['lotnumber'] == lotnumber]) >= 800])
selected_tp_cell_name = st.multiselect('Select Sealing Cell weling ID', default_tp_cell_name)

if not selected_tp_cell_name:
    st.warning('Please select at least one TP Cell Name.')
else:
     # Filter the DataFrame based on the selected lot number and TP Cell Name
    subset_scd_anomaly = scd_anomaly[(scd_anomaly['lotnumber'] == selected_lotnumber) & (scd_anomaly['tp_cell_name'].isin(selected_tp_cell_name))]
    # Display the scatter plot for the selected lot number and TP Cell Name
    display_one_scd_anomaly(subset_scd_anomaly)

    # Show the plot
    plt.show()
    
if len(triger_alarm_tbl) != 0:
    last_row = triger_alarm_tbl.iloc[len(triger_alarm_tbl)-1]
    st.markdown(f"#### {int(last_row['pred_year']):0d}, {int(last_row['pred_week']):0d} anomaly trend....")
    st.markdown(' ###### SealingCell welding:  ')
    alarm_desc= triger_alarm_table(last_row)
    if last_row['alarm_pre_stitcharea'] !=0:
        alarm_color = 'red'
    else:
        alarm_color = 'green'
else:
    alarm_color = 'yellow'

st.markdown(f'<p style="color:{alarm_color}">{alarm_desc}</p>', unsafe_allow_html=True)

#add prediction performance




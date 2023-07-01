import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from consumer_anomaly_analysis import spc_trend

# Define a custom event handler that reloads the DataFrame when changes occur
class FileChangeHandler(FileSystemEventHandler):
    def __init__(self, file_path, cache):
        super().__init__()
        self.file_path = file_path
        self.cache = cache

    def on_modified(self, event):
        if event.src_path == self.file_path:
            # Clear the cache
            self.cache.clear()

def highlight_alarm(row):
    alarm_value = int(row['alarm'].split(':')[0])
    if alarm_value != 0:
        return ['background-color: red'] * len(row)
    else:
        return ['background-color: green'] * len(row)

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

def main():
    st.title("SPC Trend Plot")

    # Get user input for the file paths
    spc_file_path = st.text_input("Enter the SPC file path:")
    scd_file_path = st.text_input("Enter the SCD file path:")

    # Create a cache for the DataFrames
    @st.cache(allow_output_mutation=True)
    def load_dataframe(file_path):
        return pd.read_hdf(file_path)

    # Load the DataFrames (initial load or reload if changes occur)
    spc_df = load_dataframe(spc_file_path)
    scd_df = load_dataframe(scd_file_path)

    # Create an event handler to monitor file changes
    event_handler = FileChangeHandler(spc_file_path, load_dataframe)

    # Create an observer and start monitoring the file
    observer = Observer()
    observer.schedule(event_handler, path=spc_file_path.rsplit('/', 1)[0])
    observer.start()

    # Display the SPC DataFrame
    st.subheader("SPC DataFrame")
    st.dataframe(spc_df.style.apply(highlight_alarm, axis=1))

    # Get user inputs
    feature = st.selectbox("Select a feature:", spc_df.columns)
    hi_limit = st.number_input("Enter the high limit:", value=None)
    lo_limit = st.number_input("Enter the low limit:", value=None)
    hi_value = st.number_input("Enter the high value:", value=None)
    lo_value = st.number_input("Enter the low value:", value=None)

    # Call the spc_trend function
    result_df = spc_trend(spc_df, feature, hi_limit, lo_limit, hi_value, lo_value)

    # Plot the SPC Trend
    plt.figure(figsize=(14, 6))
    plt.plot(result_df.index, result_df[feature], 'b-', label=feature)
    plt.plot(result_df.index, result_df['MA'], 'r-', label=f'Moving Average (10 days)')
    plt.plot(result_df.index, result_df['SPC_Lower'], 'g--', label='SPC Lower Limit (2 sigma)')
    plt.plot(result_df.index, result_df['SPC_Upper'], 'g--', label='SPC Upper Limit (2 sigma)')

    # Add upper limit lines based on hi_value, lo_value, hi_spc, and lo_spc values
    lo_spc = result_df['SPC_Lower'].mean()
    hi_spc = result_df['SPC_Upper'].mean()
    if hi_value is not None:
        plt.axhline(y=hi_value, color='orange', linestyle='--', label='Hi')
    if lo_value is not None:
        plt.axhline(y=lo_value, color='purple', linestyle='--', label='Lo')
    if hi_spc is not None:
        plt.axhline(y=hi_spc, color='red', linestyle='--', label='Hi SPC')
    if lo_spc is not None:
        plt.axhline(y=lo_spc, color='blue', linestyle='--', label='Lo SPC')

    plt.xlabel('Index')
    plt.ylabel(feature)
    plt.title(f'SPC Trend for {feature}')
    plt.legend()

    # Display the SPC Trend plot
    st.subheader("SPC Trend Plot")
    st.pyplot(plt)

    # Call the display_scd_anomaly function
    scd_anomaly_fig = display_scd_anomaly(scd_df)

    # Display the SCD Anomaly plot
    st.subheader("SCD Anomaly Scatter Plot")
    st.pyplot(scd_anomaly_fig)

if __name__ == '__main__':
    main()

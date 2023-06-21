import glob
import seaborn as sns
import streamlit as st
import requests
import pandas as pd
import json
import pickle
from sklearn.ensemble import IsolationForest
import matplotlib.pyplot as plt
import warnings


@st.cache_data()
def fetch_data_from_api(url, path):
    try:
        # Make the GET request to the server
        _filter = request.args.get('year'=2023, 'quarter'=None, 'month'=6, 'yearweek'=None, 'weekday'=None, 'configs_id'=917)  #
        response = requests.post(url, params={' _filter':  _filter})
        st.write(response)
        response.raise_for_status()  # Raise an exception for non-2xx response codes
        # Get the response data (JSON)
        result = response.json()
        return result

    except requests.exceptions.RequestException as e:
        print('Error sending request:', str(e))
        st.write('Error sending request:', str(e))
        return(str(e))

def predict_anomalies(data, feature,contamination_val):
    try:
        # Convert the result dictionary to a pandas DataFrame
        df = pd.DataFrame.from_dict(data['lotnumber'])
        # Assign feature names to the DataFrame columns
        df.columns = ['blisterid', 'domecasegap', 'stitcharea', 'testtimemin','testtimesec','material']
        # Create a subset of the data with the selected features
        X = df[feature]
 
        # Handle missing values by replacing them with a specific value (e.g., -999)
        X = X.fillna(-999)

        # Create the Isolation Forest model with the selected contamination value
        isolation_forest = IsolationForest(contamination=contamination_val)

        # Display the selected contamination value
        st.write("Selected Contamination Value:", f"{contamination_val:.3f}")

        # Fit the model to the data
        isolation_forest.fit(X)
        # Predict the anomalies in the data
        df['anomaly'] = isolation_forest.predict(X)
        return df
    except (ValueError, TypeError, AttributeError) as e:
        st.write(str(e))
        return(str(e))

def display_anomaly_evaluation(df,feature):
    try:
 
        if (feature == 'domecasegap') :
            ratio_ = len( df[df['anomaly'] == -1])  / len(df[df['anomaly'] != -1]) 
            amount = len( df[df['anomaly'] == -1])  
            mean_gap = df[df['anomaly'] == -1]['domecasegap'].mean() 
            mean_gap_good = df[df['anomaly'] != -1]['domecasegap'].mean() 
            # Create a two-column layout
            col1, col2, col3 = st.columns(3)
            # Display the metrics in each column
            with col1:
                st.metric(label="Gap Amount (Anomal/Nomal)", value=f'{ratio_:.2%}', delta_color='inverse')
                st.write('<style>div[data-baseweb="metric"] {font-size: small;}</style>', unsafe_allow_html=True)
            with col2:
                st.metric(label="Gap Amount ", value=f'{amount}', delta_color='inverse')
                st.write('<style>div[data-baseweb="metric"] {font-size: small;}</style>', unsafe_allow_html=True)
            with col3:
                st.metric(label="Mean Gap(Anomal/Nomal)", value=f'{mean_gap:.2f}', delta=f'{mean_gap / mean_gap_good:.2%}')
                st.write('<style>div[data-baseweb="metric"] {font-size: small;}</style>', unsafe_allow_html=True)


        if (feature == 'testtimesec') :
            ratio_ = len( df[df['anomaly'] == -1])  / len(df[df['anomaly'] != -1]) 
            amount = len( df[df['anomaly'] == -1])  
            mean_testtimesec = df[df['anomaly'] == -1]['testtimesec'].mean() 
            mean_testtimesec_good = df[df['anomaly'] != -1]['testtimesec'].mean() 
           # Create a two-column layout
            col1, col2, col3 = st.columns(3)
            # Display the metrics in each column
            with col1:
                st.metric(label="Mean TestTimeSec ", value= f'{amount:.0f}' , delta_color='inverse')
            with col3:
                st.metric(label="Mean TestTimeSec(Anomal/Nomal)", value=f'{mean_testtimesec:.0f}', delta=f'{mean_testtimesec / mean_testtimesec_good:.2%}')
 
        if (feature == 'stitcharea') :
            ratio_ = len( df[df['anomaly'] == -1])  / len(df[df['anomaly'] != -1]) 
            amount = len( df[df['anomaly'] == -1])  
            mean_stitch = df[df['anomaly'] == -1]['stitcharea'].mean() 
            mean_stitch_good = df[df['anomaly'] != -1]['stitcharea'].mean() 
            # Create a two-column layout
            col1, col2,col3 = st.columns(3)
            # Display the metrics in each column
            # with col1:
            #     st.metric(label='StitchArea Amount (Anomal/Nomal)', value=f'{ratio_:.2%}', delta_color='inverse')
            # with col2:
            #     st.metric(label="StitchArea Amount", value=f'{amount:.0f}', delta_color='inverse')
            with col3:
                st.metric(label="Mean StitchArea(Anomal/Nomal)", value=f'{mean_stitch:.2f}', delta=f'{mean_stitch / mean_stitch_good:.2%}')
    except (ValueError, TypeError, AttributeError) as e:
        st.write(str(e))
        return(str(e))

             
def display_splot_anomaly(df):
    df2 = pd.concat([df[df.anomaly == -1], df[df.anomaly != -1]])
    g = sns.pairplot(df2, hue='anomaly')
    st.pyplot(g.fig)

def main():
    try:
        # Set the app title
        st.title("Welding Anomaly Detection")

        # Set the server URL
        server_url = "http://localhost:8000/SealingCell_data"

        # Display the file selection section
        st.subheader("File Selection")

        # Generate a unique identifier for the contamination slider
        slider_key = "contamination_slider"

         # Set the initial value for contamination
        default_contamination = 0.015

        # Create a slider widget to adjust the contamination value
        contamination_val = st.slider(
            "Contamination",
            min_value=0.001,
            max_value=0.02,
            value=default_contamination,
            step=0.002,
            key=slider_key,
            format="%.3f"
        )
         # Convert the contamination value to a float
        contamination_val = float(contamination_val)

        # Select the directory
        directory = st.text_input("Directory", r"C:\Python3102\Projects_Sources")

        # Set the file pattern
        pattern = "SealingCell_*.csv"           ############@@@@@@@@@@@@@@@@@@@@@@@@@
        #pattern = "SealingCell_59229U.csv"

        # Get the list of matching files
        files = glob.glob(f"{directory}/{pattern}")

        # Display the file selection dropdown
        selected_file = st.selectbox("Select a File", files)
        if selected_file:
            if st.button("Fetch Data"):
                # Fetch data from the API
                st.write(server_url,selected_file)
                data = fetch_data_from_api(server_url, selected_file)
                features = ["domecasegap","stitcharea","testtimesec"]
                if data:
                    for feature in features:                    
                        # Perform anomaly detection
                        df = predict_anomalies(data, features,contamination_val)

                        # Display evaluation and pairplot
                        display_anomaly_evaluation(df,feature)
                    display_splot_anomaly(df)
                else:
                    st.write('No sensor data')
   
    except (ValueError, TypeError, AttributeError) as e:
        print('ValueError:', str(e))

if __name__ == '__main__':
    main()

# import glob
# import seaborn as sns
# import streamlit as st
# import requests
# import pandas as pd
# import json
# import pickle
# from sklearn.ensemble import IsolationForest
# import matplotlib.pyplot as plt
# import warnings
# from datetime import datetime
# import calendar


# @st.cache_data()
# def fetch_data_from_api(url, path, selected_date):
#     try:
#         # Convert the selected date to a string
#         date_str = selected_date.strftime("%Y-%m-%d")

#         # Make the GET request to the server with the selected date parameter
#         response = requests.post(url, params={'path': path, 'date': date_str})

#         # Rest of the function code...


# def main():
#     # Set the app title
#     st.title("Welding Anomaly Detection")

#     # Set the server URL
#     server_url = "http://localhost:8000/SealingCell_data"

#     # Set the initial value for contamination
#     default_contamination = 0.015

#     # Create a sidebar section for measures and controls
#     st.sidebar.title("Measures and Controls")

#     # Select the directory
#     directory = st.sidebar.text_input("Directory", r"C:\Python3102\Projects_Sources")

#     # Set the file pattern
#     pattern = "SealingCell_*.csv"

#     # Get the list of matching files
#     files = glob.glob(f"{directory}/{pattern}")

#     # Display the file selection dropdown in the sidebar
#     selected_file = st.sidebar.selectbox("Select a File", files)

#     # Calendar Date Input in the sidebar
#     selected_date = st.sidebar.date_input("Select a Date", value=datetime.now())

#     # Generate a unique identifier for the contamination slider
#     slider_key = "contamination_slider"

#     # Create a slider widget to adjust the contamination value in the sidebar
#     contamination_val = st.sidebar.slider(
#         "Contamination",
#         min_value=0.001,
#         max_value=0.02,
#         value=default_contamination,
#         step=0.002,
#         key=slider_key,
#         format="%.3f"
#     )

#     # Convert the contamination value to a float
#     contamination_val = float(contamination_val)

#     if selected_file and selected_date:
#         if st.sidebar.button("Fetch Data"):
#             # Fetch data from the API
#             st.write(server_url, selected_file)
#             data = fetch_data_from_api(server_url, selected_file, selected_date)
#             features = ["domecasegap", "stitcharea", "testtimesec"]
#             if data:
#                 for feature in features:
#                     # Perform anomaly detection
#                     df = predict_anomalies(data, feature, contamination_val)

#                     # Display evaluation in the sidebar
#                     display_anomaly_evaluation(df, feature)

#                 # Display the plot on the right
#                 st.subheader("Anomaly Plot")
#                 display_splot_anomaly(df)
#             else:
#                 st.write('No sensor data')


# if __name__ == '__main__':
#     main()

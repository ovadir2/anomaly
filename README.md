##### Programmer : Ovadia Ronen
# Final Project 
## weekly anomaly prediction along with daily amomaly detector 
### buisness case : monitor on a critical paramenter, that may hazard the patient 
#### this module will be part of the monitoring system

#### Determining Anomaly Level:

- Anomaly level will be determined by the business, ensure clear criteria are defined for what constitutes an anomaly.
- Consider incorporating domain expertise and historical data analysis to set appropriate thresholds for anomaly detection.
- Define the acceptable range or limits for each sensor parameter and identify when values approach or exceed those limits.

#### Tracking Values Close to Limits:

- Implement monitoring mechanisms to track sensor values that are approaching the predefined limits.
- Continuously compare the current sensor values to the limit thresholds and generate alerts or notifications when values are close to the limits.
- Include historical context and trends to identify if values are consistently moving towards the limits over time.

#### Prediction Model for Future Values:

- Train a prediction model using historical and aggregated data to forecast future values of sensor parameters.
- Use the prediction model to estimate if the future values may exceed the predefined limits.
- Incorporate the prediction results into the anomaly detection process to enhance proactive identification of potential anomalies.

#### Visualization and Alerting:

- Design the user interface (UI) to visualize sensor data, historical trends, and predicted values.
- Highlight data points and trends that are approaching or exceeding the predefined limits.
- Implement alerting mechanisms within the UI to notify users when anomalies or potential limit breaches are detected.

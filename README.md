# Snowpark_VaR_Tensor_Demo

Value at Risk (VaR) Calculation using TensorFlow and Snowpark
This project demonstrates how to calculate Value at Risk (VaR) for financial assets using Monte Carlo simulations powered by TensorFlow, and integrates with Snowflake's Snowpark for data storage and processing.

Overview
The project:

Uses Monte Carlo simulations to model stock price movements based on historical data.
Utilizes TensorFlow for efficient simulation of Geometric Brownian Motion.
Integrates with Snowflake via Snowpark to save and manage data in the cloud.

Installation
Prerequisites
Ensure you have the following installed:

Python 3.7+
TensorFlow 2.x
Snowflake Connector for Python (for Snowpark)
yfinance for fetching stock data
Install the required packages with:


```Python
pip install tensorflow yfinance
````
![image](https://github.com/user-attachments/assets/4abfec41-073c-4fc3-aae6-5f68ecd77f5e)

![image](https://github.com/user-attachments/assets/8facffce-ffff-4985-b47c-41f02d55b8d0)

![image](https://github.com/user-attachments/assets/f3e53415-2c38-4932-b06b-a0ee2cf4fe38)

Snowflake Setup
Make sure you have a Snowflake account and set up your credentials for Snowpark access.
In this example, Snowpark fetches an active session (get_active_session()). You should have your Snowflake environment configured to handle this, or replace it with your session creation logic if needed.
![image](https://github.com/user-attachments/assets/39a8d3a4-0ccb-4dd6-9c3d-7bb71e183f18)

Step : to create the function to Fetch AAPL stock data using yfinance
![image](https://github.com/user-attachments/assets/51472a07-6e99-4927-b34b-67c5d62d9af3)

Step : to create the function for Monte Carlo simulation using TensorFlow (Geometric Brownian Motion)
![image](https://github.com/user-attachments/assets/2bf08cfb-70db-4b46-8b73-751fb1d73016)

Step : VaR Calculation using Monte Carlo Simulations
![image](https://github.com/user-attachments/assets/977e2d79-7803-44b2-b0b2-6a71c6ebb7fe)

Step : Main function to fetch data, run VaR, and save to Snowflake
![image](https://github.com/user-attachments/assets/941bdd32-ae26-4976-861f-98a0bfe24899)

Step : Main function to fetch data, run VaR, and save to Snowflake
![image](https://github.com/user-attachments/assets/be5a5430-cf09-407c-a4ba-3cd718749992)

Step : Run the VaR calculation
![image](https://github.com/user-attachments/assets/3ebe6c14-3e37-44c2-8866-ad251cdb5fe0)

Step : Close the session
![image](https://github.com/user-attachments/assets/254daf5b-8b4a-4f3a-a3ca-56de534b1d65)

The script calculates VaR for AAPL stock over a 5-year period using Monte Carlo simulations with TensorFlow.

Steps:

Fetch Stock Data: Uses yfinance to download 5 years of historical data for AAPL.
Run Monte Carlo Simulation: Simulates stock price movements using Geometric Brownian Motion (GBM) in TensorFlow.
Calculate VaR: Computes VaR at a 95% confidence level based on the simulated price paths.
Save Results: Saves the data and VaR calculation into a Snowflake table using Snowpark.

Monte Carlo Simulation
Monte Carlo simulation is used to model the randomness of stock prices using the Geometric Brownian Motion (GBM) formula. TensorFlow generates multiple simulated price paths based on the stock’s historical volatility (sigma) and expected return (mu).

Key steps:

The simulation runs over 10,000 iterations (you can adjust n_simulations).
The results help determine the potential loss (VaR) at a 95% confidence level.
Snowpark Integration
This project leverages Snowpark to:

Load historical stock data fetched via yfinance.
Store the final VaR results into a Snowflake table.
At the end of the process, the results are saved to a Snowflake table as follows:

```python
snowpark_df.write.mode("overwrite").save_as_table("AAPL_VAR_RESULTS")
```
Make sure your Snowflake environment has the necessary permissions to write data.

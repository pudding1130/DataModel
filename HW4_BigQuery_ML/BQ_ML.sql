-- Q1
SELECT trip_distance, pickup_datetime, dropoff_datetime, passenger_count, tolls_amount, fare_amount
FROM `bigquery-public-data.new_york.tlc_yellow_trips_2015`
LIMIT 1000;

-- Q2
-- No need to filter missing value since no NULLs in key columns
SELECT COUNT(*) AS any_na
FROM `bigquery-public-data.new_york.tlc_yellow_trips_2015`
WHERE trip_distance IS NULL
OR pickup_datetime IS NULL
OR dropoff_datetime IS NULL
OR passenger_count IS NULL
OR tolls_amount IS NULL
OR fare_amount IS NULL;

-- Q3
-- randomly sample 0.1% of the data and transform columns for feature engineering 
-- create row_number for spliting data
CREATE OR REPLACE TABLE `s113356002hw.NYT.transformed_data` AS
SELECT 
  ROW_NUMBER() OVER (ORDER BY pickup_datetime) AS row_number,
  (tolls_amount+fare_amount) AS total_fare,
  TIMESTAMP_DIFF(dropoff_datetime, pickup_datetime, MINUTE) AS trip_duration,
  EXTRACT(HOUR FROM pickup_datetime) AS pickup_hour,
  EXTRACT(DAYOFWEEK FROM pickup_datetime) AS pickup_day_of_week
FROM `bigquery-public-data.new_york.tlc_yellow_trips_2015`;

-- Q4
-- split the data into 80% for training and 20% for testing
CREATE OR REPLACE TABLE  `s113356002hw.NYT.train` AS
SELECT *
FROM `s113356002hw.NYT.transformed_data`
WHERE MOD(row_number, 10) <8;

CREATE OR REPLACE TABLE  `s113356002hw.NYT.test` AS
SELECT *
FROM `s113356002hw.NYT.transformed_data`
WHERE MOD(row_number, 10) >=8;

-- create linear regression model
CREATE OR REPLACE MODEL `s113356002hw.NYT.taxi_fare_model`
OPTIONS(
  model_type = 'linear_reg',
  input_label_cols = ['total_fare']
) AS
SELECT * FROM `s113356002hw.NYT.train`;

-- Q5
-- evalute model with test data by MAE & RMSE
SELECT mean_absolute_error AS MAE, sqrt(mean_squared_error) AS RMSE
FROM ML.EVALUATE(
  MODEL `s113356002hw.NYT.taxi_fare_model`,
  (
    SELECT * FROM `s113356002hw.NYT.test`
  )
);

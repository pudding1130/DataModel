CREATE OR REPLACE TABLE `data-model-final.airport.ATL_LAX` AS
WITH DEP_ATL_LAX AS (
  SELECT *
  FROM `data-model-final.airport.ATL_dep`
  WHERE Destination = 'LAX'
  )
,ARR_ATL_LAX AS (
  SELECT * 
  FROM `data-model-final.airport.LAX`
  WHERE Origin = 'ATL'
)
SELECT A.Carrier_code, A.Date, B.Origin, A. Destination, A.Flight_no, A.Delay_mins AS Delay_mins_departure,
     A.Taxi_in_mins, A.Delay_Carrier_mins, A.Delay_Weather_mins, 
     A.Delay_National_Aviation_mins, A.Delay_Security, A.Delay_Late_Aircraft_Arrival, B.Delay_mins AS Delay_mins_arrival
FROM DEP_ATL_LAX AS A
LEFT JOIN ARR_ATL_LAX AS B
ON A.Flight_no = B.Flight_no AND A.Date = B.Date;

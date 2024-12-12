---Q1

CREATE OR REPLACE TABLE 
    `skilful-asset-435412-g9.bq_assignment.ga_sessions_copy` AS
SELECT fullVisitorId, visitId, visitNumber, visitStartTime, date,
        totals.visits as visits, totals.hits as hits, totals.pageviews as pageviews,
        trafficSource.medium as medium, device.deviceCategory as device_category,
        geoNetwork.country as country
FROM `bigquery-public-data.google_analytics_sample.ga_sessions_20170801` LIMIT 100




---Q2

CREATE OR REPLACE MATERIALIZED VIEW `skilful-asset-435412-g9.bq_assignment.mv_country_sessions` AS 
SELECT country,
  count(*) AS session_count
FROM `skilful-asset-435412-g9.bq_assignment.ga_sessions_copy`
GROUP BY country;

SELECT * FROM `skilful-asset-435412-g9.bq_assignment.mv_country_sessions`
WHERE session_count > 10
ORDER BY session_count DESC

---Q3

SELECT fullVisitorId, country, 
  CASE WHEN country = 'United States' 
    THEN NULL ELSE pageviews END AS pageviews,
  CASE WHEN country = 'United States' 
    THEN NULL ELSE hits END AS hits
FROM `skilful-asset-435412-g9.bq_assignment.ga_sessions_copy` 
WHERE country = 'United States'
LIMIT 10;

SELECT fullVisitorId,
  CONCAT('***',SUBSTR(fullVisitorID, 4, LENGTH(fullVisitorID)-6 ),'***') AS masked_fullVisitorID,
   visitid, date
FROM `skilful-asset-435412-g9.bq_assignment.ga_sessions_copy`
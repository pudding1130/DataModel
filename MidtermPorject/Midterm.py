from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account
from itertools import combinations
import numpy as np
import pandas as pd
import json, csv, math



project_id = 'final-113356002'
credentials = service_account.Credentials.from_service_account_file(f'C:\\Users\weihs\桌面\\WEIHSUAN\\2024\碩班\\資料模式\\final-113356002-280ded50b43d.json')
bigquery_client = bigquery.Client(project = project_id, credentials = credentials)

#%% data-preprocess - upload data of BQ
#upload external data 
storage_client = storage.Client(project = project_id, credentials = credentials)
bucket_name = 'bucket_final_113356002'
bucket = storage_client.bucket(bucket_name)
new_bucket = storage_client.create_bucket(bucket, location='US')
new_bucket.storage_class = 'STANDARD'
new_bucket.patch()
dataset = bigquery_client.dataset('preprocess')
bigquery_client.create_dataset(dataset)

##1-1. customer_logs
table_name = 'customer_logs'
blob = bucket.blob(f'{table_name}.csv')
blob.upload_from_filename(f'C:\\Users\\weihs\\桌面\\WEIHSUAN\\2024\\碩班\資料模式\\{table_name}.csv')
#df = pd.read_csv(f'C:\\Users\\weihs\\桌面\\WEIHSUAN\\2024\\碩班\資料模式\\{table_name}.csv')
table_ref = bigquery.TableReference(dataset_ref, table_name)
schema = [
            bigquery.SchemaField('log_id', 'STRING'),
            bigquery.SchemaField('timestamp', 'TIMESTAMP'),
            bigquery.SchemaField('ad_id', 'STRING'),
            bigquery.SchemaField('user_action', 'STRING'),
            bigquery.SchemaField('country', 'STRING'),
]
table = bigquery.Table(table_ref, schema = schema)
###set up config and make sure the uris directing the to csv url
external_config = bigquery.ExternalConfig('CSV')
external_config.source_uris = f'gs://{bucket_name}/{table_name}.csv'
external_config.options.skip_leading_rows = 1
external_config.options.field_delimiter = ","
table.external_data_configuration = external_config
###create table
bigquery_client.create_table(table)

##1-2. ad_performance

###經過查詢發現BQ不支援NESTED結構，改先轉成JSON再匯入

table_name = 'ad_performance'
csv_file = f'C:\\Users\\weihs\\桌面\\WEIHSUAN\\2024\\碩班\資料模式\\{table_name}.csv'
json_file = f'C:\\Users\\weihs\\桌面\\WEIHSUAN\\2024\\碩班\資料模式\\{table_name}.json'

with open(csv_file, mode='r', encoding='utf-8') as infile, open(json_file, mode='w', encoding='utf-8') as outfile:
    csv_reader = csv.DictReader(infile)  # 讀取csv檔
    for row in csv_reader:
        # 逐行轉為JSON格式
        ad_performance = {
            "region_id": row["region_id"],
            "impressions": int(row["impressions"]),
            "clicks": int(row["clicks"]),
            "conversions": int(row["conversions"]),
            "country": row["country"]
        }
        
        json_data = {
            "ad_id": row["ad_id"],
            "ad_performance": ad_performance
        }
        
        # 逐行寫入JSON
        outfile.write(json.dumps(json_data) + "\n")


blob = bucket.blob(f'{table_name}.json')
blob.upload_from_filename(f'C:\\Users\\weihs\\桌面\\WEIHSUAN\\2024\\碩班\資料模式\\{table_name}.json')


table_ref = bigquery.TableReference(dataset_ref, table_name)
schema = [  bigquery.SchemaField('ad_id', 'STRING'),
            bigquery.SchemaField('ad_performance', 'RECORD', fields = [
                bigquery.SchemaField('region_id', 'STRING'),
                bigquery.SchemaField('impressions', 'INTEGER'),    
                bigquery.SchemaField('clicks', 'INTEGER'),
                bigquery.SchemaField('conversions', 'INTEGER'),
                bigquery.SchemaField('country', 'STRING')   
            ]
        ),
]

table = bigquery.Table(table_ref, schema = schema)
###set up config and make sure the uris directing the to csv url
external_config = bigquery.ExternalConfig('NEWLINE_DELIMITED_JSON')
external_config.source_uris = f'gs://{bucket_name}/{table_name}.json'
table.external_data_configuration = external_config
###create table
bigquery_client.create_table(table)



#%% data-preprocess - create united table
## 發現score中有超過80%的score是NULL
## 常見的方法為直接使用眾數or平均數進行填補，但因score缺失值比例高，且該欄位的NULL可能含有重要資訊
## 若貿然補值可能會遺失重要特徵，甚至增加模型的雜訊；但又因查詢BigQuery文件，發現如果不處理NA，則BQ的LINEAR_REG會直接drop該欄位
## 最終選擇使用transform的方式新增一個欄位score_NA，並將score為NULL的欄位用平均數進行填補

# CREATE TABLE g_trends
query1 = '''
CREATE OR REPLACE TABLE `final-113356002.preprocess.g_trends` AS
SELECT  A.country_code, A.country_name, A.term, A.week,
   CASE WHEN A.score IS NULL THEN ROUND((B.mean-B.min)/(B.max-B.min), 3) ELSE ROUND((A.score-B.min)/(B.max-B.min), 3) END AS score,
   CASE WHEN A.score IS NULL THEN 1 ELSE 0 END AS score_null
FROM bigquery-public-data.google_trends.international_top_rising_terms AS A,
     (SELECT ROUND(AVG(score), 1) AS mean, MAX(score) AS max, 
     MIN(score) AS min FROM bigquery-public-data.google_trends.international_top_rising_terms) AS B
WHERE RAND() <= 0.001;'''

bigquery_client.query(query1)

# CREATE TABLE ga

query2 ='''CREATE OR REPLACE TABLE `final-113356002.preprocess.ga` AS
SELECT geoNetwork.country AS country, sum(totals.pageviews) AS pageviews, 
   CASE WHEN sum(totals.transactions) IS NULL THEN 0 ELSE sum(totals.transactions) END AS transactions
FROM bigquery-public-data.google_analytics_sample.ga_sessions_20170801
GROUP BY geoNetwork.country;'''

bigquery_client.query(query2)

# CREATE TABLE ad_geo as united table of geotargets and geomapping

query3 = '''
CREATE OR REPLACE TABLE `final-113356002.preprocess.ad_geo` AS
SELECT A.country_code, B.target_country_region AS country_name,
    COUNT(DISTINCT B.target_city) AS target_city_num,
    COUNT(DISTINCT B.target_region) AS target_region_num
FROM CTE AS bigquery-public-data.google_ads.geotargets
RIGHT JOIN bigquery-public-data.google_ads_geo_mapping_us.ads_geo_criteria_mapping AS B
ON CAST(A.criteria_id AS INT64) = CAST(B.ads_criteria_id AS INT64)
GROUP BY B.target_country_region, A.country_code;
'''

bigquery_client.query(query3)

# CREATE TABLE ad_info

#original version
""" query4 = '''CREATE OR REPLACE TABLE `final-113356002.preprocess.ad_info_original` AS 
WITH customer_cte AS (
   SELECT count(*) AS action_num, ad_id, user_action, country 
   FROM `final-113356002.preprocess.customer_logs`
   GROUP BY  ad_id, user_action, country
)
SELECT A.*, (A.ad_performance.clicks / A.ad_performance.impressions) AS CTR, 
       B.country, B.action_num, B.user_action
FROM `final-113356002.preprocess.ad_performance` AS A
JOIN customer_cte AS B
ON A.ad_performance.country = B.country AND A.ad_id = B.ad_id;'''

bigquery_client.query(query4)
 """
#updated version for more sophisticated information by country
query4 = '''CREATE OR REPLACE TABLE `final-113356002.preprocess.ad_info` AS 
WITH customer_cte AS (
   SELECT count(*) AS action_num, ad_id, user_action, country 
   FROM `final-113356002.preprocess.customer_logs`
   GROUP BY ad_id, user_action, country
), customer_cte2 AS(
   SELECT ad_id, country,
   SUM(CASE WHEN user_action = 'click' THEN action_num ELSE 0 END) AS click_num,
   SUM(CASE WHEN user_action = 'view' THEN action_num ELSE 0 END) AS view_num,
   SUM(CASE WHEN user_action = 'purchase' THEN action_num ELSE 0 END) AS purchase_num
   FROM customer_cte
   GROUP BY ad_id, country
)
SELECT A.*, (A.ad_performance.clicks / A.ad_performance.impressions) AS CTR, 
    B.country, B.click_num, B.view_num, B.purchase_num
FROM `final-113356002.preprocess.ad_performance` AS A
JOIN customer_cte2 AS B
ON A.ad_performance.country = B.country AND A.ad_id = B.ad_id;'''

bigquery_client.query(query4)

#  CREATE united table integrated_ad_analysis

""" query5 = '''
CREATE OR REPLACE TABLE `final-113356002.preprocess.integrated_ad_analysis_original` AS
SELECT A.country_code, A.term, A.week, A.score, A.score_null,
     B.target_city_num, B.target_region_num, C.*,
        D.pageviews, D.transactions
FROM `final-113356002.preprocess.g_trends` A
LEFT JOIN `final-113356002.preprocess.ad_geo` B
    ON A.country_name = B.country_name
LEFT JOIN `final-113356002.preprocess.ad_info_original` C
    ON A.country_name = C.country
LEFT JOIN `final-113356002.preprocess.ga` D
    ON A.country_name = D.country
'''

bigquery_client.query(query5) """

#updated table with sophisticated ad_info
query5 = '''
CREATE OR REPLACE TABLE `final-113356002.preprocess.integrated_ad_analysis` AS
SELECT A.country_code, A.term, A.week, A.score, A.score_null,
        B.target_city_num, B.target_region_num, C.*,
        D.pageviews, D.transactions
FROM `final-113356002.preprocess.g_trends` A
LEFT JOIN `final-113356002.preprocess.ad_geo` B
    ON A.country_code = B.country_code
LEFT JOIN `final-113356002.preprocess.ad_info` C
    ON A.country_name = C.country
LEFT JOIN `final-113356002.preprocess.ga` D
    ON A.country_name = D.country
'''

bigquery_client.query(query5)

#%% analysis - Regional Trends

query6 = '''
SELECT term, country, score FROM (
  SELECT term, country, score, 
  RANK() OVER (PARTITION BY country ORDER BY score DESC) AS rank
  FROM `final-113356002.preprocess.integrated_ad_analysis`
  WHERE week = '2024-12-01')
WHERE rank <= 5 AND country IS NOT NULL'''

df_query6 = bigquery_client.query(query6).to_dataframe()
print(df_query6)
#因為score跟時間性有關，因此score相同(original_score = 100)的term會有很多

#%% analysis - Correlations

vars = ['score', 'ad_performance.conversions', 'pageviews', 'transactions']
pair_comb = list(combinations(vars, 2))

for i in range(len(pair_comb)):
    var1, var2 = pair_comb[i]
    query7 = f'''
    SELECT ROUND(CORR(CAST({var1} AS FLOAT64), 
                  CAST({var2} AS FLOAT64)), 4) as corr
    FROM `final-113356002.preprocess.integrated_ad_analysis`'''
    corr = bigquery_client.query(query7).to_dataframe()['corr'][0]
    corr = 0 if math.isnan(corr) else corr 
    #nan because covariance btw two variables is 0
    print(f'corr of {var1}, {var2}: {corr}')

#%% analysis - CTR
# CTR for each country

query8 = '''
SELECT DISTINCT country, ROUND(ad_performance.clicks / ad_performance.impressions, 2) AS CTR
FROM `final-113356002.preprocess.integrated_ad_analysis`
WHERE (ad_performance.clicks / ad_performance.impressions) IS NOT NULL
GROUP BY country
ORDER BY AVG(ad_performance.clicks / ad_performance.impressions) DESC
'''
df_query8 = bigquery_client.query(query8).to_dataframe()
print(df_query8)



#CTR for each ad_id

query9 = '''
SELECT ad_id, AVG(ad_performance.clicks / ad_performance.impressions) AS CTR
FROM `final-113356002.preprocess.integrated_ad_analysis`
WHERE (ad_performance.clicks / ad_performance.impressions) IS NOT NULL
GROUP BY ad_id
ORDER BY AVG(ad_performance.clicks / ad_performance.impressions) DESC
'''
df_query9 = bigquery_client.query(query9).to_dataframe()
print(df_query9)

# approximate distinct count of country
query10 = '''
SELECT APPROX_COUNT_DISTINCT(country) as country_num
FROM `final-113356002.preprocess.integrated_ad_analysis`
'''
print(bigquery_client.query(query10).to_dataframe())

#%% ML modeling

dataset = bigquery_client.dataset('model')
bigquery_client.create_dataset(dataset)

#sampling
query11 = '''

CREATE OR REPLACE TABLE `final-113356002.model.all` AS
SELECT *
FROM `final-113356002.preprocess.integrated_ad_analysis`;

CREATE OR REPLACE TABLE  `final-113356002.model.train` AS
SELECT *
FROM `final-113356002.model.all`
WHERE sample_num < 0.8;

CREATE OR REPLACE TABLE  `final-113356002.model.test` AS
SELECT *
FROM `final-113356002.model.all`
WHERE sample_num >= 0.8;

'''
bigquery_client.query(query11)

#training

query12 = '''
CREATE OR REPLACE MODEL `final-113356002.model.ML1`
OPTIONS(
  model_type = 'linear_reg',
  input_label_cols = ['conversions']
) AS
SELECT 
CAST(ad_performance.conversions AS FLOAT64) AS conversions, score, CTR, click_num
FROM `final-113356002.model.train`
WHERE ad_performance.conversions IS NOT NULL
'''

bigquery_client.query(query12)

#evaluating
query13 = '''

SELECT ROUND(mean_absolute_error,3) AS MAE, 
        ROUND(sqrt(mean_squared_error),3) AS RMSE,
        ROUND(r2_score,3) AS R_SQUARE
FROM ML.EVALUATE(
  MODEL `final-113356002.model.ML1`,
  (SELECT
  CAST(ad_performance.conversions AS FLOAT64) AS conversions, score, CTR, click_num
  FROM `final-113356002.model.test`
  WHERE ad_performance.conversions IS NOT NULL
  )
)
;
'''
df_query13 = bigquery_client.query(query13).to_dataframe()
#print(df_query13)

#%% Complex Model

#training

query14 = '''

CREATE OR REPLACE MODEL `final-113356002.model.ML2`
OPTIONS(
  model_type = 'linear_reg',
  input_label_cols = ['conversions']
) AS
SELECT 
CAST(ad_performance.conversions AS FLOAT64) AS conversions,
    score, score_null, CTR, click_num, view_num, purchase_num
FROM `final-113356002.model.train`
WHERE ad_performance.conversions IS NOT NULL
'''
bigquery_client.query(query14)

#evaluating
query15 = '''
SELECT ROUND(mean_absolute_error,3) AS MAE, 
        ROUND(sqrt(mean_squared_error),3) AS RMSE,
        ROUND(r2_score,3) AS R_SQUARE
FROM ML.EVALUATE(
  MODEL `final-113356002.model.ML2`,
  (
  SELECT
    CAST(ad_performance.conversions AS FLOAT64) AS conversions,
    score, score_null, CTR, click_num, view_num, purchase_num
    FROM `final-113356002.model.test`
    WHERE ad_performance.conversions IS NOT NULL
  )
);
'''
df_query15 = bigquery_client.query(query15).to_dataframe()
#print(df_query15)

# Create Table of ABS ERROR of each ad_id
query16 = '''

CREATE TABLE `final-113356002.model.pred_ML1` AS
SELECT * FROM (
SELECT ad_id, 
    AVG(ABS(predicted_conversions - CAST (ad_performance.conversions AS FLOAT64))) AS ABS_ERROR
FROM
  ML.PREDICT(
    MODEL `final-113356002.model.ML1`,  
    TABLE `final-113356002.model.test`)
WHERE ad_performance.conversions IS NOT NULL
GROUP BY ad_id) ORDER BY ad_id;


CREATE TABLE `final-113356002.model.pred_ML2` AS
SELECT * FROM (
SELECT ad_id,
    AVG(ABS(predicted_conversions - CAST (ad_performance.conversions AS FLOAT64))) AS ABS_ERROR
FROM
  ML.PREDICT(
    MODEL `final-113356002.model.ML2`,  
    TABLE `final-113356002.model.test`)
WHERE ad_performance.conversions IS NOT NULL
GROUP BY ad_id) ORDER BY ad_id
'''

bigquery_client.query(query16)




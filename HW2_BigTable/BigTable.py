#%% 
# import必要module
from google.cloud import bigtable
from google.cloud import bigquery
from google.cloud import storage

from google.oauth2 import service_account
import pandas as pd
from google.api_core.exceptions import NotFound
import base64

#set up project and credentials
project_id = 's113356002hw3'
credentials = service_account.Credentials.from_service_account_file(f'C:\\Users\weihs\桌面\\WEIHSUAN\\2024\碩班\\資料模式\\s113356002hw3-fc36f4516903.json')



#%% Q1 create big table

#set up big table

instance_id = 'studentinstance'
table_id = 'studentdata'

#設定bigtable連線
bigtable_client = bigtable.Client(project = project_id, credentials = credentials, admin=True)

#create instance
instance = bigtable_client.instance(instance_id)
instance.create('us-central1-a')

#create table
table = instance.table(table_id)
table.create()

#create column_family user_info, activity_log if they dont exist
existing_families = table.list_column_families()
column_family = ['user_info', 'activity_log']

for i in column_family:
    if i not in existing_families:
       table.column_family(i).create()
       print(f'create column family: {i}')


#create dataframe to insert value needed for big table 
df_data = pd.DataFrame({'Row_Key': ['User1', 'User2', 'User3', 'User4'],
                  'user_info': ['Alice', 'Bob', 'Cindy', 'David'],
                  'activity': ['login', 'purchase', 'login', 'purchase'],
                  'activity_log': ['2024-01-01T12:00:00Z', '2024-01-02T08:30:00Z', '2024-01-03T15:45:00Z', '2024-01-05T17:36:00Z']})

df_column = pd.DataFrame({'col_family': ['user_info', 'user_info', 'activity_log',  'activity_log'],
                         'col_name': ['Row_Key', 'user_info', 'activity',  'activity_log']})

def insert_data(row_key, column_family_id, col_name, value):
    row = table.direct_row(row_key)
    row.set_cell(column_family_id, col_name, value)
    row.commit()

for j in range(len(df_column)):
    for i in range(len(df_data)):
        insert_data(df_data.Row_Key[i], df_column.col_family[j], df_column.col_name[j], df_data[df_column.col_name[j]][i])


#%% Q2 create external big table at big query

#建立bigquery連線
credentials = service_account.Credentials.from_service_account_file(f'C:\\Users\weihs\桌面\\WEIHSUAN\\2024\碩班\\資料模式\\s113356002hw3-fc36f4516903.json')
bigquery_client = bigquery.Client(project = project_id, credentials = credentials)

#設定bigquery dataset及table
dataset_ref = bigquery_client.dataset('bigtable')
dataset = bigquery.Dataset(dataset_ref)
dataset.location = 'US'
dataset = bigquery_client.create_dataset(dataset)
table_ref = bigquery.TableReference(dataset_ref, 'bigtable_external')
table = bigquery.Table(table_ref)

#使用external_config將big query的table導向big table的url
external_config = bigquery.ExternalConfig('BIGTABLE')
source_uris = f'https://googleapis.com/bigtable/projects/{project_id}/instances/{instance_id}/tables/{table_id}'
external_config.source_uris = source_uris
table.external_data_configuration = external_config

bigquery_client.create_table(table)


#%% Q3 create external table from csv at big query

#設定cloud store連線
storage_client = storage.Client(project = project_id, credentials = credentials)

#set up bucket to sotre the csv file 
bucket_name = 'bucket_test_113356002_hw3'
bucket = storage_client.bucket(bucket_name)
new_bucket = storage_client.create_bucket(bucket, location='US')
new_bucket.storage_class = 'STANDARD'
new_bucket.patch()

#upload file from local path (through pandas)
blob = bucket.blob('cloud_storage_external.csv')
blob.upload_from_filename('C:\\Users\\weihs\\桌面\\WEIHSUAN\\2024\\碩班\資料模式\\activity_log.csv')

df = pd.read_csv('C:\\Users\\weihs\\桌面\\WEIHSUAN\\2024\\碩班\資料模式\\activity_log.csv')

#set up the table info
csv_name = 'cloud_storage_external'
dataset_ref = bigquery_client.dataset('bigtable')
table_ref = bigquery.TableReference(dataset_ref, csv_name)

#set up schema
schema = [
            bigquery.SchemaField('user_name', 'STRING'),
            bigquery.SchemaField('activity_type', 'STRING'),
            bigquery.SchemaField('activity_time', 'TIMESTAMP'),
            bigquery.SchemaField('location', 'STRING'),
            bigquery.SchemaField('device_type', 'STRING'),
]

table = bigquery.Table(table_ref, schema = schema)


#set up config and make sure the uris directing the to csv url
external_config = bigquery.ExternalConfig('CSV')
external_config.source_uris = f'gs://{bucket_name}/{csv_name}.csv'
external_config.options.skip_leading_rows = 1
external_config.options.field_delimiter = ","
table.external_data_configuration = external_config

#create table
bigquery_client.create_table(table)





#%% Q4 JOIN table to combine both bigtable and csv source

query = '''
WITH bigtable AS(
    SELECT
        activity_log.`column`[SAFE_OFFSET(0)].`cell`[SAFE_OFFSET(0)].value AS activity_type,
        activity_log.`column`[SAFE_OFFSET(1)].`cell`[SAFE_OFFSET(0)].value AS activity_time,
        user_info.`column`[SAFE_OFFSET(1)].`cell`[SAFE_OFFSET(0)].value AS user_name
    FROM `s113356002hw3.bigtable.bigtable_external`
), csv AS(
    SELECT user_name, activity_type, activity_time
    FROM `s113356002hw3.bigtable.cloud_storage_external`
)

SELECT *
FROM bigtable AS a
INNER JOIN csv as b
ON CAST(a.user_name AS STRING) = b.user_name
WHERE 
    b.activity_type = 'login'
ORDER BY 
    b.activity_time DESC;

'''

query_job = bigquery_client.query(query)
result = query_job.to_dataframe()
print(result) #print query result


#%% Q5 UNINO two source to find the rank of login number

query = f'''
WITH bigtable AS(
    SELECT
        user_info.`column`[SAFE_OFFSET(1)].`cell`[SAFE_OFFSET(0)].value AS user_name,
        activity_log.`column`[SAFE_OFFSET(1)].`cell`[SAFE_OFFSET(0)].value AS activity_time
    FROM `s113356002hw3.bigtable.bigtable_external`
    WHERE CAST(activity_log.`column`[SAFE_OFFSET(0)].`cell`[SAFE_OFFSET(0)].value AS STRING) = 'login'
), csv AS(
    SELECT user_name, activity_time
    FROM `s113356002hw3.bigtable.cloud_storage_external`
    WHERE activity_type = 'login'
), login AS(
  SELECT 
    CAST(user_name AS STRING) AS user_name, 
    CAST(CAST(activity_time AS STRING) AS TIMESTAMP) AS activity_time
  FROM bigtable

  UNION DISTINCT

  SELECT user_name, activity_time
  FROM csv
), login_times AS(
  SELECT count(*) AS login_time, user_name
  FROM login
  GROUP BY user_name

)
SELECT user_name, login_time, RANK() OVER (ORDER BY login_time DESC) AS rank 
FROM login_times
ORDER BY rank
LIMIT 2;
'''

query_job = bigquery_client.query(query)
result = query_job.to_dataframe()
print(result) #print query result


#%% Q6 grant big query table viewer access to TA by mail


def grant_data_viewer_role(mail):
    dataset = bigquery_client.get_dataset(f'{project_id}.bigtable')
    access_entry = bigquery.AccessEntry(
        role = "roles/bigquery.dataViewer",
        entity_type = 'userByEmail',
        entity_id = mail,
    )
    entries = list(dataset.access_entries)
    entries.append(access_entry)
    dataset.access_entries = entries
    bigquery_client.update_dataset(dataset, ['access_entries'])

grant_data_viewer_role('113356042@g.nccu.edu.tw')    

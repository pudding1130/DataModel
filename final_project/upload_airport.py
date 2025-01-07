import pandas as pd
import os
from google.cloud import bigquery
from google.cloud import storage
from google.oauth2 import service_account
from itertools import combinations

#%% connect to storage and BQ

credentials = service_account.Credentials.from_service_account_file(r"C:\Users\weihs\桌面\WEIHSUAN\2024\碩班\資料模式\final_project\data-model-final-084d9169a7a0.json")
project_id = 'data-model-final'
storage_client = storage.Client(project = project_id, credentials = credentials)
bigquery_client = bigquery.Client(project = project_id, credentials = credentials)
bucket = storage_client.bucket('bucket_airport')
new_bucket = storage_client.create_bucket(bucket, location='US')
new_bucket.storage_class = 'STANDARD'
new_bucket.patch()
dataset = bigquery_client.dataset('airport')
bigquery_client.create_dataset(dataset)


#%% arrival data

schema = [
            bigquery.SchemaField('Carrier_code', 'STRING'),
            bigquery.SchemaField('Date', 'DATE'),
            bigquery.SchemaField('Flight_no', 'STRING'),
            bigquery.SchemaField('Tail_no', 'STRING'),
            bigquery.SchemaField('Origin', 'STRING'),
            bigquery.SchemaField('Scheduled_arrival', 'TIME'),
            bigquery.SchemaField('Actual_arrival', 'TIME'),
            bigquery.SchemaField('Scheduled_elasped', 'INTEGER'),     
            bigquery.SchemaField('Actual_elasped', 'INTEGER'),     
            bigquery.SchemaField('Delay_mins', 'INTEGER'),
            bigquery.SchemaField('Wheels_on_Time', 'TIME'),
            bigquery.SchemaField('Taxi_in_mins', 'INTEGER'), 
            bigquery.SchemaField('Delay_Carrier_mins', 'INTEGER'),     
            bigquery.SchemaField('Delay_Weather_mins', 'INTEGER'),
            bigquery.SchemaField('Delay_National_Aviation_mins', 'INTEGER'),
            bigquery.SchemaField('Delay_Security', 'INTEGER'),
            bigquery.SchemaField('Delay_Late_Aircraft_Arrival', 'INTEGER')

]


class Airport_arrival:
    def __init__(self, name):
        print(f'airport obejct named {name} is created')
        self.name = name
        self.df = pd.DataFrame()

    def load(self):
        self.df = pd.DataFrame()
        print(f'dataframe of airport {self.name} is loading')
        for obj in  os.listdir(f'{os.getcwd()}\\{self.name}'):
            tmp = pd.read_csv(f'{os.getcwd()}\\{self.name}\\{obj}')
            self.df = pd.concat([self.df, tmp],axis = 0, ignore_index=True)
            print(f'{obj} row is {len(tmp)}')
        self.df['Date (MM/DD/YYYY)']  = pd.to_datetime(self.df.iloc[:,1],format='%m/%d/%Y').dt.strftime('%Y-%m-%d')
        for time_col in ['Scheduled Arrival Time', 'Actual Arrival Time', 'Wheels-on Time']:
            self.df[time_col] = self.df[time_col].replace('24:00:00', '00:00')
            self.df[time_col] = self.df[time_col] + ':00'
            self.df[time_col] = pd.to_datetime(self.df[time_col], format='%H:%M:%S').dt.time
        print(f'total sample of {self.name} (arrival) is {len(self.df)}')

    def save(self):
        self.df.to_csv(f'{self.name}.csv', index = False)
        print(f'dataframe of airport {self.name} (arrival) is saved in dataset')
    def to_cloud(self):
        blob = bucket.blob(f'{self.name}.csv')
        blob.upload_from_filename(f'{os.getcwd()}\\{self.name}.csv')
        print(f'{self.name} (arrival) dataframe is uploading to cloud storage')
    def bq_ref(self):
        table_ref = bigquery.TableReference(globals()['dataset'], self.name)
        table = bigquery.Table(table_ref, schema = globals()['schema'])
        external_config = bigquery.ExternalConfig('CSV')
        external_config.source_uris = f'gs://bucket_airport/{self.name}.csv'
        external_config.options.skip_leading_rows = 1
        external_config.options.field_delimiter = ","
        table.external_data_configuration = external_config
        bigquery_client.create_table(table)
        print(f'{self.name} table is created in BigQuery')



os.chdir(r'C:\Users\weihs\桌面\WEIHSUAN\2024\碩班\資料模式\final_project\dataset\Arrival')
airport_folder = [airport for airport in os.listdir() if airport.endswith('csv') == False]
for airport in airport_folder:
    globals()[airport] = Airport_arrival(airport)
    globals()[airport].load()
    globals()[airport].save()
    globals()[airport].to_cloud()
    globals()[airport].bq_ref()
    



#%% departure data

schema_dep = [
            bigquery.SchemaField('Carrier_code', 'STRING'),
            bigquery.SchemaField('Date', 'DATE'),
            bigquery.SchemaField('Flight_no', 'STRING'),
            bigquery.SchemaField('Tail_no', 'STRING'),
            bigquery.SchemaField('Destination', 'STRING'),
            bigquery.SchemaField('Scheduled_depature', 'TIME'),
            bigquery.SchemaField('Actual_depature', 'TIME'),
            bigquery.SchemaField('Scheduled_elasped', 'INTEGER'),     
            bigquery.SchemaField('Actual_elasped', 'INTEGER'),     
            bigquery.SchemaField('Delay_mins', 'INTEGER'),
            bigquery.SchemaField('Wheels_on_Time', 'TIME'),
            bigquery.SchemaField('Taxi_in_mins', 'INTEGER'), 
            bigquery.SchemaField('Delay_Carrier_mins', 'INTEGER'),     
            bigquery.SchemaField('Delay_Weather_mins', 'INTEGER'),
            bigquery.SchemaField('Delay_National_Aviation_mins', 'INTEGER'),
            bigquery.SchemaField('Delay_Security', 'INTEGER'),
            bigquery.SchemaField('Delay_Late_Aircraft_Arrival', 'INTEGER')

]

class Airport_dep:
    def __init__(self, name):
        print(f'airport obejct named {name} is created')
        self.name = name
        self.df = pd.DataFrame()
#
    def load(self):
        self.df = pd.DataFrame()
        print(f'dataframe of airport {self.name} is loading')
        for obj in  os.listdir(f'{os.getcwd()}\\{self.name}'):
            tmp = pd.read_csv(f'{os.getcwd()}\\{self.name}\\{obj}')
            self.df = pd.concat([self.df, tmp],axis = 0, ignore_index=True)
            print(f'{obj} row is {len(tmp)}')
        self.df['Date (MM/DD/YYYY)']  = pd.to_datetime(self.df.iloc[:,1],format='%m/%d/%Y').dt.strftime('%Y-%m-%d')
        for time_col in ['Scheduled departure time', 'Actual departure time', 'Wheels-off time']:
            self.df[time_col] = self.df[time_col].replace('24:00:00', '00:00')
            self.df[time_col] = self.df[time_col] + ':00'
            self.df[time_col] = pd.to_datetime(self.df[time_col], format='%H:%M:%S').dt.time
        print(f'total sample of {self.name} (departure) is {len(self.df)}')
#
    def save(self):
        self.df.to_csv(f'{self.name}_dep.csv', index = False)
        print(f'dataframe of airport {self.name}  (departure)  is saved in dataset')
#
    def to_cloud(self):
        blob = bucket.blob(f'{self.name}_dep.csv')
        blob.upload_from_filename(f'{os.getcwd()}\\{self.name}_dep.csv')
        print(f'{self.name} dataframe is uploading to cloud storage')
    def bq_ref(self):
        table_ref = bigquery.TableReference(globals()['dataset'], self.name+'_dep')
        table = bigquery.Table(table_ref, schema = globals()['schema_dep'])
        external_config = bigquery.ExternalConfig('CSV')
        external_config.source_uris = f'gs://bucket_airport/{self.name}_dep.csv'
        external_config.options.skip_leading_rows = 1
        external_config.options.field_delimiter = ","
        table.external_data_configuration = external_config
        bigquery_client.create_table(table)
        print(f'{self.name} table is created in BigQuery')


os.chdir(r'C:\Users\weihs\桌面\WEIHSUAN\2024\碩班\資料模式\final_project\dataset\Departure')
airport_folder = [airport for airport in os.listdir() if airport.endswith('csv') == False]

for airport in airport_folder:
    globals()[airport] = Airport_dep(airport)
    globals()[airport].load()
    globals()[airport].save()
    globals()[airport].to_cloud()
    globals()[airport].bq_ref()
    
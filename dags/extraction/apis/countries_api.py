import pendulum
from airflow.decorators import dag, task


@dag(start_date=pendulum.yesterday(), schedule_interval=None, catchup=False, tags=['extract', 'api'])
def countries_api_extractor():
    @task
    def extract_to_landing():
        from airflow.providers.http.hooks.http import HttpHook
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook

        now = pendulum.now()
        ds = now.format('YYYY-MM-DD')
        ts_nodash = now.format('YYYYMMDDTHHMMSS')

        http_hook = HttpHook(http_conn_id='countries_api', method='GET')
        response = http_hook.run('independent?status=true&fields=name,cca3,area,population')
        data = response.text

        json_file_path = f'landing/apis/countries_api/new/{ds}/response_{ts_nodash}.json'
        s3_hook = S3Hook(aws_conn_id='aws_default')
        s3_hook.load_string(
            string_data=data,
            bucket_name='victorouttes-landing',
            key=json_file_path
        )
        return json_file_path

    @task
    def landing_to_bronze(path: str):
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        import pandas as pd
        import json
        from utils.python_iceberg_sdc2 import IcebergDatabase, IcebergTable

        print(f'Reading data from {path}...')

        s3_hook = S3Hook(aws_conn_id='aws_default')
        file = s3_hook.get_key(key=path, bucket_name='victorouttes-landing')
        data_bytes = file.get()['Body'].read()
        data_str = data_bytes.decode('utf-8')

        data_json = json.loads(data_str)
        df = pd.json_normalize(data_json)
        df = df[['cca3', 'area', 'population', 'name.common']]
        df = df.rename(columns={'cca3': 'code', 'name.common': 'name'})

        df['area'] = '0'

        database = IcebergDatabase(
            database_name='iceberg_lake',
            catalog_s3_path='s3://victorouttes-landing/dw/iceberg_lake.db',
            aws_hook=s3_hook
        )
        IcebergTable(
            database=database,
            table_name='countries',
            business_keys=['code']
        ).prepare_data(df=df).scd2_add()


    path = extract_to_landing()
    landing_to_bronze(path)

countries_api_extractor()

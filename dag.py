from airflow.utils.task_group import TaskGroup
from airflow import DAG
from airflow.operators.python import PythonOperator,BranchPythonOperator,PythonVirtualenvOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.bash import BashOperator
from dividas.src.features.requests_features import *

from datetime import datetime, timedelta

ip = '172.23.0.2'
args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 10),
 }

####
def download():
    import requests

    url = 'https://temporeal.pbh.gov.br/?param=D'
    response = requests.get(url)
    response.raise_for_status()

    format_str = '%Y%m%d%H%M%S'
    now = datetime.now().strftime(format_str)

    file_name = f'tempo_real_convencional_json_{now}.json'

    if response.status_code == 200:
        with open(f'/opt/airflow/downloads/data/raw/{file_name}', 'wb') as f:
            f.write(response.content)
        print('File downloaded successfully.')

    return now

def transform_dtypes(now):
    import json
    from datetime import datetime

    format_str = '%Y%m%d%H%M%S'
    file_name = f'tempo_real_convencional_json_{now}.json'

    dtypes = {
        "EV": str,  # event position code
        "HR": str,  # date/time: aaaammddhhmmss
        "LT": float,  # latitude
        "LG": float,  # longitude
        "NV": str,  # veichle id
        "VL": float,  # veichle speed (km/h)
        "NL": str,  # veichle line code
        "DG": str,  # veichle direction
        "SV": str,  # veichel way - (1) going or (2) return
        "DT": int  # distance traveled (km)
    }

    with open(f'/opt/airflow/downloads/data/raw/{file_name}') as f:
        data = json.load(f)

    for i in range(len(data)):
        for key, dtype in dtypes.items():
            if key in data[i]:
                data[i][key] = dtype(data[i][key])
            else:
                data[i][key] = None
            if key == 'HR':
                data[i][key] = str(datetime.strptime(data[i]["HR"], format_str))

    with open(f'/opt/airflow/downloads/data/transformed/{file_name}', 'w') as f:
        json.dump(data, f)

def get_only_new_records(now):
    import json
    import os

    # Get a list of all files in the directory ordered by creation time (descending),
    # the first register is the newest file from the folder
    file_list = os.listdir(f'/opt/airflow/downloads/data/transformed/')
    file_name = f'tempo_real_convencional_json_{now}.json'

    print('file_list',len(file_list))
    if len(file_list) > 1:
        newest_file = sorted(file_list, key=lambda x: os.stat(os.path.join(f'/opt/airflow/downloads/data/transformed/', x, )).st_ctime, reverse=True)[1]

        with open(f'/opt/airflow/downloads/data/transformed/{newest_file}', 'r') as f:
            existing_data = json.load(f)

        print(file_name)
        with open(f'/opt/airflow/downloads/data/transformed/{file_name}', 'r') as f:
            data = json.load(f)

        # Remove duplicates
        new_records = []

        existing_hr_nl = {(d['HR'], d['NL']) for d in existing_data}
        new_records = [d for d in data if (d['HR'], d['NL']) not in existing_hr_nl]

        print(f'new records: {len(new_records)}')

        with open(f'/opt/airflow/downloads/data/trusted/new_records_{now}.json','w') as f:
            json.dump(new_records,f)

def insert_db_with_geometry(now):
    import pandas as pd
    import psycopg2
    from sqlalchemy import create_engine

    # send new_records to RDS
    print(now)
    df = pd.read_json(f'/opt/airflow/downloads/data/trusted/new_records_{now}.json')
    print(df)

    # establish connections
    engine = create_engine(f'postgresql://airflow:airflow@{ip}:5432/postgres')
    conn = psycopg2.connect(
        host=ip,
        database="postgres",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()

    df.to_sql('your_table_name', engine, if_exists='append', index=False)
    cur.execute("""ALTER TABLE your_table_name ADD COLUMN IF NOT EXISTS geom geometry;""")
    conn.commit()
    cur.execute(
        """
            UPDATE your_table_name
            SET geom = ST_SetSRID
            (
                ST_MakePoint
                (
                    "LG",
                    "LT"
                ),4326
            )
        """)
    conn.commit()
    cur.close()
    conn.close()
    engine.dispose()

def validation_using_spatial(ip):
    import psycopg2
    from geobr import read_municipality
    from sqlalchemy import create_engine

    print("IP",ip)
    engine = create_engine(f'postgresql://airflow:airflow@{ip}:5432/postgres')
    conn = psycopg2.connect(
        host=ip,
        database="postgres",
        user="airflow",
        password="airflow"
    )
    cur = conn.cursor()

    # Read city boundaries
    mun = read_municipality(code_muni=3106200, year=2020)
    # Send to PostGIS
    mun.to_postgis('city_boundaries', engine, if_exists='replace', index=False)

    queries = [
        f"""SELECT UpdateGeometrySRID('city_boundaries','geometry',4326);""",
        f"""ALTER TABLE your_table_name ADD COLUMN IF NOT EXISTS valid_data boolean default true;""",
        f"""UPDATE your_table_name AS a SET valid_data = false FROM city_boundaries AS b WHERE ST_Intersects(a.geom,b.geometry);"""
    ]

    for query in queries:
        cur.execute(query)
        conn.commit()

def export_to_s3_as_geoparquet(ip):
    import boto3
    import geopandas as gpd
    from sqlalchemy import create_engine

    engine = create_engine(f'postgresql://airflow:airflow@{ip}:5432/postgres')
    sql = """SELECT * FROM your_table_name"""
    df = gpd.GeoDataFrame.from_postgis(sql, engine)
    df.to_parquet('/opt/airflow/downloads/data/trusted/data.parquet')
    print(df)

    key_id = 'AKIA2GWUMHU3VDMMRFGZ'
    secret_key = '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'
    region = 'us-east-2'

    # Instantiate client
    client = boto3.resource(
        's3',
        aws_access_key_id=key_id,
        aws_secret_access_key=secret_key,
        region_name=region
    )

    bucket_access = client.Bucket('data-tables-general')
    print("S3 Bucket conection made")

    bucket_access.put_object(Body='file_path', Key='file_name')
    print(f"The file was uploaded on {'file_name'}")

with DAG(
    dag_id='real_time_public_transport',
    description='get real time data from public transport based on Belo Horizonte/MG - Brazil',
    default_args=args,
    schedule_interval=timedelta(minutes=1),
    catchup=False) as dag:

    Download = PythonOperator(
            task_id="download",
            python_callable=download
    )

    SetDtypes = PythonOperator(
            task_id="transform_dtypes",
            python_callable=transform_dtypes,
            op_kwargs={'now':'{{ti.xcom_pull(task_ids="download")}}'}
    )

    GetOnlyNewRecords = PythonOperator(
            task_id="get_only_new_records",
            python_callable=get_only_new_records,
            op_kwargs={'now':'{{ti.xcom_pull(task_ids="download")}}'}
    )

    InsertDbWithGeometry = PythonOperator(
            task_id="insert_db_with_geometry",
            python_callable=insert_db_with_geometry,
            op_kwargs={'now':'{{ti.xcom_pull(task_ids="download")}}'}
    )

    ValidationUsingSpatial = PythonVirtualenvOperator(
            task_id="validation_using_spatial",
            requirements="geopandas==0.10.2",
            python_callable=validation_using_spatial,
            op_kwargs={'ip':ip}
    )

    ExportToS3AsGeoparquet = PythonVirtualenvOperator(
            task_id="export_to_s3_as_geoparquet",
            requirements=["pyarrow==4.0.1","geopandas==0.10.2"],
            python_callable=export_to_s3_as_geoparquet,
            op_kwargs={'ip':ip}
    )

    Download >> SetDtypes >> GetOnlyNewRecords >> InsertDbWithGeometry >> ValidationUsingSpatial >> ExportToS3AsGeoparquet

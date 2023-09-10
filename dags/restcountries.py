from airflow import DAG
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.decorators import task

from datetime import datetime
from datetime import timedelta

import requests
import logging
import psycopg2


def get_Redshift_connection(autocommit=True):
    hook = PostgresHook(postgres_conn_id='redshift_dev_db')
    conn = hook.get_conn()
    conn.autocommit = autocommit
    return conn.cursor()


def create_table(schema, table):
    logging.info("create table started")    
    cur = get_Redshift_connection()
    cur.execute("DROP TABLE IF EXISTS {}.{};".format(schema, table))
    sql = "CREATE TABLE {}.{} (".format(schema, table)
    sql += "country varchar(255),"
    sql += " population int4,"
    sql += " area float8"
    sql += ");"
    cur.execute(sql)


@task
def extract(url):
    logging.info(datetime.utcnow())
    f = requests.get(url)
    return f.json()


@task
def transform(json):
    records = list()
    for j in json:
        country = j['name']['official'].replace('\'', '\\\'')
        population = j['population']
        area = j['area']
        
        records.append({
            'country' : country,
            'population' : population,
            'area' : area
        })
        
        logging.info("Transform ended")
    return records


@task
def load(schema, table, records):
    logging.info("load started")
    cur = get_Redshift_connection()
    # BEGIN과 END를 사용해서 SQL 결과를 트랜잭션으로 만들어주는 것이 좋음
    try:
        cur.execute("BEGIN;")
        cur.execute("DELETE FROM {}.{};".format(schema, table)) 
        # DELETE FROM을 먼저 수행 -> FULL REFRESH을 하는 형태
        for r in records:
            country = r['country']
            population = r['population']
            area = r['area']
            sql = "INSERT INTO {}.{} (country, population, area) VALUES ('{}', {}, {});".format(schema, table, country, population, area)
            logging.info(sql)
            cur.execute(sql)
        cur.execute("COMMIT;")   # cur.execute("END;")
    except (Exception, psycopg2.DatabaseError) as error:
        logging.error(error)
        cur.execute("ROLLBACK;")
    logging.info("load done")


with DAG(
    dag_id='restcountries',
    start_date=datetime(2023, 9, 6),  # 날짜가 미래인 경우 실행이 안됨
    schedule='30 6 * * 6',  # 적당히 조절
    max_active_runs=1,
    catchup=False,
    default_args={
        'retries': 1,
        'retry_delay': timedelta(minutes=3),
        # 'on_failure_callback': slack.on_failure_callback,
    }
) as dag:

    url = Variable.get("restcountries_url")
    schema = Variable.get("my_schema")   ## 자신의 스키마로 변경
    table = 'country_info'
    
    create_table(schema, table)
    records = transform(extract(url))
    load(schema, table, records)

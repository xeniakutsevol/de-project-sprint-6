import boto3
import pendulum
import vertica_python
from airflow.decorators import dag
from airflow.models import Variable
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

AWS_ACCESS_KEY_ID = Variable.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = Variable.get("AWS_SECRET_ACCESS_KEY")
HOST = Variable.get("HOST")
PORT = Variable.get("PORT")
USER = Variable.get("USER")
PASSWORD = Variable.get("PASSWORD")
DB = Variable.get("DB")

conn_info = {
    "host": HOST,
    "port": PORT,
    "user": USER,
    "password": PASSWORD,
    "database": DB,
    "autocommit": True,
    "use_prepared_statements": False,
    "disable_copy_local": False,
}


def fetch_s3_file(bucket: str, key: str):

    session = boto3.session.Session()
    s3_client = session.client(
        service_name="s3",
        endpoint_url="https://storage.yandexcloud.net",
        aws_access_key_id=AWS_ACCESS_KEY_ID,
        aws_secret_access_key=AWS_SECRET_ACCESS_KEY,
    )
    s3_client.download_file(Bucket=bucket, Key=key, Filename=f"../../../data/{key}")


def load_group_log(conn_info=conn_info):
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute(
            "COPY XENIAKUTSEVOL_GMAIL_COM__STAGING.group_log (group_id, user_id, user_id_from, event, datetime) FROM LOCAL"
            "'/data/group_log.csv' DELIMITER ','"
            "REJECTED DATA '/data/group_log_rejects.txt'"
            "EXCEPTIONS '/data/group_log_exceptions.txt'",
            buffer_size=65536,
        )
        res = cur.fetchall()
        return res


bash_command_tmpl = """
head -10 {{ params.files }}
"""


@dag(schedule_interval=None, start_date=pendulum.parse("2023-02-14"))
def sprint6_project_dag():
    bucket_files = ["group_log.csv"]
    get_data = PythonOperator(
        task_id=f"fetch_group_log.csv",
        python_callable=fetch_s3_file,
        op_kwargs={"bucket": "sprint6", "key": "group_log.csv"},
    )

    print_10_lines_of_each = BashOperator(
        task_id="print_10_lines_of_each",
        bash_command=bash_command_tmpl,
        params={"files": f"""'/data/{f}'""" for f in bucket_files},
    )

    load_data = PythonOperator(
        task_id="load_group_log_staging", python_callable=load_group_log
    )

    get_data >> print_10_lines_of_each >> load_data


_ = sprint6_project_dag()

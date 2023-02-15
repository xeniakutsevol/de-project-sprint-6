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


def load_groups(conn_info=conn_info):
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute(
            "COPY XENIAKUTSEVOL_GMAIL_COM__STAGING.groups (id, admin_id, group_name, registration_dt, is_private) FROM LOCAL"
            "'/data/groups.csv' DELIMITER ','"
            "REJECTED DATA '/data/groups_rejects.txt'"
            "EXCEPTIONS '/data/groups_exceptions.txt'",
            buffer_size=65536,
        )
        res = cur.fetchall()
        return res


def load_users(conn_info=conn_info):
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute(
            "COPY XENIAKUTSEVOL_GMAIL_COM__STAGING.users (id, chat_name, registration_dt, country, age) FROM LOCAL"
            "'/data/users.csv' DELIMITER ','"
            "REJECTED DATA '/data/users_rejects.txt'"
            "EXCEPTIONS '/data/users_exceptions.txt'",
            buffer_size=65536,
        )
        res = cur.fetchall()
        return res


def load_dialogs(conn_info=conn_info):
    with vertica_python.connect(**conn_info) as conn:
        cur = conn.cursor()
        cur.execute(
            "COPY XENIAKUTSEVOL_GMAIL_COM__STAGING.dialogs (message_id, message_ts, message_from, message_to, message, message_group) FROM LOCAL"
            "'/data/dialogs.csv' DELIMITER ','"
            "REJECTED DATA '/data/dialogs_rejects.txt'"
            "EXCEPTIONS '/data/dialogs_exceptions.txt'",
            buffer_size=65536,
        )
        res = cur.fetchall()
        return res


# эту команду надо будет поправить, чтобы она выводила
# первые десять строк каждого файла
bash_command_tmpl = """
head -10 {{ params.files }}
"""


@dag(schedule_interval=None, start_date=pendulum.parse("2022-07-13"))
def sprint6_dag_get_data():
    bucket_files = ["groups.csv", "users.csv", "dialogs.csv"]
    task1 = PythonOperator(
        task_id=f"fetch_groups.csv",
        python_callable=fetch_s3_file,
        op_kwargs={"bucket": "sprint6", "key": "groups.csv"},
    )
    task2 = PythonOperator(
        task_id=f"fetch_users.csv",
        python_callable=fetch_s3_file,
        op_kwargs={"bucket": "sprint6", "key": "users.csv"},
    )
    task3 = PythonOperator(
        task_id=f"fetch_dialogs.csv",
        python_callable=fetch_s3_file,
        op_kwargs={"bucket": "sprint6", "key": "dialogs.csv"},
    )

    print_10_lines_of_each = BashOperator(
        task_id="print_10_lines_of_each",
        bash_command=bash_command_tmpl,
        params={
            "files": f"""'/data/{f}' '/data/{f}' '/data/{f}'""" for f in bucket_files
        },
    )

    load_groups_task = PythonOperator(
        task_id="load_groups_staging", python_callable=load_groups
    )

    load_users_task = PythonOperator(
        task_id="load_users_staging", python_callable=load_users
    )

    load_dialogs_task = PythonOperator(
        task_id="load_dialogs_staging", python_callable=load_dialogs
    )

    (
        [task1, task2, task3]
        >> print_10_lines_of_each
        >> [load_groups_task, load_users_task, load_dialogs_task]
    )


_ = sprint6_dag_get_data()

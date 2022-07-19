from datetime import datetime

from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.google.suite.hooks.sheets import GSheetsHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


def fill_list(checked_list: list, max_length: int):
    checked_list.extend([None] * (max_length - len(checked_list)))

    return checked_list


def replace_empty(checked_list: list, replacer="unknown"):
    if len(checked_list) == 0:
        checked_list.append(replacer)

    for index, item in enumerate(checked_list):
        if not item:
            checked_list[index] = replacer

    return checked_list


def _spreadsheet_extract_data():
    SPREADSHEET_ID = Variable.get("SPREADSHEET_ID")
    TEAM = Variable.get("TEAM", default_var="Employee!A:A")
    EMPLOYEE = Variable.get("EMPLOYEE", default_var="Employee!F:H")
    MANAGER = Variable.get("MANAGER", default_var="Employee!K:K")
    RANGES = [TEAM, EMPLOYEE, MANAGER]

    return GSheetsHook(gcp_conn_id="gcp_connection").batch_get_values(SPREADSHEET_ID, RANGES)


def _spreadsheet_transform_data(ti):
    response = ti.xcom_pull(task_ids="spreadsheet_extract_data").get("valueRanges", [])
    employees = []

    for data_index, data in enumerate(response):
        data_values = data.get("values", [[]])

        if data_index == 0:
            for values in data_values:
                employees.append(replace_empty(values))
            continue

        if data_index == 1:
            for values in data_values:
                fill_list(values, 3)

        for row_index, rows in enumerate(data_values):
            employees[row_index].extend(replace_empty(rows))

    return employees


def _insert(ti):
    employees = ti.xcom_pull(task_ids="spreadsheet_transform_data")[1:]
    postgres_hook = PostgresHook(postgres_conn_id="postgres_connection")
    fields = ['team', 'work_status', 'full_name', 'job_title', 'manager']
    postgres_hook.insert_rows(
        table='employees',
        rows=employees,
        target_fields=fields,
    )


with DAG(
        "google_spreadsheets_dag",
        schedule_interval="@daily",
        start_date=datetime(2022, 7, 1),
        catchup=False,
        tags=["spreadsheets"],
) as dag:
    spreadsheet_extract_data = PythonOperator(
        task_id="spreadsheet_extract_data",
        python_callable=_spreadsheet_extract_data,
    )

    spreadsheet_transform_data = PythonOperator(
        task_id="spreadsheet_transform_data",
        python_callable=_spreadsheet_transform_data,
    )

    create_postgres_table = PostgresOperator(
        task_id='create_postgres_table',
        postgres_conn_id="postgres_connection",
        sql="""
            CREATE TABLE IF NOT EXISTS employees (
                id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
                full_name VARCHAR,
                job_title VARCHAR,
                team VARCHAR,
                manager VARCHAR,
                work_status VARCHAR
            )
        """
    )

    insert_into_table = PythonOperator(
        task_id='insert_into_table',
        python_callable=_insert
    )

    chain(
        spreadsheet_extract_data,
        spreadsheet_transform_data,
        create_postgres_table,
        insert_into_table
    )

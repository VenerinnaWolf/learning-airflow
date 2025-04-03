from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable  # переменные из настроек Airflow
from datetime import datetime

import pandas as pd
import os.path

# ------- Переменные и константы -------

# PATH_TO_FILES = Variable.get("path_to_files")  # = /files/
# PATH_TO_TITANIC = f"{PATH_TO_FILES}titanic/"
# EXPORT_PATH = "C:\\Users\\vzelikova\\airflow\\files\\titanic\\"
#
# # OS_PATH = os.getcwd()  # getcwd() вернет строку - текущий рабочий каталог
# # FULL_PATH = os.path.join(os.getcwd(), PATH_TO_TITANIC)
# # FULL_PATH = os.path.join(os.getcwd(), 'files/titanic', 'test_out2.csv')
# # FULL_PATH = os.path.join(os.getcwd(), '/files/titanic', 'test_out2.csv')
# # FULL_PATH = os.path.join(os.getcwd(), 'test_out2.csv')
# # FULL_PATH = os.path.join(os.getcwd(), 'dags', 'test_out2.csv')
# FULL_PATH = os.path.join(os.getcwd(), 'dags/output', 'test_out2.csv')

INPUT_PATH = os.path.join(os.getcwd(), 'dags/input')
OUTPUT_PATH = os.path.join(os.getcwd(), 'dags/output')


# ------- Функции -------

def import_data(file_name, file_path, **context):
    """Загрузить данные из csv файла и передать в XCOM"""
    path = os.path.join(file_path, file_name)
    # df = pd.read_csv(f"{file_path}{file_name}.csv")
    df = pd.read_csv(path)
    print(df)  # !убрать!

    # Загружаем данные в XCOM переменную для использования в следующих тасках
    ti = context["task_instance"]
    ti.xcom_push(key="data", value=df)


def transform_data(**context):
    """Изменить данные"""

    # Вытаскиваем из XCOM данные, переданные функцией import_data
    ti = context["task_instance"]
    df = ti.xcom_pull(key="data", task_ids="import_data")

    print(df)  # !убрать!
    # transformation

    # Загружаем измененные данные в XCOM
    ti.xcom_push(key="data", value=df)


def export_data(file_name, file_path, **context):
    """Загрузить данные в новый csv файл"""
    # Вытаскиваем из XCOM данные, переданные функцией transform_data
    ti = context["task_instance"]
    df = ti.xcom_pull(key="data", task_ids="transform_data")

    # path = f"{file_path}{file_name}.csv"
    path = os.path.join(file_path, file_name)

    print(df)  # !убрать!
    print(f"Saving to {path}")

    # Выгружаем датафрейм в csv файл с названием file_name, лежащий по пути file_path
    df.to_csv(path, index=False)


# ------- DAG -------

default_args = {
    "owner": "vzelikova",
    "start_date": datetime(2025, 3, 25),   # дата начала от которой следует начинать запускать DAG согласно расписанию
    "retries": 1,    # количество попыток повторить выполнение задачи при ошибке
    # 'run_as_user': 'vzelikova'
}

with DAG(
    # ------- Параметры DAG -------

    "first",
    default_args=default_args,
    description="Пустой даг",
    catchup=False,  # не выполняет запланированные по расписанию запуски DAG, которые находятся раньше текущей даты (если start_date раньше, чем текущая дата)

    schedule=None   # расписание в формате cron - с какой периодичностью будет автоматически выполняться DAG (None = DAG нужно запускать только мануально)
) as dag:
    # ------- Задачи -------

    start = EmptyOperator(
        task_id="start"
    )

    end = EmptyOperator(
        task_id="end"
    )

    import_data_task = PythonOperator(
        task_id="import_data",
        python_callable=import_data,
        op_kwargs={"file_name": "test.csv", "file_path": INPUT_PATH},
        provide_context=True
    )

    transform_data_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
        provide_context=True
    )

    export_data_task = PythonOperator(
        task_id="export_data",
        python_callable=export_data,
        op_kwargs={"file_name": "test_out.csv", "file_path": OUTPUT_PATH},
        provide_context=True
    )

    # ------- Порядок выполнения задач -------
    (
        start
        >> import_data_task
        >> transform_data_task
        >> export_data_task
        >> end
    )

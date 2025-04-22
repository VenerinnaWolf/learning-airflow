from airflow import DAG
# from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
# from airflow.models import Variable  # переменные из настроек Airflow
from datetime import datetime

import pandas as pd
import os.path

# ------- Переменные и константы -------

INPUT_PATH = os.path.join(os.getcwd(), 'dags/first/input')
OUTPUT_PATH = os.path.join(os.getcwd(), 'dags/first/output')


# ------- Функции -------

def import_data(file_name, file_path, **context):
    """Загрузить данные из csv файла и передать в XCOM"""

    # Загружаем данные из csv файла
    path = os.path.join(file_path, file_name)  # полный путь до файла
    df = pd.read_csv(path)

    # Логируем датафрейм, который выгрузили из файла
    print(df)

    # Загружаем данные в XCOM переменную для использования в следующих тасках
    ti = context["task_instance"]
    ti.xcom_push(key="data", value=df)


def transform_data(**context):
    """Изменить данные. Очистить данные от пустых значений и выбрать только мужчин старше 30"""

    # Вытаскиваем из XCOM данные, переданные функцией import_data
    ti = context["task_instance"]
    df = ti.xcom_pull(key="data", task_ids="import_data")

    # Логируем датафрейм, который получили из xcom
    print(df)

    # Трансформируем данные
    df_new = df.dropna()  # очищаем данные от пустых значений
    df_new = df_new[(df_new['Sex'] == 'male') & (df_new['Age'] >= 30)]  # выбираем только мужчин старше 30

    # Логируем измененный датафрейм
    print('Transformed df:')
    print(df_new)

    # Загружаем измененные данные в XCOM
    ti.xcom_push(key="data", value=df_new)


def export_data(file_name, file_path, **context):
    """Загрузить данные в новый csv файл"""

    # Вытаскиваем из XCOM данные, переданные функцией transform_data
    ti = context["task_instance"]
    df = ti.xcom_pull(key="data", task_ids="transform_data")

    path = os.path.join(file_path, file_name)  # полный путь до файла

    # Логируем датафрейм, загружаемый в csv файл
    print(df)
    print(f"Saving to {path}")

    # Выгружаем датафрейм в csv файл
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
    description="Даг простого ETL процесса. Читает данные из csv файла, очищает, обрабатывает и выгружает в новый файл",
    catchup=False,  # не выполняет запланированные по расписанию запуски DAG, которые находятся раньше текущей даты (если start_date раньше, чем текущая дата)
    schedule=None   # расписание в формате cron - с какой периодичностью будет автоматически выполняться DAG (None = DAG нужно запускать только мануально)

) as dag:
    # ------- Задачи -------

    # start = EmptyOperator(
    #     task_id="start"
    # )
    #
    # end = EmptyOperator(
    #     task_id="end"
    # )

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
        import_data_task
        >> transform_data_task
        >> export_data_task
    )

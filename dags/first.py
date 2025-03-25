from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

default_args = {
    "owner": "vzelikova",
    "start_date": datetime(2025, 3, 25),   # дата начала от которой следует начинать запускать DAG согласно расписанию
    "retries": 1                            # количество попыток повторить выполнение задачи при ошибке
}

with DAG(
    # Параметры DAG

    "first",                          # название DAG
    default_args=default_args,              # параметры по умолчанию - присвоим им значение ранее определенной переменной
    description="Пустой даг",  # описание DAG
    catchup=False,                          # не выполняет запланированные по расписанию запуски DAG, которые находятся раньше текущей даты (если start_date раньше, чем текущая дата)
    # schedule="0 0 * * *"
    schedule=None                           # расписание в формате cron - с какой периодичностью будет автоматически выполняться DAG (None = DAG нужно запускать только мануально)
) as dag:

    start = EmptyOperator (
        task_id="start"
    )

    end = EmptyOperator (
        task_id="end"
    )

    (
        start >> end
    )
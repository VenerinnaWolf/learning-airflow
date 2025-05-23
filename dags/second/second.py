from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator

import os
from datetime import datetime, timedelta

import osmnx as ox
import pandas as pd
import geopandas as gpd
import numpy as np
import matplotlib.pyplot as plt
import mapclassify
import folium
from shapely.geometry import Point

# ------- Переменные и константы -------

OUTPUT_PATH = os.path.join(os.getcwd(), 'dags/second/output')
BUILDINGS_PATH = os.path.join(OUTPUT_PATH, 'buildings.geojson')
CITY_GEOMETRY_PATH = os.path.join(OUTPUT_PATH, 'city_geometry.geojson')
HOSPITALS_PATH = os.path.join(OUTPUT_PATH, 'hospitals.geojson')
BUILDINGS_FINAL_PATH = os.path.join(OUTPUT_PATH, 'buildings_final.geojson')
HOSPITALS_FINAL_PATH = os.path.join(OUTPUT_PATH, 'hospitals_final.geojson')
MAP_PATH = os.path.join(OUTPUT_PATH, 'city_map.html')

CITY_NAME = 'France, Orlean'
RADIUS = 500  # радиус, в котором мы считаем дома для загруженности больниц


# ------- Функции -------

def get_city_data(city, buildings_file_path, city_file_path):
    """Получение данных обо всех зданиях в городе `city` и геометрии самого города.
    Сохранение данных в отдельные файлы `buildings_file_path`, `city_file_path`.
    Файлы должны быть в формате .geojson"""

    # Получим геометрию города и данные обо всех зданиях
    city_geometry = ox.geocode_to_gdf(city)
    buildings_gdf = ox.features_from_place(city, {'building': True})

    # Удалим индекс element и перенесем индекс id в качестве обычного столбца.
    buildings_gdf = buildings_gdf.reset_index(level='element', drop=True)
    buildings_gdf = buildings_gdf.reset_index(level='id')

    # Сохраним полученные данные в виде файлов формата .geojson
    city_geometry.to_file(city_file_path)
    buildings_gdf.to_file(buildings_file_path)


def get_hospitals(city, hospitals_file_path):
    """Получение данных о больницах в городе `city` с сохранением в отдельный файл `hospitals_file_path`"""

    # Получим данные о больницах в городе
    hospitals = ox.features_from_place(city, tags={'amenity': 'hospital'})

    # Удалим индекс element и перенесем индекс id в качестве обычного столбца.
    hospitals = hospitals.reset_index(level='element', drop=True)
    hospitals = hospitals.reset_index(level='id')

    # Сохраним полученные данные в виде файлов формата .geojson
    hospitals.to_file(hospitals_file_path)


def count_buildings_near_hospitals(radius, buildings_file_path, hospitals_file_path, output_file_path):
    """Определение количества домов, которые приходятся на каждую больницу в определенном радиусе (в метрах)
    и сохранение в файл `output_file_path`.
    Расстояние считается от точки до точки напрямую"""

    # Загружаем данные из файлов
    buildings_gdf_copy = gpd.read_file(buildings_file_path)
    hospitals_copy = gpd.read_file(hospitals_file_path)

    # Преобразуем CRS в проекцию EPSG:3857 (Web Mercator), которая сохраняет расстояния.
    # Это проекция, которая используется в большинстве веб-карт и она подходит для расчетов расстояний.
    buildings_gdf_copy1 = buildings_gdf_copy.to_crs(epsg=3857)
    hospitals_copy1 = hospitals_copy.to_crs(epsg=3857)

    # Список с количеством зданий для каждой больницы
    buildings_per_hospital = []

    # Переберем все больницы:
    for _, hospital in hospitals_copy1.iterrows():
        # Считаем расстояние до всех зданий
        distances = buildings_gdf_copy1.geometry.distance(hospital.geometry)
        # Считаем количество зданий в радиусе
        buildings_count = (distances <= radius).sum()
        # Сохраняем в список
        buildings_per_hospital.append({
            'id': hospital.id,
            'buildings_count': buildings_count
        })

    # Преобразуем список в датафрейм
    buildings_per_hospital = pd.DataFrame(buildings_per_hospital)

    # Добавим информацию о количестве зданий в изначальный датафрейм больниц
    # Проекцию сохраняем старой.
    hospitals_final = pd.merge(hospitals_copy, buildings_per_hospital, on=['id'])

    # Преобразуем из DataFrame в GeoDataFrame
    hospitals_final = gpd.GeoDataFrame(hospitals_final)

    # Сохраняем файл
    hospitals_final.to_file(output_file_path)


def count_distances_to_hospitals(buildings_file_path, hospitals_file_path, output_file_path):
    """Определение расстояний (в метрах) от каждого дома до ближайшей больницы
    и сохранение в файл `output_file_path`.
    Расстояние считается от точки до точки напрямую"""

    # Загружаем данные из файлов
    buildings_gdf_copy = gpd.read_file(buildings_file_path)
    hospitals_copy = gpd.read_file(hospitals_file_path)

    # Преобразуем CRS в проекцию EPSG:3857 (Web Mercator), которая сохраняет расстояния.
    # Это проекция, которая используется в большинстве веб-карт и она подходит для расчетов расстояний.
    buildings_gdf_copy1 = buildings_gdf_copy.to_crs(epsg=3857)
    hospitals_copy1 = hospitals_copy.to_crs(epsg=3857)

    # Создаем список для хранения расстояний
    distances_to_nearest_hospital = []

    for _, building in buildings_gdf_copy1.iterrows():
        # Вычисляем расстояния до всех больниц
        distances = hospitals_copy1.geometry.distance(building.geometry)
        # Находим минимальное расстояние
        min_distance = distances.min()
        # Находим id больницы, докоторой было минимальное расстояние
        min_idx = distances.idxmin()
        nearest_id = hospitals_copy1.loc[min_idx, 'id']
        # Сохраняем в список
        distances_to_nearest_hospital.append({
            'id': building.id,
            'hospital_id': nearest_id,
            'distance_to_nearest_hospital': min_distance
        })

    # Преобразуем список в датафрейм
    distances_to_nearest_hospital = pd.DataFrame(distances_to_nearest_hospital)

    # Добавим информацию в изначальный датафрейм зданий
    # Проекцию сохраняем старой
    buildings_final = pd.merge(buildings_gdf_copy, distances_to_nearest_hospital, on=['id'])

    # Преобразуем из DataFrame в GeoDataFrame
    buildings_final = gpd.GeoDataFrame(buildings_final)

    # Сохраняем файл
    buildings_final.to_file(output_file_path)


def visualize_map(city_file_path, buildings_file_path, hospitals_file_path, output_file_path):
    """Отрисовка графика (карты) города со зданиями и загрузкой больниц.
    Сохранение карты в файл `output_file_path`"""

    # Загружаем данные из файлов
    city_geometry = gpd.read_file(city_file_path)
    buildings = gpd.read_file(buildings_file_path)
    hospitals = gpd.read_file(hospitals_file_path)

    # Меняем проекцию для файла hospitals на Web Mercator для отображения круга вокруг больниц
    hospitals_proj = hospitals.to_crs(epsg=3857)

    # Визуализируем:
    # 1. Создаем базовую карту города
    f = folium.Figure(width=750, height=500)
    basemap = city_geometry.explore(tooltip=False, color='grey')
    basemap.add_to(f)

    # 2. Добавляем здания на карту
    buildings.explore(tooltip=['distance_to_nearest_hospital'],
                      column='distance_to_nearest_hospital',  # колонка для окраски по тепловой карте
                      cmap='magma_r',  # тепловая карта
                      m=basemap)

    # 3. Добавляем круг вокруг больниц, который определяет радиус в 500 метров
    for _, hospital in hospitals_proj.iterrows():
        # Создаем буфер (координаты должны быть в проекции Web Mercator)
        buffer_geom = hospital.geometry.buffer(500)  # 500 метров

        # Переводим буфер обратно в систему координат, в которой рисует Folium
        buffer_wgs84 = gpd.GeoSeries([buffer_geom], crs='EPSG:3857').to_crs(epsg=4326).iloc[0]

        # Создаем текст подсказки при наведении 
        tooltip = f'{hospital.buildings_count} зданий в радиусе 500 метров от больницы'

        # Добавляем буфер на карту
        folium.GeoJson(buffer_wgs84, tooltip=tooltip, style_function=lambda x: {
            'fillColor': 'blue',
            'color': 'blue',
            'fillOpacity': 0.4,
            'weight': 1,
        }).add_to(basemap)

    # 4. Добавляем больницы
    hospitals.explore(tooltip=['name', 'id'], color='red', m=basemap)

    # Сохраняем карту в файл
    basemap.save(output_file_path)


# ------- DAG -------

default_args = {
    "owner": "vzelikova",
    "start_date": datetime(2025, 5, 20),   # дата начала от которой следует начинать запускать DAG согласно расписанию
    "retries": 1,    # количество попыток повторить выполнение задачи при ошибке
}

with DAG(
    # ------- Параметры DAG -------

    "second",
    default_args=default_args,
    description="Вычисление загруженности больниц в городе и построение соответствующего графика.",
    catchup=False,  # не выполняет запланированные по расписанию запуски DAG, которые находятся раньше текущей даты (если start_date раньше, чем текущая дата)
    schedule=None   # расписание в формате cron - с какой периодичностью будет автоматически выполняться DAG (None = DAG нужно запускать только мануально)

) as dag:
    # ------- Задачи -------

    # OUTPUT_PATH = os.path.join(os.getcwd(), 'dags/second/output')
    # BUILDINGS_PATH = os.path.join(OUTPUT_PATH, 'buildings.geojson')
    # CITY_GEOMETRY_PATH = os.path.join(OUTPUT_PATH, 'city_geometry.geojson')
    # HOSPITALS_PATH = os.path.join(OUTPUT_PATH, 'hospitals.geojson')
    # BUILDINGS_FINAL_PATH = os.path.join(OUTPUT_PATH, 'buildings_final.geojson')
    # HOSPITALS_FINAL_PATH = os.path.join(OUTPUT_PATH, 'hospitals_final.geojson')
    # MAP_PATH = os.path.join(OUTPUT_PATH, 'city_map.html')

    # CITY_NAME = 'France, Orlean'

    get_city_data_task = PythonOperator(
        task_id="get_city_data",
        execution_timeout=timedelta(minutes=30),
        python_callable=get_city_data,
        op_kwargs={"city": CITY_NAME,
                   "buildings_file_path": BUILDINGS_PATH,
                   "city_file_path": CITY_GEOMETRY_PATH}
    )

    get_hospitals_task = PythonOperator(
        task_id="get_hospitals",
        execution_timeout=timedelta(minutes=30),
        python_callable=get_hospitals,
        op_kwargs={"city": CITY_NAME,
                   "hospitals_file_path": HOSPITALS_PATH}
    )

    count_buildings_task = PythonOperator(
        task_id="count_buildings_near_hospitals",
        python_callable=count_buildings_near_hospitals,
        op_kwargs={"radius": RADIUS,
                   "buildings_file_path": BUILDINGS_PATH,
                   "hospitals_file_path": HOSPITALS_PATH,
                   "output_file_path": HOSPITALS_FINAL_PATH}
    )

    count_distances_task = PythonOperator(
        task_id="count_distances_to_hospitals",
        python_callable=count_distances_to_hospitals,
        op_kwargs={"buildings_file_path": BUILDINGS_PATH,
                   "hospitals_file_path": HOSPITALS_PATH,
                   "output_file_path": BUILDINGS_FINAL_PATH}
    )

    visualize_map_task = PythonOperator(
        task_id="visualize_map",
        execution_timeout=timedelta(minutes=30),
        python_callable=visualize_map,
        op_kwargs={"city_file_path": CITY_GEOMETRY_PATH,
                   "buildings_file_path": BUILDINGS_FINAL_PATH,
                   "hospitals_file_path": HOSPITALS_FINAL_PATH,
                   "output_file_path": MAP_PATH}
    )

    # Операторы-разделители

    start = EmptyOperator(
        task_id="start"
    )

    split = EmptyOperator(
        task_id="split"
    )

    # ------- Порядок выполнения задач -------
    (
        start
        >> [get_city_data_task, get_hospitals_task]
        >> split
        >> [count_buildings_task, count_distances_task]
        >> visualize_map_task
    )

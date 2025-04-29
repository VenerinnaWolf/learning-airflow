import pandas as pd


def fill_nan(df: pd.DataFrame) -> pd.DataFrame:
    """
    Заменяет пустые значения в целочисленных столбцах датафрейма
    на среднее значение по столбцу.
    """
    # Скопируем изначальный датафрейм
    df_copy = df.copy()
    df_copy

    # Ищем целочисленные столбцы
    numeric_cols = df_copy.select_dtypes('number').columns
    numeric_cols

    # Заполняем средними значениями для каждого столбца
    df_copy[numeric_cols] = df_copy[numeric_cols].fillna(
        df_copy[numeric_cols].mean())
    # Вместо функции `mean` можно использовать, например,
    # `median` или `mode` в зависимости от того, что больше подходит.

    return df_copy


def fill_nan_grouped(df: pd.DataFrame, group_cols: list[str]) -> pd.DataFrame:
    """
    Заменяет пустые значения в целочисленных столбцах датафрейма на среднее значение,
    используя группировку по дополнительным столбцам `group_cols`.
    """
    # Скопируем изначальный датафрейм
    df_copy = df.copy()
    df_copy

    # Ищем целочисленные столбцы
    numeric_cols = df_copy.select_dtypes('number').columns
    numeric_cols

    # Заполняем средними значениями для каждого столбца, используя группировку
    df_copy[numeric_cols] = df_copy[numeric_cols].fillna(
        df_copy.groupby(group_cols)[numeric_cols].transform('mean'))

    return df_copy


if __name__ == '__main__':
    data = {
        'name': ['Xavier', 'Ann', 'Jana', 'Yi', 'Robin', 'Amal', 'Nori'],
        'city': ['Mexico City', 'Toronto', 'Prague', 'Shanghai', 'Manchester', 'Cairo', 'Osaka'],
        'sex': ['M', 'F', 'F', 'M', 'F', 'M', 'M'],
        'age': [41, None, 33, 34, None, 31, 37],
        'height': [178, 164, 172, None, None, 180, 195],
        'weight': [80, 52, None, 61, 62, 73, None],
        'score': [None, None, None, None, None, None, None],
    }

    df = pd.DataFrame(data)
    filled_df = fill_nan(df)
    filled_grouped_df = fill_nan_grouped(df, ['sex'])
    print('Заполнение пустых значений без группировки:')
    print(filled_df)
    print('\nЗаполнение пустых значений с группировкой по полу:')
    print(filled_grouped_df)

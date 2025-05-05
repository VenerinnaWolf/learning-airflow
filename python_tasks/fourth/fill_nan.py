import pandas as pd


def fill_nan(df: pd.DataFrame, group_cols: list[str] = None) -> pd.DataFrame:
    """
    Заменяет пустые значения в целочисленных столбцах датафрейма
    на среднее значение по столбцу.
    Дополнительно можно передать столбцы для группировки.
    """
    # Скопируем изначальный датафрейм
    df_copy = df.copy()

    # Ищем целочисленные столбцы
    numeric_cols = df_copy.select_dtypes('number').columns

    # Заполняем средними значениями для каждого столбца
    if group_cols is None:  # без группировки
        df_copy[numeric_cols] = df_copy[numeric_cols].fillna(
            df_copy[numeric_cols].mean())
    else:  # с группировкой
        df_copy[numeric_cols] = df_copy[numeric_cols].fillna(
            df_copy.groupby(group_cols)[numeric_cols].transform('mean'))
    # Вместо функции `mean` можно использовать, например,
    # `median` или `mode` в зависимости от того, что больше подходит.

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
    filled_grouped_df = fill_nan(df, ['sex'])

    print('Изначальный датафрейм:')
    print(df)
    print('\nЗаполнение пустых значений без группировки:')
    print(filled_df)
    print('\nЗаполнение пустых значений с группировкой по полу:')
    print(filled_grouped_df)

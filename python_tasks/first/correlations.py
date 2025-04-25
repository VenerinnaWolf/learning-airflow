import pandas as pd


def get_highest_correlation(df: pd.DataFrame) -> tuple:
    """Возвращает имена двух столбцов c наибольшей положительной корреляцией для датафрейма"""
    # Вычисляем корреляционную матрицу по методу Пирсона:
    corr_mat = df.corr()

    # Запишем все корреляции из нижнего треугольника матрицы (с названиями строк и столбцов)
    corrs = [
        (corr_mat.iloc[row, col], corr_mat.columns[row], corr_mat.columns[col])
        for row in range(corr_mat.shape[0]) for col in range(row)  # нижний треугольник
    ]

    # Найдем наибольший элемент из полученных корреляций и возвращаем только названия столбцов:
    return max(corrs, key=lambda x: x[0])[1:]


if __name__ == "__main__":
    data = {
        'Region': [2, 1, 3, 1],
        'Sales': [200, 150, 300, 250],
        'Advertising': [50, 40, 70, 60]
    }
    df = pd.DataFrame(data)
    print(get_highest_correlation(df))

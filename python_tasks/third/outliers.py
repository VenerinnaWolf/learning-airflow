import numpy as np


def find_outliers(my_list: list[int | float]) -> list[int | float]:
    """Возвращает из списка все значения, которые являются выбросами"""
    # Сортируем список
    my_list = sorted(my_list)

    # Считаем квартили и межквартильный размах
    q1 = np.quantile(my_list, 0.25)  # первый квартиль
    q3 = np.quantile(my_list, 0.75)  # третий квартиль
    iqr = q3-q1  # межквартильный размах

    # Считаем выбросы
    outliers = []
    for i in my_list:
        if i < (q1 - 1.5 * iqr) or i > (q3 + 1.5 * iqr):
            outliers.append(i)

    return outliers


if __name__ == "__main__":
    my_list = [1, 4, 1, -61, 2, 3, 3, 4, 5, 5, 6, 100]
    print(find_outliers(my_list))

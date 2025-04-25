def count_rows_in_file(file_path: str) -> int:
    count = 0
    with open(file_path, 'r', encoding='utf-8') as file:
        for line in file:
            count += 1
    return count


if __name__ == '__main__':
    file_path = 'input.csv'
    print(count_rows_in_file(file_path))

import os
from primitive_db.utils import convert_value, DATA_DIR

ALLOWED_TYPES = {'int', 'str', 'bool'}

def create_table(metadata, table_name, columns):
    """Создаёт новую таблицу (без изменений)."""
    if table_name in metadata:
        print(f'Ошибка: Таблица "{table_name}" уже существует.')
        return False

    table_columns = [('ID', 'int')]
    for col in columns:
        if ':' not in col:
            print(f'Некорректное значение: {col}. Должно быть в формате имя:тип')
            return False
        name, typ = col.split(':', 1)
        if typ not in ALLOWED_TYPES:
            print(f'Некорректное значение: {col}. Тип должен быть int, str или bool')
            return False
        table_columns.append((name, typ))

    metadata[table_name] = table_columns
    print(f'Таблица "{table_name}" успешно создана со столбцами: ' +
          ', '.join(f'{name}:{typ}' for name, typ in table_columns))
    return True

def drop_table(metadata, table_name):
    """Удаляет таблицу и соответствующий файл данных."""
    if table_name not in metadata:
        print(f'Ошибка: Таблица "{table_name}" не существует.')
        return False
    del metadata[table_name]
    # Удаляем файл данных, если он существует
    filepath = os.path.join(DATA_DIR, f'{table_name}.json')
    if os.path.exists(filepath):
        os.remove(filepath)
    print(f'Таблица "{table_name}" успешно удалена.')
    return True

def list_tables(metadata):
    if not metadata:
        print('Нет таблиц.')
    else:
        for table in metadata:
            print(f'- {table}')

def _validate_values(metadata, table_name, values):
    """
    Проверяет количество и типы значений для вставки.
    Возвращает список значений, преобразованных к нужным типам.
    """
    if table_name not in metadata:
        raise ValueError(f"Таблица '{table_name}' не существует")

    columns = metadata[table_name]  # список кортежей (имя, тип)
    expected_count = len(columns) - 1  # без ID
    if len(values) != expected_count:
        raise ValueError(f"Неверное количество значений. Ожидалось {expected_count}, получено {len(values)}")

    converted = []
    for i, val_str in enumerate(values):
        col_name, col_type = columns[i+1]  # пропускаем ID
        try:
            converted.append(convert_value(val_str, col_type))
        except ValueError as e:
            raise ValueError(f"Ошибка в столбце '{col_name}': {e}")
    return converted

def insert(metadata, table_name, values):
    """
    values - список строк (значения без ID)
    Возвращает обновленные данные таблицы (список записей)
    """
    from primitive_db.utils import load_table_data
    converted = _validate_values(metadata, table_name, values)
    data = load_table_data(table_name)

    if data:
        new_id = max(rec['ID'] for rec in data) + 1
    else:
        new_id = 1

    columns = metadata[table_name]
    new_record = {'ID': new_id}
    for i, (col_name, _) in enumerate(columns[1:]):
        new_record[col_name] = converted[i]

    data.append(new_record)
    return data

def select(table_data, where_clause=None):
    """
    where_clause: словарь {column: value} или None
    Возвращает отфильтрованный список записей.
    """
    if not where_clause:
        return table_data
    result = []
    for record in table_data:
        match = all(record.get(k) == v for k, v in where_clause.items())
        if match:
            result.append(record)
    return result

def update(table_data, set_clause, where_clause):
    """
    set_clause: {column: new_value}
    where_clause: {column: value}
    Обновляет записи, удовлетворяющие условию.
    Возвращает обновлённые данные.
    """
    updated = 0
    for record in table_data:
        if all(record.get(k) == v for k, v in where_clause.items()):
            for col, new_val in set_clause.items():
                record[col] = new_val
            updated += 1
    if updated == 0:
        print("Нет записей, удовлетворяющих условию.")
    else:
        print(f"Обновлено записей: {updated}")
    return table_data

def delete(table_data, where_clause):
    """
    Удаляет записи, удовлетворяющие условию.
    Возвращает обновлённые данные.
    """
    new_data = []
    deleted = 0
    for record in table_data:
        if all(record.get(k) == v for k, v in where_clause.items()):
            deleted += 1
        else:
            new_data.append(record)
    if deleted == 0:
        print("Нет записей, удовлетворяющих условию.")
    else:
        print(f"Удалено записей: {deleted}")
    return new_data

def info(metadata, table_name, table_data):
    """Выводит информацию о таблице."""
    if table_name not in metadata:
        print(f"Таблица '{table_name}' не существует.")
        return
    columns = metadata[table_name]
    col_str = ', '.join(f'{name}:{typ}' for name, typ in columns)
    print(f"Таблица: {table_name}")
    print(f"Столбцы: {col_str}")
    print(f"Количество записей: {len(table_data)}")
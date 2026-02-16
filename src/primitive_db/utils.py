import json
import os

METADATA_FILE = 'db_meta.json'
DATA_DIR = 'data'

def ensure_data_dir():
    """Создаёт папку data, если её нет."""
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

def load_metadata(filepath=METADATA_FILE):
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return {}
    except json.JSONDecodeError:
        return {}

def save_metadata(data, filepath=METADATA_FILE):
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

def load_table_data(table_name):
    """Загружает данные таблицы из файла data/<table_name>.json."""
    ensure_data_dir()
    filepath = os.path.join(DATA_DIR, f'{table_name}.json')
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except FileNotFoundError:
        return []
    except json.JSONDecodeError:
        return []

def save_table_data(table_name, data):
    """Сохраняет данные таблицы в файл data/<table_name>.json."""
    ensure_data_dir()
    filepath = os.path.join(DATA_DIR, f'{table_name}.json')
    with open(filepath, 'w', encoding='utf-8') as f:
        json.dump(data, f, indent=4, ensure_ascii=False)

def convert_value(value_str, target_type):
    """Преобразует строку в нужный тип (int, bool, str)."""
    if target_type == 'int':
        try:
            return int(value_str)
        except ValueError:
            raise ValueError(f"Не удалось преобразовать '{value_str}' в int")
    elif target_type == 'bool':
        lower = value_str.lower()
        if lower in ('true', '1', 'yes', 'да'):
            return True
        elif lower in ('false', '0', 'no', 'нет'):
            return False
        else:
            raise ValueError(f"Не удалось преобразовать '{value_str}' в bool")
    elif target_type == 'str':
        # Убираем внешние кавычки, если они есть
        if value_str.startswith(('"', "'")) and value_str.endswith(('"', "'")):
            return value_str[1:-1]
        return str(value_str)
    else:
        raise ValueError(f"Неизвестный тип {target_type}")
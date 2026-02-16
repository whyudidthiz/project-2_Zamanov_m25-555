import shlex
from primitive_db import core
from primitive_db.utils import (
    load_metadata, save_metadata,
    load_table_data, save_table_data,
    convert_value
)
from primitive_db.parser import parse_where_condition, parse_set_condition, extract_values
from prettytable import PrettyTable

META_FILE = 'db_meta.json'

def print_help():
    print('***Операции с данными***')
    print('Функции:')
    print('<command> insert into <имя_таблицы> values (<значение1>, <значение2>, ...) - создать запись.')
    print('<command> select from <имя_таблицы> where <столбец> = <значение> - прочитать записи по условию.')
    print('<command> select from <имя_таблицы> - прочитать все записи.')
    print('<command> update <имя_таблицы> set <столбец1> = <новое_значение1> where <столбец_условия> = <значение_условия> - обновить запись.')
    print('<command> delete from <имя_таблицы> where <столбец> = <значение> - удалить запись.')
    print('<command> info <имя_таблицы> - вывести информацию о таблице.')
    print('<command> exit - выход из программы')
    print('<command> help- справочная информация')

def display_table(records, columns):
    """Выводит записи в виде prettytable."""
    if not records:
        print("Нет записей.")
        return
    table = PrettyTable()
    field_names = [col for col, _ in columns]
    table.field_names = field_names
    for rec in records:
        row = [rec.get(col, '') for col in field_names]
        table.add_row(row)
    print(table)

def run():
    print('***База данных***')
    print_help()
    while True:
        try:
            user_input = input('>>>Введите команду: ').strip()
            if not user_input:
                continue

            tokens = shlex.split(user_input)
            if not tokens:
                continue
            command = tokens[0].lower()

            metadata = load_metadata(META_FILE)

            if command == 'exit':
                print('Выход из программы.')
                break
            elif command == 'help':
                print_help()
            elif command == 'create_table':
                if len(tokens) < 3:
                    print('Ошибка: недостаточно аргументов. Используйте: create_table <имя_таблицы> <столбец1:тип> ...')
                    continue
                table_name = tokens[1]
                columns = tokens[2:]
                if core.create_table(metadata, table_name, columns):
                    save_metadata(metadata, META_FILE)
            elif command == 'drop_table':
                if len(tokens) != 2:
                    print('Ошибка: нужно указать имя таблицы. Используйте: drop_table <имя_таблицы>')
                    continue
                table_name = tokens[1]
                if core.drop_table(metadata, table_name):
                    save_metadata(metadata, META_FILE)
            elif command == 'list_tables':
                core.list_tables(metadata)
            elif command == 'insert':
                if len(tokens) < 5 or tokens[1].lower() != 'into':
                    print('Ошибка синтаксиса. Используйте: insert into <имя_таблицы> values (значения)')
                    continue
                table_name = tokens[2]
                if tokens[3].lower() != 'values':
                    print('Ошибка: ожидалось "values"')
                    continue
                value_tokens = tokens[4:]
                values = extract_values(value_tokens)
                try:
                    new_data = core.insert(metadata, table_name, values)
                    save_table_data(table_name, new_data)
                    last_id = new_data[-1]['ID'] if new_data else None
                    print(f'Запись с ID={last_id} успешно добавлена в таблицу "{table_name}".')
                except Exception as e:
                    print(f'Ошибка: {e}')
            elif command == 'select':
                if len(tokens) < 3 or tokens[1].lower() != 'from':
                    print('Ошибка синтаксиса. Используйте: select from <имя_таблицы> [where условие]')
                    continue
                table_name = tokens[2]
                if table_name not in metadata:
                    print(f'Таблица "{table_name}" не существует.')
                    continue
                where_clause = None
                if len(tokens) > 3 and tokens[3].lower() == 'where':
                    try:
                        where_clause, _ = parse_where_condition(tokens[3:])
                        col_name = list(where_clause.keys())[0]
                        # Определяем тип столбца
                        col_type = next((ct for cn, ct in metadata[table_name] if cn == col_name), None)
                        if col_type is None:
                            print(f'Столбец "{col_name}" не существует.')
                            continue
                        where_clause[col_name] = convert_value(where_clause[col_name], col_type)
                    except Exception as e:
                        print(f'Ошибка в условии where: {e}')
                        continue
                data = load_table_data(table_name)
                records = core.select(data, where_clause)
                display_table(records, metadata[table_name])
            elif command == 'update':
                if len(tokens) < 7:
                    print('Ошибка синтаксиса. Используйте: update <имя_таблицы> set <столбец> = <значение> where <столбец> = <значение>')
                    continue
                table_name = tokens[1]
                if table_name not in metadata:
                    print(f'Таблица "{table_name}" не существует.')
                    continue
                try:
                    set_idx = tokens.index('set')
                    where_idx = tokens.index('where')
                except ValueError:
                    print('Ошибка: ожидались ключевые слова "set" и "where"')
                    continue
                # Разбор set
                set_tokens = tokens[set_idx:where_idx]
                set_clause, _ = parse_set_condition(set_tokens)
                # Разбор where
                where_tokens = tokens[where_idx:]
                where_clause, _ = parse_where_condition(where_tokens)
                # Преобразование типов для set и where
                try:
                    # Для set
                    for col, val_str in set_clause.items():
                        col_type = next((ct for cn, ct in metadata[table_name] if cn == col), None)
                        if col_type is None:
                            print(f'Столбец "{col}" не существует.')
                            continue
                        set_clause[col] = convert_value(val_str, col_type)
                    # Для where
                    for col, val_str in where_clause.items():
                        col_type = next((ct for cn, ct in metadata[table_name] if cn == col), None)
                        if col_type is None:
                            print(f'Столбец "{col}" не существует.')
                            continue
                        where_clause[col] = convert_value(val_str, col_type)
                except Exception as e:
                    print(f'Ошибка преобразования типов: {e}')
                    continue

                data = load_table_data(table_name)
                data = core.update(data, set_clause, where_clause)
                save_table_data(table_name, data)
            elif command == 'delete':
                if len(tokens) < 5 or tokens[1].lower() != 'from':
                    print('Ошибка синтаксиса. Используйте: delete from <имя_таблицы> where <столбец> = <значение>')
                    continue
                table_name = tokens[2]
                if table_name not in metadata:
                    print(f'Таблица "{table_name}" не существует.')
                    continue
                if tokens[3].lower() != 'where':
                    print('Ошибка: ожидалось "where"')
                    continue
                try:
                    where_clause, _ = parse_where_condition(tokens[3:])
                    col_name = list(where_clause.keys())[0]
                    col_type = next((ct for cn, ct in metadata[table_name] if cn == col_name), None)
                    if col_type is None:
                        print(f'Столбец "{col_name}" не существует.')
                        continue
                    where_clause[col_name] = convert_value(where_clause[col_name], col_type)
                except Exception as e:
                    print(f'Ошибка в условии where: {e}')
                    continue
                data = load_table_data(table_name)
                data = core.delete(data, where_clause)
                save_table_data(table_name, data)
            elif command == 'info':
                if len(tokens) != 2:
                    print('Ошибка: нужно указать имя таблицы. Используйте: info <имя_таблицы>')
                    continue
                table_name = tokens[1]
                data = load_table_data(table_name)
                core.info(metadata, table_name, data)
            else:
                print(f'Функции "{command}" нет. Попробуйте снова.')
        except KeyboardInterrupt:
            print('\nВыход из программы.')
            break
        except Exception as e:
            print(f'Ошибка: {e}')
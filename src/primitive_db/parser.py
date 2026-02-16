def parse_where_condition(tokens):
    """
    Принимает список токенов, начиная с 'where'.
    Возвращает {column: value} и оставшиеся токены.
    """
    if len(tokens) < 4 or tokens[0].lower() != 'where':
        raise ValueError("Ожидалось 'where' и три токена")
    if tokens[2] != '=':
        raise ValueError("Ожидался символ '='")
    column = tokens[1]
    value = tokens[3]
    return {column: value}, tokens[4:]

def parse_set_condition(tokens):
    """
    Принимает список токенов, начиная с 'set'.
    Возвращает {column: value} и оставшиеся токены.
    """
    if len(tokens) < 4 or tokens[0].lower() != 'set':
        raise ValueError("Ожидалось 'set' и три токена")
    if tokens[2] != '=':
        raise ValueError("Ожидался символ '='")
    column = tokens[1]
    value = tokens[3]
    return {column: value}, tokens[4:]

def extract_values(tokens):
    """
    Извлекает значения из списка токенов после 'values',
    удаляя скобки и запятые.
    """
    values = []
    for token in tokens:
        if token not in ('(', ')', ','):
            values.append(token)
    return values
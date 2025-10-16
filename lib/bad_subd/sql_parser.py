import re
from typing import List, Dict, Any, Tuple
from .engine import BadSUBDEngine

class SQLParser:
    """Парсер SQL запросов для BadSUBD"""
    
    def __init__(self, engine: BadSUBDEngine):
        self.engine = engine
    
    def execute(self, sql: str) -> Any:
        """Выполнить SQL запрос"""
        sql = sql.strip().replace('\n', ' ').replace('\t', ' ')
        

        if sql.upper().startswith('CREATE TABLE'):
            return self._parse_create_table(sql)
        elif sql.upper().startswith('INSERT INTO'):
            return self._parse_insert(sql)
        elif sql.upper().startswith('SELECT'):
            return self._parse_select(sql)
        elif sql.upper().startswith('DELETE'):
            return self._parse_delete(sql)
        else:
            raise ValueError(f"Unsupported SQL statement: {sql}")
    
    def _parse_create_table(self, sql: str) -> bool:
        """Парсинг CREATE TABLE"""
        
        sql = re.sub(r'CREATE TABLE\s+', '', sql, flags=re.IGNORECASE)
        
        # Извлекаем имя таблицы
        table_match = re.match(r'(\w+)\s*\((.*)\)', sql, re.IGNORECASE | re.DOTALL)
        if not table_match:
            raise ValueError("Invalid CREATE TABLE syntax")
        
        table_name = table_match.group(1)
        columns_str = table_match.group(2)
        
        # Парсим колонки
        columns = self._parse_columns(columns_str)
        
        # Создаем таблицу через движок
        self.engine.create_table(table_name, columns)
        return True
    
    def _parse_columns(self, columns_str: str) -> List[Dict[str, Any]]:
        """Парсинг определений колонок"""
        columns = []
        # Улучшенный парсинг колонок с учетом скобок
        column_defs = []
        current_col = ""
        paren_depth = 0
        
        for char in columns_str:
            if char == '(':
                paren_depth += 1
            elif char == ')':
                paren_depth -= 1
            elif char == ',' and paren_depth == 0:
                if current_col.strip():
                    column_defs.append(current_col.strip())
                current_col = ""
                continue
            current_col += char
        
        if current_col.strip():
            column_defs.append(current_col.strip())
        
        for col_def in column_defs:
            if not col_def:
                continue
                
            # Разбираем определение колонки
            parts = col_def.split()
            if len(parts) < 2:
                raise ValueError(f"Invalid column definition: {col_def}")
            
            col_name = parts[0]
            col_type = parts[1].upper()
            
            if col_type == 'INT':
                columns.append({
                    'name': col_name,
                    'type': 'INT'
                })
            elif col_type.startswith('VARCHAR'):
                # Извлекаем размер из скобок VARCHAR(50)
                size_match = re.search(r'\((\d+)\)', col_def)
                if size_match:
                    size = int(size_match.group(1))
                else:
                    size = 255  
                
                columns.append({
                    'name': col_name,
                    'type': 'VARCHAR',
                    'size': size
                })
            else:
                raise ValueError(f"Unsupported data type: {col_type}")
        
        return columns
    
    def _parse_insert(self, sql: str) -> bool:
        """Парсинг INSERT INTO"""
        # Убираем INSERT INTO
        sql = re.sub(r'INSERT INTO\s+', '', sql, flags=re.IGNORECASE)
        
        # Извлекаем имя таблицы и значения
        pattern = r'(\w+)\s*(?:\(([^)]+)\))?\s*VALUES\s*\(([^)]+)\)'
        match = re.search(pattern, sql, re.IGNORECASE | re.DOTALL)
        if not match:
            raise ValueError("Invalid INSERT syntax")
        
        table_name = match.group(1)
        columns_str = match.group(2)
        values_str = match.group(3)
        
        # Парсим значения
        values = self._parse_values(values_str)
        
        # Если указаны конкретные колонки
        if columns_str:
            column_names = [col.strip() for col in columns_str.split(',')]
            if len(column_names) != len(values):
                raise ValueError("Number of columns doesn't match number of values")
            row_data = dict(zip(column_names, values))
        else:
            # Используем порядок колонок из схемы
            try:
                table_info = self.engine.get_table_info(table_name)
                column_names = [col['name'] for col in table_info['columns']]
                if len(column_names) != len(values):
                    raise ValueError("Number of values doesn't match table schema")
                row_data = dict(zip(column_names, values))
            except Exception as e:
                raise ValueError(f"Cannot determine column order for table {table_name}: {e}")
        
        # Вставляем данные
        return self.engine.insert(table_name, row_data)
    
    def _parse_values(self, values_str: str) -> List[Any]:
        """Парсинг значений для INSERT"""
        values = []
        current_value = ""
        in_quotes = False
        quote_char = None
        paren_depth = 0
        
        for char in values_str:
            if char in ['"', "'"] and not in_quotes:
                in_quotes = True
                quote_char = char
            elif char == quote_char and in_quotes:
                in_quotes = False
            elif char == '(':
                paren_depth += 1
            elif char == ')':
                paren_depth -= 1
            elif char == ',' and not in_quotes and paren_depth == 0:
                if current_value.strip():
                    values.append(self._convert_value(current_value.strip()))
                current_value = ""
                continue
            
            current_value += char
        
        # Добавляем последнее значение
        if current_value.strip():
            values.append(self._convert_value(current_value.strip()))
        
        return values
    
    def _convert_value(self, value: str) -> Any:
        """Конвертация строкового значения в правильный тип"""
        value = value.strip()
        
        # Строки в кавычках
        if (value.startswith("'") and value.endswith("'")) or (value.startswith('"') and value.endswith('"')):
            return value[1:-1]  # Убираем кавычки
        # Числа
        elif value.isdigit():
            return int(value)
        # NULL значение
        elif value.upper() == 'NULL':
            return None
        else:
            # Попробуем конвертировать в число, если возможно
            try:
                return int(value)
            except ValueError:
                return value
    
    def _parse_select(self, sql: str) -> List[Dict[str, Any]]:
        """Парсинг SELECT"""
        # Убираем SELECT
        sql = re.sub(r'SELECT\s+', '', sql, flags=re.IGNORECASE)
        
        # Извлекаем колонки
        from_match = re.search(r'FROM\s+(\w+)(?:\s+WHERE\s+(.*))?$', sql, re.IGNORECASE)
        if not from_match:
            raise ValueError("Invalid SELECT syntax")
        
        columns_str = sql.split('FROM')[0].strip()
        table_name = from_match.group(1)
        where_clause = from_match.group(2)
        
        # Определяем колонки
        if columns_str == '*':
            columns = None
        else:
            columns = [col.strip() for col in columns_str.split(',')]
        
        # Парсим WHERE условие
        where_condition = self._parse_where(where_clause) if where_clause else None
        
        # Выполняем запрос
        return self.engine.select(table_name, columns, where_condition)
    
    def _parse_where(self, where_clause: str) -> Dict[str, Any]:
        """Парсинг WHERE условия"""
        conditions = {}
        
        if not where_clause:
            return conditions
        
        # Простая поддержка условий равенства
        pattern = r'(\w+)\s*=\s*([^\s,]+)'
        matches = re.findall(pattern, where_clause)
        
        for col_name, value in matches:
            conditions[col_name] = self._convert_value(value)
        
        return conditions
    
    def _parse_delete(self, sql: str) -> int:
        """Парсинг DELETE"""
        # Убираем DELETE
        sql = re.sub(r'DELETE\s+', '', sql, flags=re.IGNORECASE)
        
        if sql.upper().startswith('* FROM'):
            # DELETE * FROM table_name
            table_match = re.search(r'FROM\s+(\w+)(?:\s+WHERE\s+(.*))?$', sql, re.IGNORECASE)
            if not table_match:
                raise ValueError("Invalid DELETE syntax")
            
            table_name = table_match.group(1)
            where_clause = table_match.group(2)
            
            if where_clause:
                where_condition = self._parse_where(where_clause)
                return self.engine.delete(table_name, where_condition)
            else:
                return self.engine.delete(table_name)
        else:
            # DELETE FROM table_name WHERE ...
            from_match = re.search(r'FROM\s+(\w+)(?:\s+WHERE\s+(.*))?$', sql, re.IGNORECASE)
            if not from_match:
                raise ValueError("Invalid DELETE syntax")
            
            table_name = from_match.group(1)
            where_clause = from_match.group(2)
            
            if where_clause:
                where_condition = self._parse_where(where_clause)
                return self.engine.delete(table_name, where_condition)
            else:
                return self.engine.delete(table_name)
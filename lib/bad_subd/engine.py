import os
from typing import List, Dict, Any, Optional
from .schema import TableSchema, SchemaManager, ColumnDefinition
from .storage import UTF32RowStorage
from .index import SimpleHashIndex
from .table_file import TableFileManager
from .config import bad_subd_config

class BadSUBDEngine:
    """Движок собственной СУБД с UTF-32 хранением"""
    
    def __init__(self, base_path: str = None):
        self.base_path = base_path or bad_subd_config.BASE_DATA_DIR
        self.schema_manager = SchemaManager()
        self.table_manager = TableFileManager()
        self.indexes: Dict[str, Dict[str, SimpleHashIndex]] = {}
        self.storages: Dict[str, UTF32RowStorage] = {}
    
    def create_table(self, table_name: str, columns: List[Dict]) -> None:
        """Создание таблицы с указанием размера VARCHAR"""
        if self.schema_manager.schema_exists(table_name):
            raise ValueError(f"Table {table_name} already exists")
        
        # Создаем схему с указанием размера VARCHAR
        column_defs = []
        for col in columns:
            if col['type'] == 'INT':
                column_defs.append(ColumnDefinition(col['name'], 'INT'))
            elif col['type'] == 'VARCHAR':
                size = col.get('size', bad_subd_config.MAX_VARCHAR_SIZE)
                column_defs.append(ColumnDefinition(col['name'], 'VARCHAR', size))
            else:
                raise ValueError(f"Unsupported data type: {col['type']}")
        
        schema = TableSchema(table_name, column_defs)
        self.schema_manager.save_schema(schema)
        
        # Создаем хранилище
        self.storages[table_name] = UTF32RowStorage(schema.columns)
        
        # Создаем файл таблицы
        self.table_manager.create_table_file(schema)
        
        print(f"Table '{table_name}' created with UTF-32 storage")
        print(f"Row size: {self.storages[table_name].row_size} bytes")
    
    def create_index(self, table_name: str, column_name: str) -> None:
        """Создание индекса для числовой колонки"""
        schema = self.schema_manager.load_schema(table_name)
        column = next((c for c in schema.columns if c.name == column_name), None)
        
        if not column:
            raise ValueError(f"Column {column_name} not found in table {table_name}")
        if column.data_type != 'INT':
            raise ValueError("Indexes only supported for INT columns")
        
        if table_name not in self.indexes:
            self.indexes[table_name] = {}
        
        index = SimpleHashIndex(table_name, column_name)
        self.indexes[table_name][column_name] = index
        
        # Построение индекса для существующих данных
        storage = self._get_storage(table_name)
        for row in self.table_manager.scan_rows(table_name, storage):
            if column_name in row:
                index.insert(row[column_name], row['_position'])
        
        print(f"Index created on {table_name}.{column_name}")
    
    def insert(self, table_name: str, values: Dict[str, Any]) -> bool:
        """Вставка данных в таблицу"""
        try:
            storage = self._get_storage(table_name)
            
            # Вставка
            position = self.table_manager.insert_row(table_name, values, storage)
            
            # Обновление индексов
            if table_name in self.indexes:
                for col_name, index in self.indexes[table_name].items():
                    if col_name in values:
                        index.insert(values[col_name], position)
            
            return True
            
        except Exception as e:
            print(f"Insert failed: {e}")
            return False
    
    def select(self, table_name: str, columns: List[str] = None, where: Dict = None) -> List[Dict[str, Any]]:
        """Выборка данных из таблицы"""
        storage = self._get_storage(table_name)
        results = []
        
        # Используем индекс если есть подходящий
        if where and table_name in self.indexes:
            for col_name, value in where.items():
                if col_name in self.indexes[table_name]:
                    positions = self.indexes[table_name][col_name].find(value)
                    for pos in positions:
                        try:
                            row = self.table_manager.read_row_at_position(table_name, pos, storage)
                            if self._matches_where(row, where):
                                results.append(self._project_columns(row, columns))
                        except:
                            continue  # Игнорируем удаленные строки
                    return results
        
        # Полное сканирование таблицы
        for row in self.table_manager.scan_rows(table_name, storage):
            if self._matches_where(row, where):
                results.append(self._project_columns(row, columns))
        
        return results
    
    def delete(self, table_name: str, where: Dict = None) -> int:
        """Удаление данных из таблицы"""
        storage = self._get_storage(table_name)
        deleted_count = 0
        
        if where:
            # Удаление по условию WHERE
            rows_to_delete = []
            for row in self.table_manager.scan_rows(table_name, storage):
                if self._matches_where(row, where):
                    rows_to_delete.append((row['_position'], row))
            
            # Помечаем строки как удаленные
            for pos, row in rows_to_delete:
                file_path = self.table_manager._get_table_path(table_name)
                with open(file_path, 'r+b') as f:
                    f.seek(pos)
                    f.write(b'\x01')  # Флаг удаления
                    deleted_count += 1
                
                # Обновляем индексы
                if table_name in self.indexes:
                    for col_name, index in self.indexes[table_name].items():
                        if col_name in row:
                            index.delete(row[col_name], pos)
        else:
            # DELETE * - полная очистка таблицы
            schema = self.schema_manager.load_schema(table_name)
            self.table_manager.create_table_file(schema)
            deleted_count = -1  # Специальное значение
        
        return deleted_count
    
    def get_table_info(self, table_name: str) -> Dict:
        """Получить информацию о таблице"""
        schema = self.schema_manager.load_schema(table_name)
        storage = self._get_storage(table_name)
        
        return {
            'table_name': table_name,
            'columns': [{'name': col.name, 'type': col.data_type, 'size': col.size} for col in schema.columns],
            'row_size': storage.row_size,
            'total_rows': self.table_manager.get_total_rows(table_name),
            'indexes': list(self.indexes.get(table_name, {}).keys())
        }
    
    def _get_storage(self, table_name: str) -> UTF32RowStorage:
        """Получить объект хранилища для таблицы"""
        if table_name not in self.storages:
            schema = self.schema_manager.load_schema(table_name)
            self.storages[table_name] = UTF32RowStorage(schema.columns)
        return self.storages[table_name]
    
    def _matches_where(self, row: Dict, where: Dict) -> bool:
        """Проверка условия WHERE"""
        if not where:
            return True
        
        for col, value in where.items():
            if row.get(col) != value:
                return False
        return True
    
    def _project_columns(self, row: Dict, columns: List[str]) -> Dict:
        """Проекция столбцов"""
        if not columns or '*' in columns:
            return {k: v for k, v in row.items() if not k.startswith('_')}
        
        result = {}
        for col in columns:
            if col in row:
                result[col] = row[col]
        return result
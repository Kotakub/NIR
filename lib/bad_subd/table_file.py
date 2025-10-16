import os
import struct
from typing import List, Dict, Any, Iterator
from .storage import UTF32RowStorage
from .schema import TableSchema
from .config import bad_subd_config

class TableFileManager:
    def __init__(self, table_dir: str = None):
        self.table_dir = table_dir or bad_subd_config.TABLE_DIR
    
    def create_table_file(self, schema: TableSchema) -> None:
        file_path = self._get_table_path(schema.table_name)
        with open(file_path, 'wb') as f:
            f.write(b'CDB3')
            f.write(struct.pack('>Q', 0))
            f.write(struct.pack('>I', 0))
    
    def insert_row(self, table_name: str, row_data: Dict[str, Any], storage: UTF32RowStorage):
        file_path = self._get_table_path(table_name)
        row_bytes = storage.serialize_row(row_data)
        
        with open(file_path, 'r+b') as f:
            f.seek(4)
            row_count_bytes = f.read(8)
            row_count = struct.unpack('>Q', row_count_bytes)[0]
            
            f.seek(0, 2)
            position = f.tell()
            
            f.write(row_bytes)
            
            f.seek(4)
            f.write(struct.pack('>Q', row_count + 1))
        
        return position
    
    def read_row_at_position(self, table_name: str, position: int, storage: UTF32RowStorage) -> Dict[str, Any]:
        file_path = self._get_table_path(table_name)
        with open(file_path, 'rb') as f:
            f.seek(position)
            row_data = f.read(storage.row_size)
            if len(row_data) != storage.row_size:
                raise ValueError("Неправильная строка")
            return storage.deserialize_row(row_data)
    
    def read_row_by_index(self, table_name: str, row_index: int, storage: UTF32RowStorage) -> Dict[str, Any]:
        position = 16 + row_index * storage.row_size
        return self.read_row_at_position(table_name, position, storage)
    
    def update_row(self, table_name: str, position: int, row_data: Dict[str, Any], storage: UTF32RowStorage) -> None:
        file_path = self._get_table_path(table_name)
        row_bytes = storage.serialize_row(row_data)
        
        with open(file_path, 'r+b') as f:
            f.seek(position)
            f.write(row_bytes)
    
    def scan_rows(self, table_name: str, storage: UTF32RowStorage):
        file_path = self._get_table_path(table_name)
        
        if not os.path.exists(file_path):
            return
            
        with open(file_path, 'rb') as f:
            f.seek(16)
            
            row_index = 0
            while True:
                position = f.tell()
                row_data = f.read(storage.row_size)
                
                if not row_data or len(row_data) < storage.row_size:
                    break
                
                row = storage.deserialize_row(row_data)
                if not row.get('_deleted', False):
                    row['_position'] = position
                    row['_index'] = row_index
                    yield row
                
                row_index += 1
    
    def get_total_rows(self, table_name: str) -> int:
        """Получить общее количество строк в таблице"""
        file_path = self._get_table_path(table_name)
        
        if not os.path.exists(file_path):
            return 0
            
        with open(file_path, 'rb') as f:
            f.seek(4)
            row_count_bytes = f.read(8)
            return struct.unpack('>Q', row_count_bytes)[0]
    
    def delete_table_file(self, table_name: str) -> None:
        """Удаление файла таблицы"""
        file_path = self._get_table_path(table_name)
        if os.path.exists(file_path):
            os.remove(file_path)
    
    def _get_table_path(self, table_name: str) -> str:
        return os.path.join(self.table_dir, f"{table_name}.dat")
# custom_db/index.py
import struct
import os
from typing import List, Dict, Any
from collections import defaultdict
from .config import custom_db_config

class SimpleHashIndex:
    """Простой хэш-индекс для числовых колонок"""
    
    def __init__(self, table_name: str, column_name: str):
        self.table_name = table_name
        self.column_name = column_name
        self.filename = os.path.join(custom_db_config.INDEX_DIR, f"{table_name}_{column_name}.idx")
        self._index_dict = defaultdict(list)
        self._load_index()
    
    def insert(self, key: int, row_position: int) -> None:
        """Вставка ключа и позиции строки в индекс"""
        if key not in self._index_dict:
            self._index_dict[key] = []
        self._index_dict[key].append(row_position)
        self._save_index()
    
    def find(self, key: int) -> List[int]:
        """Поиск позиций строк по ключу"""
        return self._index_dict.get(key, [])
    
    def delete(self, key: int, row_position: int = None) -> None:
        """Удаление из индекса"""
        if key in self._index_dict:
            if row_position is None:
                del self._index_dict[key]
            else:
                self._index_dict[key] = [pos for pos in self._index_dict[key] if pos != row_position]
                if not self._index_dict[key]:
                    del self._index_dict[key]
            self._save_index()
    
    def _load_index(self) -> None:
        """Загрузка индекса из файла"""
        if os.path.exists(self.filename):
            with open(self.filename, 'rb') as f:
                while True:
                    key_data = f.read(8)
                    if not key_data:
                        break
                    key = struct.unpack('>Q', key_data)[0]
                    count_data = f.read(4)
                    count = struct.unpack('>I', count_data)[0]
                    positions = []
                    for _ in range(count):
                        pos_data = f.read(8)
                        positions.append(struct.unpack('>Q', pos_data)[0])
                    self._index_dict[key] = positions
    
    def _save_index(self) -> None:
        """Сохранение индекса в файл"""
        with open(self.filename, 'wb') as f:
            for key, positions in sorted(self._index_dict.items()):
                f.write(struct.pack('>Q', key))
                f.write(struct.pack('>I', len(positions)))
                for pos in positions:
                    f.write(struct.pack('>Q', pos))
    
    def get_index_size(self) -> int:
        """Получить размер индекса в байтах"""
        if os.path.exists(self.filename):
            return os.path.getsize(self.filename)
        return 0
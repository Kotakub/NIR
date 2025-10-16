# custom_db/storage.py
import struct
import os
from typing import Dict, List, Any, Optional
from dataclasses import dataclass
from .config import custom_db_config

@dataclass
class ColumnDefinition:
    """Определение столбца таблицы"""
    name: str
    data_type: str  # 'INT' или 'VARCHAR'
    size: int = 0   # Для VARCHAR - количество символов

class UTF32RowStorage:
    """Хранение строк в UTF-32 с фиксированной длиной"""
    
    def __init__(self, columns: List[ColumnDefinition]):
        self.columns = columns
        self.row_size = self._calculate_row_size()
        
    def _calculate_row_size(self) -> int:
        """Вычисляет размер одной строки в байтах"""
        size = 1  # Флаг удаления
        
        for col in self.columns:
            if col.data_type == 'INT':
                size += custom_db_config.INT_SIZE
            elif col.data_type == 'VARCHAR':
                # Каждый символ занимает 4 байта в UTF-32
                size += col.size * custom_db_config.CHAR_SIZE
                
        return size
    
    def _string_to_utf32_fixed(self, text: str, max_chars: int) -> bytes:
        """Конвертация строки в UTF-32 с фиксированной длиной"""
        if len(text) > max_chars:
            text = text[:max_chars]
        
        # Конвертируем в UTF-32LE
        utf32_data = text.encode('utf-32-le')
        
        # Дополняем до фиксированного размера
        expected_size = max_chars * 4
        if len(utf32_data) < expected_size:
            utf32_data += b'\x00' * (expected_size - len(utf32_data))
        
        return utf32_data
    
    def _utf32_to_string(self, data: bytes) -> str:
        """Конвертация UTF-32 данных в строку"""
        data = data.rstrip(b'\x00')
        
        # Дополняем до кратности 4
        remainder = len(data) % 4
        if remainder != 0:
            data += b'\x00' * (4 - remainder)
        
        if len(data) == 0:
            return ""
            
        return data.decode('utf-32-le', errors='ignore')
    
    def serialize_row(self, row: Dict[str, Any]) -> bytes:
        """Сериализация строки в бинарный формат"""
        data = bytearray()
        
        # Флаг удаления (0 - активна, 1 - удалена)
        data.append(0)
        
        for col in self.columns:
            value = row.get(col.name)
            
            if col.data_type == 'INT':
                # uint64 (8 байт)
                int_val = value if value is not None else 0
                if not isinstance(int_val, int) or int_val < 0:
                    raise ValueError(f"INT value must be positive integer: {col.name}")
                if int_val > 2**64 - 1:
                    raise ValueError(f"INT value too large: {col.name}")
                    
                data.extend(struct.pack('>Q', int_val))
                
            elif col.data_type == 'VARCHAR':
                # VARCHAR: фиксированный размер в UTF-32
                str_val = value if value is not None else ""
                if not isinstance(str_val, str):
                    raise ValueError(f"VARCHAR value must be string: {col.name}")
                
                utf32_data = self._string_to_utf32_fixed(str_val, col.size)
                data.extend(utf32_data)
        
        return bytes(data)
    
    def deserialize_row(self, data: bytes) -> Dict[str, Any]:
        """Десериализация строки из бинарного формата"""
        if len(data) != self.row_size:
            raise ValueError(f"Invalid row data size: expected {self.row_size}, got {len(data)}")
            
        row = {}
        pos = 0
        
        # Флаг удаления
        deleted = data[pos]
        pos += 1
        row['_deleted'] = bool(deleted)
        
        for col in self.columns:
            if col.data_type == 'INT':
                # uint64 (8 байт)
                value = struct.unpack('>Q', data[pos:pos+8])[0]
                pos += 8
                row[col.name] = value
                
            elif col.data_type == 'VARCHAR':
                # VARCHAR: фиксированный размер в UTF-32
                str_data = data[pos:pos + col.size * 4]
                pos += col.size * 4
                row[col.name] = self._utf32_to_string(str_data)
        
        return row
    
    def get_column_offset(self, column_name: str) -> int:
        """Получить смещение столбца в строке"""
        offset = 1  # Начинаем после флага удаления
        
        for col in self.columns:
            if col.name == column_name:
                return offset
                
            if col.data_type == 'INT':
                offset += custom_db_config.INT_SIZE
            elif col.data_type == 'VARCHAR':
                offset += col.size * custom_db_config.CHAR_SIZE
                
        raise ValueError(f"Column {column_name} not found")
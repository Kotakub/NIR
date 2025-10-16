import json
import os
from typing import List, Dict, Any
from dataclasses import dataclass, asdict
from .storage import ColumnDefinition
from .config import bad_subd_config

@dataclass
class TableSchema:
    """Схема таблицы"""
    table_name: str
    columns: List[ColumnDefinition]
    primary_key: str = None
    
    def to_dict(self) -> Dict[str, Any]:
        """Сериализация схемы в словарь"""
        return {
            'table_name': self.table_name,
            'columns': [asdict(col) for col in self.columns],
            'primary_key': self.primary_key
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TableSchema':
        """Десериализация схемы из словаря"""
        columns = []
        for col_data in data['columns']:
            columns.append(ColumnDefinition(**col_data))
        return cls(
            table_name=data['table_name'],
            columns=columns,
            primary_key=data.get('primary_key')
        )

class SchemaManager:
    """Управление схемами таблиц"""
    
    def __init__(self, schema_dir: str = None):
        self.schema_dir = schema_dir or bad_subd_config.SCHEMA_DIR
    
    def save_schema(self, schema: TableSchema) -> None:
        """Сохранение схемы в файл"""
        file_path = os.path.join(self.schema_dir, f"{schema.table_name}.json")
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(schema.to_dict(), f, indent=2, ensure_ascii=False)
    
    def load_schema(self, table_name: str) -> TableSchema:
        """Загрузка схемы из файла"""
        file_path = os.path.join(self.schema_dir, f"{table_name}.json")
        with open(file_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return TableSchema.from_dict(data)
    
    def schema_exists(self, table_name: str) -> bool:
        """Проверка существования схемы"""
        file_path = os.path.join(self.schema_dir, f"{table_name}.json")
        return os.path.exists(file_path)
    
    def delete_schema(self, table_name: str) -> None:
        """Удаление схемы"""
        file_path = os.path.join(self.schema_dir, f"{table_name}.json")
        if os.path.exists(file_path):
            os.remove(file_path)
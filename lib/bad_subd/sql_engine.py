from typing import Any, List, Dict
from .engine import BadSUBDEngine
from .sql_parser import SQLParser

class SQLBadSUBDEngine:
    """SQL-интерфейс для BadSUBD"""
    
    def __init__(self, base_path: str = None):
        self.engine = BadSUBDEngine(base_path)
        self.parser = SQLParser(self.engine)
    
    def execute(self, sql: str) -> Any:
        """Выполнить SQL запрос"""
        return self.parser.execute(sql)
    
    def create_table(self, table_name: str, columns: List[Dict]) -> None:
        """Создать таблицу (совместимость с существующим кодом)"""
        self.engine.create_table(table_name, columns)
    
    def create_index(self, table_name: str, column_name: str) -> None:
        """Создать индекс (совместимость с существующим кодом)"""
        self.engine.create_index(table_name, column_name)
    
    def get_table_info(self, table_name: str) -> Dict:
        """Получить информацию о таблице (совместимость)"""
        return self.engine.get_table_info(table_name)

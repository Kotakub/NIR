from .engine import BadSUBDEngine
from .sql_engine import SQLBadSUBDEngine

class BadSUBD:
    """СУБД с фиксированной длиной символов UTF-32"""
    
    def __init__(self, base_path: str = None, use_sql: bool = False):
        if use_sql:
            self.engine = SQLBadSUBDEngine(base_path)
        else:
            self.engine = BadSUBDEngine(base_path)
    
    def execute(self, sql: str):
        """Выполнить SQL запрос (только если use_sql=True)"""
        if hasattr(self.engine, 'execute'):
            return self.engine.execute(sql)
        else:
            raise RuntimeError("SQL support not enabled. Initialize with use_sql=True")
    
    def create_table_users(self):
        """Создать таблицу users"""
        self.engine.create_table("users", [
            {"name": "id", "type": "INT"},
            {"name": "login", "type": "VARCHAR", "size": 45},
            {"name": "email", "type": "VARCHAR", "size": 45},
            {"name": "password", "type": "VARCHAR", "size": 15}
        ])
        self.engine.create_index("users", "id")
    
    def create_table_lessons(self):
        """Создать таблицу lessons"""
        self.engine.create_table("lessons", [
            {"name": "id", "type": "INT"},
            {"name": "schedule_id", "type": "INT"},
            {"name": "teacher_name", "type": "VARCHAR", "size": 100},
            {"name": "subject_name", "type": "VARCHAR", "size": 60},
            {"name": "place", "type": "VARCHAR", "size": 70},
            {"name": "startDate", "type": "VARCHAR", "size": 15},
            {"name": "endDate", "type": "VARCHAR", "size": 15},
            {"name": "startTime", "type": "VARCHAR", "size": 15},
            {"name": "endTime", "type": "VARCHAR", "size": 15},
            {"name": "repeat", "type": "VARCHAR", "size": 15}
        ])
        self.engine.create_index("lessons", "id")
        self.engine.create_index("lessons", "schedule_id")
    
    def create_table_comments(self):
        """Создать таблицу comments"""
        self.engine.create_table("comments", [
            {"name": "id", "type": "INT"},
            {"name": "lesson_id", "type": "INT"},
            {"name": "date", "type": "VARCHAR", "size": 15},
            {"name": "text", "type": "VARCHAR", "size": 255}
        ])
        self.engine.create_index("comments", "id")
        self.engine.create_index("comments", "lesson_id")
    
    def create_all_tables(self):
        self.create_table_users()
        self.create_table_lessons()
        self.create_table_comments()
    
    def demo_storage(self):
        for table in ["users", "lessons", "comments"]:
            try:
                info = self.engine.get_table_info(table)
                print(f"Table: {info['table_name']}")
                print(f"  Row size: {info['row_size']} bytes")
                print(f"  Total rows: {info['total_rows']}")
                print(f"  Indexes: {info['indexes']}")
            except:
                print(f"Table {table} not found")


bad_sudb = BadSUBD()  
bad_sudb_sql = BadSUBD(use_sql=True) 
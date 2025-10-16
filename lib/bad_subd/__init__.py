# custom_db/__init__.py
from .engine import CustomDBEngine

class CustomSimpleDB:
    """СУБД с фиксированной длиной символов UTF-32"""
    
    def __init__(self, base_path: str = None):
        self.engine = CustomDBEngine(base_path)
    
    def create_table_users(self):
        """Создать таблицу users"""
        self.engine.create_table("users", [
            {"name": "id", "type": "INT"},
            {"name": "login", "type": "VARCHAR", "size": 50},
            {"name": "email", "type": "VARCHAR", "size": 100},
            {"name": "password", "type": "VARCHAR", "size": 50}
        ])
        self.engine.create_index("users", "id")
    
    def create_table_lessons(self):
        """Создать таблицу lessons"""
        self.engine.create_table("lessons", [
            {"name": "id", "type": "INT"},
            {"name": "schedule_id", "type": "INT"},
            {"name": "teacher_name", "type": "VARCHAR", "size": 100},
            {"name": "subject_name", "type": "VARCHAR", "size": 100},
            {"name": "place", "type": "VARCHAR", "size": 100},
            {"name": "startDate", "type": "VARCHAR", "size": 20},
            {"name": "endDate", "type": "VARCHAR", "size": 20},
            {"name": "startTime", "type": "VARCHAR", "size": 10},
            {"name": "endTime", "type": "VARCHAR", "size": 10},
            {"name": "repeat", "type": "VARCHAR", "size": 50}
        ])
        self.engine.create_index("lessons", "id")
        self.engine.create_index("lessons", "schedule_id")
    
    def create_table_comments(self):
        """Создать таблицу comments"""
        self.engine.create_table("comments", [
            {"name": "id", "type": "INT"},
            {"name": "lesson_id", "type": "INT"},
            {"name": "date", "type": "VARCHAR", "size": 20},
            {"name": "text", "type": "VARCHAR", "size": 255}
        ])
        self.engine.create_index("comments", "id")
        self.engine.create_index("comments", "lesson_id")
    
    def create_all_tables(self):
        """Создать все таблицы"""
        self.create_table_users()
        self.create_table_lessons()
        self.create_table_comments()
        print("All UTF-32 tables created successfully!")
    
    def demo_storage(self):
        """Демонстрация работы СУБД"""
        print("\n=== Custom DB Storage Demo ===")
        
        # Покажем информацию о таблицах
        for table in ["users", "lessons", "comments"]:
            try:
                info = self.engine.get_table_info(table)
                print(f"Table: {info['table_name']}")
                print(f"  Row size: {info['row_size']} bytes")
                print(f"  Total rows: {info['total_rows']}")
                print(f"  Indexes: {info['indexes']}")
            except:
                print(f"Table {table} not found")

# Глобальный экземпляр для удобства
custom_db = CustomSimpleDB()
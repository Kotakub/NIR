import psycopg
from typing import Optional, Callable

class SandboxManager:
    def __init__(self, database_instance):
        self.db = database_instance
    
    def create_sandbox(self):
        """Создание песочницы (тестовой БД)"""
        try:
            conn = self.db.get_connection(test_db=False)
            conn.autocommit = True
            
            with conn.cursor() as cur:
                try:
                    cur.execute(f"CREATE DATABASE test_db")
                    print(f"База данных test_db создана")
                except psycopg.Error as e:
                    if "already exists" in str(e):
                        print(f"База данных test_db уже существует")
                    else:
                        raise e
            
            conn.close()
            
            # Создаем таблицы в тестовой БД используя наш метод
            self.copy_schema_to_sandbox()
            return True
            
        except Exception as e:
            print(f"Ошибка при создании песочницы: {e}")
            return False
    
    def copy_schema_to_sandbox(self):
        """Копирование схемы из основной БД в песочницу"""
        self.db.create_tables(test_db=True)
        print("Схема скопирована в песочницу")
    
    def copy_data_to_sandbox(self, table_name: str):
        try:
            # Получаем данные из основной БД используя get_database_connection
            with self.db.get_database_connection(test_db=False) as conn:
                with conn.cursor() as cur:
                    cur.execute(f"SELECT * FROM {table_name}")
                    data = cur.fetchall()
            
            if not data:
                print(f"Таблица {table_name} пуста, пропускаем")
                return
            
            # Получаем информацию о столбцах
            with self.db.get_database_connection(test_db=False) as conn:
                with conn.cursor() as cur:
                    cur.execute(f"""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_name = %s 
                        ORDER BY ordinal_position
                    """, (table_name,))
                    columns = [row[0] for row in cur.fetchall()]
            
            columns_str = ', '.join(columns)
            placeholders = ', '.join(['%s'] * len(columns))
            query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
            
            with self.db.get_database_connection(test_db=True) as conn:
                with conn.cursor() as cur:
                    for row in data:
                        cur.execute(query, row)
            
            print(f"Данные таблицы {table_name} скопированы в песочницу")
                    
        except Exception as e:
            print(f"Ошибка при копировании данных таблицы {table_name}: {e}")
            raise
    
    def copy_all_data_to_sandbox(self):
        """Копирование всех данных из основной БД в песочницу"""
        tables = ['users', 'schedules', 'lessons', 'schedule_user', 'comments', 'attendance']
        for table in tables:
            self.copy_data_to_sandbox(table)
        print("Все данные скопированы в песочницу")
    
    def cleanup_sandbox(self):
        """Очистка песочницы"""
        tables = ['attendance', 'comments', 'schedule_user', 'lessons', 'schedules', 'users']
        
        with self.db.get_database_connection(test_db=True) as conn:
            with conn.cursor() as cur:
                for table in tables:
                    try:
                        cur.execute(f"TRUNCATE TABLE {table} CASCADE")
                    except Exception:
                        pass
        print("Песочница очищена")
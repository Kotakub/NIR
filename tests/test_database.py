# tests/test_database.py
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from lib.database.config import settings
from lib.database.database import DataBaseMain

class TestDatabase:
    """Тестирование функциональности базы данных"""
    
    def __init__(self):
        self.db_main = DataBaseMain(settings.DATABASE_URL)
        self.db_test = DataBaseMain(settings.TEST_DATABASE_URL)
    
    def test_table_creation(self):
        """Тестирование создания таблиц"""
        print("=== ТЕСТИРОВАНИЕ СОЗДАНИЯ ТАБЛИЦ ===")
        
        try:
            # Создание таблиц в основной БД
            print("Создание таблиц в основной БД...")
            self.db_main.create_tables()
            print("✓ Таблицы созданы успешно")
            
            # Создание таблиц в тестовой БД
            print("Создание таблиц в тестовой БД...")
            self.db_test.create_tables()
            print("✓ Таблицы в тестовой БД созданы успешно")
            
            return True
        except Exception as e:
            print(f"✗ Ошибка при создании таблиц: {e}")
            return False
    
    def test_connection(self):
        """Тестирование подключения к БД"""
        print("=== ТЕСТИРОВАНИЕ ПОДКЛЮЧЕНИЯ К БД ===")
        
        try:
            with self.db_main.get_database_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT version()")
                    result = cur.fetchone()
                    print(f"✓ Подключение к основной БД успешно: {result[0]}")
            
            with self.db_test.get_database_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("SELECT version()")
                    result = cur.fetchone()
                    print(f"✓ Подключение к тестовой БД успешно: {result[0]}")
            
            return True
        except Exception as e:
            print(f"✗ Ошибка подключения к БД: {e}")
            return False

def run_database_tests():
    """Запуск всех тестов базы данных"""
    tester = TestDatabase()
    
    tests = [
        tester.test_connection,
        tester.test_table_creation
    ]
    
    results = []
    for test in tests:
        try:
            result = test()
            results.append((test.__name__, result))
        except Exception as e:
            print(f"✗ Ошибка в тесте {test.__name__}: {e}")
            results.append((test.__name__, False))
    
    print("\n=== РЕЗУЛЬТАТЫ ТЕСТИРОВАНИЯ БАЗЫ ДАННЫХ ===")
    for test_name, result in results:
        status = "✓ УСПЕХ" if result else "✗ ОШИБКА"
        print(f"{test_name}: {status}")
    
    return all(result for _, result in results)

if __name__ == "__main__":
    run_database_tests()
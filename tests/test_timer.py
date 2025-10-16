# tests/test_query_timing.py
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from lib.database.config import settings
from lib.database.database import DataBaseMain
from lib.timer.timer import QueryTimer

class TestQueryTiming:
    """Тестирование измерения времени выполнения запросов"""
    
    def __init__(self):
        self.db_main = DataBaseMain(settings.DATABASE_URL)
        self.query_timer = QueryTimer(self.db_main.get_connection)
    
    def test_simple_query_timing(self):
        """Тестирование времени простых запросов"""
        print("Тестирование времени простых запросов...")
        
        test_queries = [
            ("SELECT COUNT(*) FROM users", None, "Подсчет пользователей"),
            ("SELECT version()", None, "Версия PostgreSQL"),
            ("SELECT current_date", None, "Текущая дата")
        ]
        
        results = []
        for query, params, description in test_queries:
            try:
                execution_time = self.query_timer.measure_query(query, params, iterations=2)
                print(f"✓ {description}: {execution_time:.6f} секунд")
                results.append(True)
            except Exception as e:
                print(f"✗ Ошибка в запросе '{description}': {e}")
                results.append(False)
        
        return all(results)
    
    def test_complex_query_timing(self):
        """Тестирование времени сложных запросов"""
        print("Тестирование времени сложных запросов...")
        
        test_queries = [
            ("SELECT * FROM lessons WHERE schedule_id = %s", (1,), "Фильтрация занятий"),
            ("""SELECT u.login, s.name 
                FROM users u 
                JOIN schedule_user su ON u.id = su.user_id 
                JOIN schedules s ON s.id = su.schedule_id""", 
             None, "JOIN запрос")
        ]
        
        results = []
        for query, params, description in test_queries:
            try:
                execution_time = self.query_timer.measure_query(query, params, iterations=2)
                print(f"✓ {description}: {execution_time:.6f} секунд")
                results.append(True)
            except Exception as e:
                print(f"✗ Ошибка в запросе '{description}': {e}")
                results.append(False)
        
        return all(results)
    
    def test_transaction_timing(self):
        """Тестирование времени транзакций"""
        print("Тестирование времени транзакций...")
        
        transaction_queries = [
            ("SELECT COUNT(*) FROM users", None),
            ("SELECT COUNT(*) FROM schedules", None),
            ("SELECT COUNT(*) FROM lessons", None)
        ]
        
        try:
            results = self.query_timer.measure_transaction(transaction_queries, iterations=2)
            for i, time_taken in enumerate(results):
                print(f"✓ Запрос {i+1}: {time_taken:.6f} секунд")
            return True
        except Exception as e:
            print(f"✗ Ошибка измерения времени транзакций: {e}")
            return False

def run_query_timing_tests():
    """Запуск всех тестов измерения времени запросов"""
    tester = TestQueryTiming()
    
    tests = [
        tester.test_simple_query_timing,
        tester.test_complex_query_timing,
        tester.test_transaction_timing
    ]
    
    print("=== ТЕСТИРОВАНИЕ ИЗМЕРЕНИЯ ВРЕМЕНИ ЗАПРОСОВ ===")
    
    results = []
    for test in tests:
        try:
            result = test()
            results.append((test.__name__, result))
        except Exception as e:
            print(f"✗ Ошибка в тесте {test.__name__}: {e}")
            results.append((test.__name__, False))
    
    print("\n=== РЕЗУЛЬТАТЫ ТЕСТИРОВАНИЯ ВРЕМЕНИ ЗАПРОСОВ ===")
    for test_name, result in results:
        status = "✓ УСПЕХ" if result else "✗ ОШИБКА"
        print(f"{test_name}: {status}")
    
    return all(result for _, result in results)

if __name__ == "__main__":
    run_query_timing_tests()
# tests/test_data_generation.py
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from lib.database.config import settings
from lib.database.database import DataBaseMain
from lib.data.generate_data import DataGenerator

class TestDataGeneration:
    """Тестирование генерации данных"""
    
    def __init__(self):
        self.db_main = DataBaseMain(settings.DATABASE_URL)
        self.data_generator = DataGenerator(self.db_main.get_connection)
    
    def test_users_generation(self):
        """Тестирование генерации пользователей"""
        print("Тестирование генерации пользователей...")
        try:
            self.data_generator.generate_users(3)
            print("✓ 3 пользователя сгенерировано успешно")
            return True
        except Exception as e:
            print(f"✗ Ошибка генерации пользователей: {e}")
            return False
    
    def test_schedules_generation(self):
        """Тестирование генерации расписаний"""
        print("Тестирование генерации расписаний...")
        try:
            self.data_generator.generate_schedules(2)
            print("✓ 2 расписания сгенерировано успешно")
            return True
        except Exception as e:
            print(f"✗ Ошибка генерации расписаний: {e}")
            return False
    
    def test_lessons_generation(self):
        """Тестирование генерации занятий"""
        print("Тестирование генерации занятий...")
        try:
            self.data_generator.generate_lessons(5)
            print("✓ 5 занятий сгенерировано успешно")
            return True
        except Exception as e:
            print(f"✗ Ошибка генерации занятий: {e}")
            return False
    
    def test_attendance_generation(self):
        """Тестирование генерации посещаемости"""
        print("Тестирование генерации посещаемости...")
        try:
            self.data_generator.generate_attendance(8)
            print("✓ 8 записей посещаемости сгенерировано успешно")
            return True
        except Exception as e:
            print(f"✗ Ошибка генерации посещаемости: {e}")
            return False
    
    def test_comments_generation(self):
        """Тестирование генерации комментариев"""
        print("Тестирование генерации комментариев...")
        try:
            self.data_generator.generate_comments(4)
            print("✓ 4 комментария сгенерировано успешно")
            return True
        except Exception as e:
            print(f"✗ Ошибка генерации комментариев: {e}")
            return False
    
    def test_schedule_user_generation(self):
        """Тестирование генерации связей пользователей с расписаниями"""
        print("Тестирование генерации связей пользователей с расписаниями...")
        try:
            self.data_generator.generate_schedule_user(6)
            print("✓ 6 связей сгенерировано успешно")
            return True
        except Exception as e:
            print(f"✗ Ошибка генерации связей: {e}")
            return False

def run_data_generation_tests():
    """Запуск всех тестов генерации данных"""
    tester = TestDataGeneration()
    
    tests = [
        tester.test_users_generation,
        tester.test_schedules_generation,
        tester.test_lessons_generation,
        tester.test_attendance_generation,
        tester.test_comments_generation,
        tester.test_schedule_user_generation
    ]
    
    print("=== ТЕСТИРОВАНИЕ ГЕНЕРАЦИИ ДАННЫХ ===")
    
    results = []
    for test in tests:
        try:
            result = test()
            results.append((test.__name__, result))
        except Exception as e:
            print(f"✗ Ошибка в тесте {test.__name__}: {e}")
            results.append((test.__name__, False))
    
    print("\n=== РЕЗУЛЬТАТЫ ТЕСТИРОВАНИЯ ГЕНЕРАЦИИ ДАННЫХ ===")
    for test_name, result in results:
        status = "✓ УСПЕХ" if result else "✗ ОШИБКА"
        print(f"{test_name}: {status}")
    
    return all(result for _, result in results)

if __name__ == "__main__":
    run_data_generation_tests()
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from lib.database.config import settings
from lib.database.database import DataBaseMain
from lib.database.sandbox import SandboxManager

class TestSandbox:
    """Тестирование операций с песочницей"""
    
    def __init__(self):
        self.db_main = DataBaseMain(settings.DATABASE_URL)
        self.db_test = DataBaseMain(settings.TEST_DATABASE_URL)
        self.sandbox_manager = SandboxManager(
            self.db_main.get_connection,
            self.db_test.get_connection,
            settings.TEST_DATABASE_URL
        )
    
    def test_sandbox_creation(self):
        """Тестирование создания песочницы"""
        print("Тестирование создания песочницы...")
        try:
            if self.sandbox_manager.create_sandbox():
                print("✓ Песочница создана успешно")
                return True
            else:
                print("✗ Не удалось создать песочницу")
                return False
        except Exception as e:
            print(f"✗ Ошибка создания песочницы: {e}")
            return False
    
    def test_schema_copy(self):
        """Тестирование копирования схемы"""
        print("Тестирование копирования схемы...")
        try:
            self.sandbox_manager.copy_schema_to_sandbox()
            print("✓ Схема скопирована успешно")
            return True
        except Exception as e:
            print(f"✗ Ошибка копирования схемы: {e}")
            return False
    
    def test_data_copy(self):
        """Тестирование копирования данных"""
        print("Тестирование копирования данных...")
        try:
            self.sandbox_manager.copy_all_data_to_sandbox()
            print("✓ Данные скопированы успешно")
            return True
        except Exception as e:
            print(f"✗ Ошибка копирования данных: {e}")
            return False
    
    def test_sandbox_cleanup(self):
        """Тестирование очистки песочницы"""
        print("Тестирование очистки песочницы...")
        try:
            self.sandbox_manager.cleanup_sandbox()
            print("✓ Песочница очищена успешно")
            return True
        except Exception as e:
            print(f"✗ Ошибка очистки песочницы: {e}")
            return False

def run_sandbox_tests():
    """Запуск всех тестов песочницы"""
    tester = TestSandbox()
    
    tests = [
        tester.test_sandbox_creation,
        tester.test_schema_copy,
        tester.test_data_copy,
        tester.test_sandbox_cleanup
    ]
    
    print("=== ТЕСТИРОВАНИЕ ОПЕРАЦИЙ С ПЕСОЧНИЦЕЙ ===")
    
    results = []
    for test in tests:
        try:
            result = test()
            results.append((test.__name__, result))
        except Exception as e:
            print(f"✗ Ошибка в тесте {test.__name__}: {e}")
            results.append((test.__name__, False))
    
    print("\n=== РЕЗУЛЬТАТЫ ТЕСТИРОВАНИЯ ПЕСОЧНИЦЫ ===")
    for test_name, result in results:
        status = "✓ УСПЕХ" if result else "✗ ОШИБКА"
        print(f"{test_name}: {status}")
    
    return all(result for _, result in results)

if __name__ == "__main__":
    run_sandbox_tests()
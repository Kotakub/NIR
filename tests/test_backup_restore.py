import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from lib.database.config import settings
from lib.database.database import DataBaseMain
from lib.backup.backup import BackupManager

class TestBackupRestore:
    """Тестирование бэкапа и восстановления"""
    
    def __init__(self):
        self.db_main = DataBaseMain(settings.DATABASE_URL)
        self.backup_manager = BackupManager(self.db_main.get_connection)
    
    def test_backup_creation(self):
        """Тестирование создания бэкапа"""
        print("Тестирование создания бэкапа...")
        try:
            backup_path = self.backup_manager.create_backup()
            print(f"✓ Бэкап создан: {backup_path}")
            
            # Проверяем, что файл существует
            if os.path.exists(backup_path):
                print("✓ Файл бэкапа существует")
                return True
            else:
                print("✗ Файл бэкапа не найден")
                return False
        except Exception as e:
            print(f"✗ Ошибка создания бэкапа: {e}")
            return False
    
    def test_backup_restore(self):
        """Тестирование восстановления из бэкапа"""
        print("Тестирование восстановления из бэкапа...")
        try:
            # Сначала создаем бэкап
            backup_path = self.backup_manager.create_backup()
            print(f"Создан бэкап для восстановления: {backup_path}")
            
            # Восстанавливаем из бэкапа
            self.backup_manager.restore_backup(backup_path)
            print("✓ Восстановление из бэкапа выполнено успешно")
            return True
        except Exception as e:
            print(f"✗ Ошибка восстановления из бэкапа: {e}")
            return False

def run_backup_restore_tests():
    """Запуск всех тестов бэкапа и восстановления"""
    tester = TestBackupRestore()
    
    tests = [
        tester.test_backup_creation,
        tester.test_backup_restore
    ]
    
    print("=== ТЕСТИРОВАНИЕ БЭКАПА И ВОССТАНОВЛЕНИЯ ===")
    
    results = []
    for test in tests:
        try:
            result = test()
            results.append((test.__name__, result))
        except Exception as e:
            print(f"✗ Ошибка в тесте {test.__name__}: {e}")
            results.append((test.__name__, False))
    
    print("\n=== РЕЗУЛЬТАТЫ ТЕСТИРОВАНИЯ БЭКАПА ===")
    for test_name, result in results:
        status = "✓ УСПЕХ" if result else "✗ ОШИБКА"
        print(f"{test_name}: {status}")
    
    return all(result for _, result in results)

if __name__ == "__main__":
    run_backup_restore_tests()
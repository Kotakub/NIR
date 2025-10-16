# tests/test_integration.py
import os
import sys
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from test_database import run_database_tests
from test_data_generation import run_data_generation_tests
from test_backup_restore import run_backup_restore_tests
from test_timer import run_query_timing_tests
from test_sandbox import run_sandbox_tests
from test_plots import run_plotting_tests

def run_all_tests():
    """Запуск всех тестов"""
    print("🚀 ЗАПУСК ПОЛНОГО ТЕСТИРОВАНИЯ СИСТЕМЫ")
    print("=" * 60)
    
    test_modules = [
        ("База данных", run_database_tests),
        ("Генерация данных", run_data_generation_tests),
        ("Бэкап и восстановление", run_backup_restore_tests),
        ("Измерение времени запросов", run_query_timing_tests),
        ("Песочница", run_sandbox_tests),
        ("Построение графиков", run_plotting_tests)
    ]
    
    results = []
    
    for module_name, test_function in test_modules:
        print(f"\n{'='*20} {module_name} {'='*20}")
        try:
            result = test_function()
            results.append((module_name, result))
        except Exception as e:
            print(f"✗ Критическая ошибка в модуле {module_name}: {e}")
            results.append((module_name, False))
    
    # Вывод итоговых результатов
    print("\n" + "=" * 60)
    print("ИТОГИ ТЕСТИРОВАНИЯ:")
    print("=" * 60)
    
    success_count = 0
    for module_name, result in results:
        status = "✅ УСПЕХ" if result else "❌ ОШИБКА"
        print(f"{module_name}: {status}")
        if result:
            success_count += 1
    
    print(f"\nУспешно пройдено: {success_count}/{len(results)}")
    
    if success_count == len(results):
        print("\n🎉 ВСЕ ТЕСТЫ УСПЕШНО ПРОЙДЕНЫ!")
        return True
    else:
        print(f"\n⚠️  ПРОЙДЕНО ТОЛЬКО {success_count} ИЗ {len(results)} ТЕСТОВ")
        return False

if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
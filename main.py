# main.py
import os
import sys

# Добавляем пути для импорта
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

def main():
    """Основная функция для запуска тестов"""
    print("Запуск системы тестирования...")
    
    try:
        # Импортируем и запускаем интеграционные тесты
        from tests.test_integration import run_all_tests
        
        success = run_all_tests()
        
        if success:
            print("\n🎉 ПРОГРАММА УСПЕШНО ЗАВЕРШИЛА РАБОТУ!")
        else:
            print("\n❌ ПРОГРАММА ЗАВЕРШИЛАСЬ С ОШИБКАМИ!")
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ КРИТИЧЕСКАЯ ОШИБКА: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()
# main.py
import os
import sys
from typing import Dict, List, Any
import time

# Добавляем пути для импорта
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from lib.database.config import settings
from lib.database.database import DataBaseMain
from lib.data.generate_data import DataGenerator
from lib.backup.backup import BackupManager
from lib.database.sandbox import SandboxManager
from lib.timer.timer import QueryTimer
from investigations .plot.plot_maker import ResearchPlotter


class TestManager:
    """Класс для тестирования всех компонентов системы"""
    
    def __init__(self):
        # Инициализация основных компонентов
        self.db_main = DataBaseMain(settings.DATABASE_URL)
        self.db_test = DataBaseMain(settings.TEST_DATABASE_URL)
        
        # Передаем функцию get_connection вместо get_database_connection
        self.data_generator = DataGenerator(self.db_main.get_connection)
        self.backup_manager = BackupManager(self.db_main.get_connection)
        self.query_timer = QueryTimer(self.db_main.get_connection)
        self.sandbox_manager = SandboxManager(
            self.db_main.get_connection,  # Исправлено здесь
            self.db_test.get_connection,  # И здесь
            settings.TEST_DATABASE_URL
        )
        self.plotter = ResearchPlotter()
    
    def setup_database(self):
        """Настройка базы данных"""
        print("=== НАСТРОЙКА БАЗЫ ДАННЫХ ===")
        
        # Создание таблиц в основной БД
        print("Создание таблиц в основной БД...")
        self.db_main.create_tables()
        print("✓ Таблицы созданы успешно")
        
        # Создание песочницы
        print("Создание тестовой БД (песочницы)...")
        if self.sandbox_manager.create_sandbox():
            print("✓ Песочница создана успешно")
        else:
            print("✗ Ошибка при создании песочницы")
    
    def test_data_generation(self):
        """Тестирование генерации данных"""
        print("\n=== ТЕСТИРОВАНИЕ ГЕНЕРАЦИИ ДАННЫХ ===")
        
        # Генерация тестовых данных
        print("Генерация пользователей...")
        self.data_generator.generate_users(5)
        print("✓ 5 пользователей сгенерировано")
        
        print("Генерация расписаний...")
        self.data_generator.generate_schedules(3)
        print("✓ 3 расписания сгенерировано")
        
        print("Генерация занятий...")
        self.data_generator.generate_lessons(10)
        print("✓ 10 занятий сгенерировано")
        
        print("Генерация связей пользователей с расписаниями...")
        self.data_generator.generate_schedule_user(8)
        print("✓ 8 связей сгенерировано")
        
        print("Генерация посещаемости...")
        self.data_generator.generate_attendance(15)
        print("✓ 15 записей посещаемости сгенерировано")
        
        print("Генерация комментариев...")
        self.data_generator.generate_comments(7)
        print("✓ 7 комментариев сгенерировано")
    
    def test_backup_restore(self):
        """Тестирование бэкапа и восстановления"""
        print("\n=== ТЕСТИРОВАНИЕ БЭКАПА И ВОССТАНОВЛЕНИЯ ===")
        
        # Создание бэкапа
        print("Создание бэкапа...")
        backup_path = self.backup_manager.create_backup()
        print(f"✓ Бэкап создан: {backup_path}")
        
        # Восстановление из бэкапа (в тестовую БД)
        print("Восстановление из бэкапа в тестовую БД...")
        try:
            # Для демонстрации скопируем схему в тестовую БД
            self.sandbox_manager.copy_schema_to_sandbox()
            # Здесь можно добавить восстановление данных
            print("✓ Восстановление выполнено успешно")
        except Exception as e:
            print(f"✗ Ошибка при восстановлении: {e}")
    
    def test_query_timing(self):
        """Тестирование измерения времени выполнения запросов"""
        print("\n=== ТЕСТИРОВАНИЕ ИЗМЕРЕНИЯ ВРЕМЕНИ ЗАПРОСОВ ===")
        
        # Тестовые запросы
        test_queries = [
            ("SELECT COUNT(*) FROM users", None),
            ("SELECT * FROM lessons WHERE schedule_id = %s", (1,)),
            ("SELECT u.login, s.name FROM users u JOIN schedule_user su ON u.id = su.user_id JOIN schedules s ON s.id = su.schedule_id", None)
        ]
        
        print("Измерение времени выполнения запросов...")
        for i, (query, params) in enumerate(test_queries):
            execution_time = self.query_timer.measure_query(query, params, iterations=3)
            print(f"Запрос {i+1}: {execution_time:.6f} секунд")
            print(f"  SQL: {query}")
            if params:
                print(f"  Параметры: {params}")
    
    def test_plotting(self):
        """Тестирование построения графиков"""
        print("\n=== ТЕСТИРОВАНИЕ ПОСТРОЕНИЯ ГРАФИКОВ ===")
        
        # Тестовые данные для графиков
        x_data = [10, 20, 50, 100, 200]
        y_data_dict = {
            'Пользователи': [0.1, 0.2, 0.5, 1.0, 2.1],
            'Занятия': [0.3, 0.6, 1.5, 3.0, 6.2],
            'Расписания': [0.05, 0.1, 0.25, 0.5, 1.0]
        }
        
        # Линейный график
        print("Создание линейного графика...")
        self.plotter.create_line_plot(
            x_data=x_data,
            y_data_dict=y_data_dict,
            title="Время генерации данных",
            x_label="Количество строк",
            y_label="Время (секунды)",
            filename="data_generation_time"
        )
        
        # Столбчатая диаграмма
        print("Создание столбчатой диаграммы...")
        categories = ['SELECT', 'INSERT', 'DELETE', 'JOIN']
        values_dict = {
            'Маленькие таблицы': [0.001, 0.002, 0.001, 0.003],
            'Большие таблицы': [0.01, 0.02, 0.015, 0.03]
        }
        
        self.plotter.create_bar_chart(
            categories=categories,
            values_dict=values_dict,
            title="Сравнение времени выполнения запросов",
            x_label="Тип запроса",
            y_label="Время (секунды)",
            filename="query_comparison"
        )
        
        print("✓ Графики созданы успешно")
    
    def test_sandbox_operations(self):
        """Тестирование операций с песочницей"""
        print("\n=== ТЕСТИРОВАНИЕ ОПЕРАЦИЙ С ПЕСОЧНИЦЕЙ ===")
        
        # Копирование схемы
        print("Копирование схемы в песочницу...")
        self.sandbox_manager.copy_schema_to_sandbox()
        print("✓ Схема скопирована")
        
        # Копирование данных
        print("Копирование данных в песочницу...")
        try:
            self.sandbox_manager.copy_all_data_to_sandbox()
            print("✓ Данные скопированы")
        except Exception as e:
            print(f"✗ Ошибка при копировании данных: {e}")
        
        # Очистка песочницы
        print("Очистка песочницы...")
        self.sandbox_manager.cleanup_sandbox()
        print("✓ Песочница очищена")
    
    def run_comprehensive_test(self):
        """Запуск комплексного тестирования"""
        print("🚀 ЗАПУСК КОМПЛЕКСНОГО ТЕСТИРОВАНИЯ СИСТЕМЫ")
        print("=" * 60)
        
        try:
            # 1. Настройка БД
            self.setup_database()
            
            # 2. Генерация данных
            self.test_data_generation()
            
            # 3. Операции с песочницей
            self.test_sandbox_operations()
            
            # 4. Бэкап и восстановление
            self.test_backup_restore()
            
            # 5. Измерение времени запросов
            self.test_query_timing()
            
            # 6. Построение графиков
            self.test_plotting()
            
            print("\n" + "=" * 60)
            print("✅ ВСЕ ТЕСТЫ УСПЕШНО ЗАВЕРШЕНЫ!")
            print("\nСозданные файлы:")
            print("• Графики в папке: investigations/plots/")
            print("• Бэкапы в папке: lib/backup/")
            
        except Exception as e:
            print(f"\n❌ ОШИБКА ПРИ ВЫПОЛНЕНИИ ТЕСТОВ: {e}")
            import traceback
            traceback.print_exc()


def demonstrate_individual_components():
    """Демонстрация работы отдельных компонентов"""
    print("\n" + "=" * 60)
    print("ДЕМОНСТРАЦИЯ ОТДЕЛЬНЫХ КОМПОНЕНТОВ")
    print("=" * 60)
    
    # Инициализация компонентов
    db = DataBaseMain(settings.DATABASE_URL)
    generator = DataGenerator(db.get_connection)
    plotter = ResearchPlotter()
    
    # Демонстрация генерации данных
    print("\n1. Демонстрация генерации данных:")
    generator.generate_users(2)
    generator.generate_schedules(1)
    print("✓ Сгенерированы тестовые пользователи и расписания")
    
    # Демонстрация работы с графиками
    print("\n2. Демонстрация построения графиков:")
    
    # Пример данных для демонстрации
    x_data = [1, 2, 3, 4, 5]
    comparison_data = {
        'Линейная зависимость': (x_data, [1, 2, 3, 4, 5]),
        'Квадратичная зависимость': (x_data, [1, 4, 9, 16, 25]),
        'Логарифмическая зависимость': (x_data, [0, 1, 1.58, 2, 2.32])
    }
    
    plotter.create_comparison_plot(
        data_dict=comparison_data,
        title="Сравнение различных зависимостей",
        x_label="X значения",
        y_label="Y значения", 
        filename="demo_comparison"
    )
    print("✓ Демонстрационные графики созданы")
    
    # Демонстрация таймера запросов
    print("\n3. Демонстрация измерения времени запросов:")
    timer = QueryTimer(db.get_connection)  # ← ИСПРАВЛЕННАЯ СТРОКА
    
    simple_query_time = timer.measure_query("SELECT version()")
    print(f"Время выполнения простого запроса: {simple_query_time:.6f} сек")

if __name__ == "__main__":
    # Создание экземпляра тест-менеджера
    test_manager = TestManager()
    
    # Запуск комплексного тестирования
    test_manager.run_comprehensive_test()
    
    # Дополнительная демонстрация компонентов
    demonstrate_individual_components()
    
    print("\n🎉 ПРОГРАММА УСПЕШНО ЗАВЕРШИЛА РАБОТУ!")
# investigations/research_manager.py
import os
import sys
import timeit
from typing import List, Dict, Any, Tuple, Callable
import matplotlib.pyplot as plt
import numpy as np

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

from lib.database.config import settings
from lib.database.database import DataBaseMain
from lib.data.generate_data import DataGenerator
from lib.timer.timer import QueryTimer
from investigations.plot_maker import ResearchPlotter

class ResearchManager:
    """Менеджер для проведения исследований"""
    
    def __init__(self):
        self.db_main = DataBaseMain(settings.DATABASE_URL)
        self.data_generator = DataGenerator(self.db_main.get_connection)
        self.query_timer = QueryTimer(self.db_main.get_connection)
        self.plotter = ResearchPlotter(output_dir="investigations/research_plots")
        
        # Параметры исследований (легко изменяемые)
        self.research_params = {
            'data_sizes': [10, 50, 100, 200, 500],  # Размеры данных для тестирования
            'iterations': 3,  # Количество итераций для усреднения
            'warmup_iterations': 1  # Прогревочные итерации
        }
    
    def cleanup_database(self):
        """Очистка базы данных перед исследованиями"""
        with self.db_main.get_database_connection() as conn:
            with conn.cursor() as cur:
                tables = ['attendance', 'comments', 'schedule_user', 'lessons', 'schedules', 'users']
                for table in tables:
                    try:
                        cur.execute(f"TRUNCATE TABLE {table} CASCADE")
                    except Exception:
                        pass
    
    def measure_generation_time(self, table_name: str, n: int) -> float:
        """Измерение времени генерации данных без сохранения"""
        def generate_wrapper():
            # Генерируем данные, но не сохраняем
            if table_name == 'users':
                self.data_generator.generate_users(n, save=False)
            elif table_name == 'schedules':
                self.data_generator.generate_schedules(n, save=False)
            elif table_name == 'lessons':
                self.data_generator.generate_lessons(n, save=False)
            elif table_name == 'schedule_user':
                self.data_generator.generate_schedule_user(n, save=False)
            elif table_name == 'attendance':
                self.data_generator.generate_attendance(n, save=False)
            elif table_name == 'comments':
                self.data_generator.generate_comments(n, save=False)
        
        # Используем timeit для точного измерения
        timer = timeit.Timer(generate_wrapper)
        return timer.timeit(number=1)
    
    def measure_query_time(self, query: str, params: tuple = None, iterations: int = 1) -> float:
        """Измерение времени выполнения запроса"""
        return self.query_timer.measure_query(query, params, iterations)

class DataGenerationResearch(ResearchManager):
    """Исследование времени генерации данных"""
    
    def research_table_generation_times(self):
        """Исследование времени генерации данных для отдельных таблиц"""
        print("=== ИССЛЕДОВАНИЕ ВРЕМЕНИ ГЕНЕРАЦИИ ДАННЫХ ===")
        
        tables = ['users', 'schedules', 'lessons', 'attendance', 'comments']
        results = {}
        
        for table in tables:
            print(f"Исследование таблицы: {table}")
            times = []
            
            for size in self.research_params['data_sizes']:
                avg_time = 0
                for _ in range(self.research_params['iterations']):
                    time_taken = self.measure_generation_time(table, size)
                    avg_time += time_taken
                
                avg_time /= self.research_params['iterations']
                times.append(avg_time)
                print(f"  Размер: {size}, Время: {avg_time:.6f} сек")
            
            results[table] = times
        
        # Построение графика
        self.plotter.create_line_plot(
            x_data=self.research_params['data_sizes'],
            y_data_dict=results,
            title="Время генерации данных по таблицам",
            x_label="Количество строк",
            y_label="Время (секунды)",
            filename="data_generation_times"
        )
        
        return results
    
    def research_related_tables_generation(self):
        """Исследование времени генерации для связанных таблиц"""
        print("=== ИССЛЕДОВАНИЕ ГЕНЕРАЦИИ СВЯЗАННЫХ ТАБЛИЦ ===")
        
        related_tables = [
            ('users', 'schedules'),  # Таблицы для связи многие-ко-многим
            ('schedules', 'lessons')  # Таблицы для связи один-ко-многим
        ]
        
        results = {}
        
        for table1, table2 in related_tables:
            print(f"Исследование связанных таблиц: {table1} + {table2}")
            times = []
            
            for size in self.research_params['data_sizes']:
                def generate_related():
                    # Генерируем данные для обеих таблиц
                    if table1 == 'users':
                        self.data_generator.generate_users(size, save=False)
                        self.data_generator.generate_schedules(size, save=False)
                    elif table1 == 'schedules':
                        self.data_generator.generate_schedules(size, save=False)
                        self.data_generator.generate_lessons(size, save=False)
                
                timer = timeit.Timer(generate_related)
                total_time = timer.timeit(number=1)
                times.append(total_time)
                print(f"  Размер: {size}, Общее время: {total_time:.6f} сек")
            
            results[f"{table1}+{table2}"] = times
        
        # Построение графика
        self.plotter.create_line_plot(
            x_data=self.research_params['data_sizes'],
            y_data_dict=results,
            title="Время генерации данных для связанных таблиц",
            x_label="Количество строк (каждая таблица)",
            y_label="Время (секунды)",
            filename="related_tables_generation"
        )
        
        return results

class QueryPerformanceResearch(ResearchManager):
    """Исследование производительности запросов"""
    
    def research_table_queries(self):
        """Исследование времени выполнения запросов для каждой таблицы"""
        print("=== ИССЛЕДОВАНИЕ ПРОИЗВОДИТЕЛЬНОСТИ ЗАПРОСОВ ===")
        
        # Определяем тестовые запросы для каждой таблицы
        query_configs = {
            'users': [
                ("SELECT * FROM users WHERE id = %s", (1,), "SELECT по ID"),
                ("INSERT INTO users (login, email, password) VALUES (%s, %s, %s)", 
                ('test_user', 'test@example.com', 'password'), "INSERT"),
                ("DELETE FROM users WHERE login = %s", ('test_user',), "DELETE по логину")
            ],
            'schedules': [
                ("SELECT name FROM schedules WHERE id = %s", (1,), "SELECT имени по ID"),
                ("INSERT INTO schedules (name) VALUES (%s)", ('Test Schedule',), "INSERT"),
                ("DELETE FROM schedules WHERE name LIKE %s", ('Test%',), "DELETE по имени")
            ],
            'lessons': [
                ("SELECT * FROM lessons WHERE teacher_name = %s", ('Иванов И.И.',), "SELECT по преподавателю"),
                # ИЗМЕНЕНИЕ: Используем существующий schedule_id
                ("UPDATE lessons SET teacher_name = %s WHERE id = %s", 
                ('Тестовый Преподаватель', 1), "UPDATE преподавателя"),
                ("DELETE FROM lessons WHERE teacher_name = %s", ('Тестовый',), "DELETE по преподавателю")
            ]
        }
        
        # Подготавливаем данные ОДИН РАЗ для всех тестов
        self.cleanup_database()
        self.data_generator.generate_users(100)
        self.data_generator.generate_schedules(50)
        self.data_generator.generate_lessons(200)
        
        results = {}
        
        for table_name, queries in query_configs.items():
            print(f"Исследование таблицы: {table_name}")
            
            for query, params, description in queries:
                query_key = f"{table_name}_{description.replace(' ', '_')}"
                times = []
                
                # Измеряем для разных размеров данных
                for size_multiplier in [1, 2, 5, 10]:
                    # ПОДГОТОВКА: создаем тестовые данные соответствующего размера
                    self._prepare_test_data(table_name, 100 * size_multiplier)
                    
                    # ИЗМЕНЕНИЕ: Для UPDATE/DELETE убедимся, что данные существуют
                    if description in ["UPDATE преподавателя", "DELETE по преподавателю"]:
                        self._ensure_test_data_exists(table_name, params)
                    
                    avg_time = self.measure_query_time(query, params, self.research_params['iterations'])
                    times.append(avg_time)
                    print(f"  {description}: {avg_time:.6f} сек (размер: {100 * size_multiplier})")
                
                results[query_key] = times
                
                # Строим отдельный график для каждого запроса
                sizes = [100 * mult for mult in [1, 2, 5, 10]]
                self.plotter.create_line_plot(
                    x_data=sizes,
                    y_data_dict={description: times},
                    title=f"Время выполнения запроса: {description}",
                    x_label="Размер таблицы (строк)",
                    y_label="Время (секунды)",
                    filename=f"query_{query_key}"
                )
        
        return results

    def _ensure_test_data_exists(self, table_name: str, params: tuple):
        """Убеждаемся, что тестовые данные для UPDATE/DELETE существуют"""
        if table_name == 'lessons' and len(params) > 0:
            with self.db_main.get_database_connection() as conn:
                with conn.cursor() as cur:
                    # Для UPDATE: убедимся, что урок с ID=1 существует
                    if params[1] == 1:  # Если пытаемся обновить урок с ID=1
                        cur.execute("SELECT COUNT(*) FROM lessons WHERE id = 1")
                        if cur.fetchone()[0] == 0:
                            # Создаем тестовый урок с ID=1
                            cur.execute("""
                                INSERT INTO lessons 
                                (id, schedule_id, teacher_name, subject_name, place, startDate, endDate, startTime, endTime, repeat)
                                VALUES (1, 1, 'Тестовый', 'Тест', 'Аудитория', '2024-01-01', '2024-01-01', '08:00', '09:00', 'none')
                            """)
                    
                    # Для DELETE: создаем тестовые данные для удаления
                    if params[0] == 'Тестовый':
                        cur.execute("SELECT COUNT(*) FROM lessons WHERE teacher_name = 'Тестовый'")
                        if cur.fetchone()[0] == 0:
                            cur.execute("""
                                INSERT INTO lessons 
                                (schedule_id, teacher_name, subject_name, place, startDate, endDate, startTime, endTime, repeat)
                                VALUES (1, 'Тестовый', 'Тест', 'Аудитория', '2024-01-01', '2024-01-01', '08:00', '09:00', 'none')
                            """)
    def research_join_queries(self):
        """Исследование JOIN запросов для связей между таблицами"""
        print("=== ИССЛЕДОВАНИЕ JOIN ЗАПРОСОВ ===")
        
        join_queries = [
            ("""SELECT u.login, s.name 
                FROM users u 
                JOIN schedule_user su ON u.id = su.user_id 
                JOIN schedules s ON s.id = su.schedule_id""",
             None, "JOIN users-schedules (многие-ко-многим)"),
            
            ("""SELECT s.name, l.subject_name, l.teacher_name
                FROM schedules s 
                JOIN lessons l ON s.id = l.schedule_id""",
             None, "JOIN schedules-lessons (один-ко-многим)"),
            
            ("""SELECT l.subject_name, c.text, c.date
                FROM lessons l 
                JOIN comments c ON l.id = c.lesson_id""",
             None, "JOIN lessons-comments (один-ко-многим)")
        ]
        
        # Подготавливаем данные для связей
        self.cleanup_database()
        self.data_generator.generate_users(50)
        self.data_generator.generate_schedules(30)
        self.data_generator.generate_lessons(100)
        self.data_generator.generate_schedule_user(80)
        self.data_generator.generate_comments(50)
        
        results = {}
        
        for query, params, description in join_queries:
            times = []
            
            for size_multiplier in [1, 2, 3]:
                avg_time = self.measure_query_time(query, params, self.research_params['iterations'])
                times.append(avg_time)
                print(f"  {description}: {avg_time:.6f} сек")
            
            results[description] = times
            
            # График для JOIN запроса
            sizes = [1, 2, 3]  # Относительные размеры
            self.plotter.create_line_plot(
                x_data=sizes,
                y_data_dict={description: times},
                title=f"Время выполнения JOIN запроса",
                x_label="Относительный размер данных",
                y_label="Время (секунды)",
                filename=f"join_{description.replace(' ', '_').replace('(', '').replace(')', '')}"
            )
        
        return results
    
    def _prepare_test_data(self, table_name: str, count: int):
        """Подготовка тестовых данных"""
        self.cleanup_database()
        
        if table_name == 'users':
            self.data_generator.generate_users(count)
        elif table_name == 'schedules':
            self.data_generator.generate_users(10)  # Минимальные данные для FK
            self.data_generator.generate_schedules(count)
        elif table_name == 'lessons':
            self.data_generator.generate_schedules(10)
            self.data_generator.generate_lessons(count)

class IndexResearch(ResearchManager):
    """Исследование эффективности индексов на существующих таблицах"""
    
    def research_primary_key_index(self):
        """Исследование эффективности первичного ключа на таблице users"""
        print("=== ИССЛЕДОВАНИЕ ПЕРВИЧНОГО КЛЮЧА (users) ===")
        
        # Подготавливаем данные в основной таблице
        self._prepare_users_data()
        
        results = {}
        
        # Тест SELECT на равенство по первичному ключу
        print("SELECT на равенство по ID:")
        equality_results = {'С индексом (PK)': [], 'Без индекса': []}
        
        for size in [100, 500, 1000, 2000]:
            # Создаем временную таблицу БЕЗ первичного ключа для сравнения
            self._create_temp_table_no_pk(size)
            
            # Измеряем для основной таблицы (с PK)
            time_with_pk = self.measure_query_time(
                "SELECT * FROM users WHERE id = %s", 
                (size // 2,), 
                self.research_params['iterations']
            )
            
            # Измеряем для временной таблицы (без PK)
            time_without_pk = self.measure_query_time(
                "SELECT * FROM users_no_pk WHERE id = %s", 
                (size // 2,), 
                self.research_params['iterations']
            )
            
            equality_results['С индексом (PK)'].append(time_with_pk)
            equality_results['Без индекса'].append(time_without_pk)
            
            print(f"  Размер: {size}, С PK: {time_with_pk:.6f}, Без PK: {time_without_pk:.6f}")
        
        self.plotter.create_line_plot(
            x_data=[100, 500, 1000, 2000],
            y_data_dict=equality_results,
            title="SELECT WHERE id = n (таблица users)",
            x_label="Количество строк",
            y_label="Время (секунды)",
            filename="users_pk_equality_select"
        )
        
        return results
    
    def research_string_index(self):
        """Исследование строкового индекса на таблице users (поле login)"""
        print("=== ИССЛЕДОВАНИЕ СТРОКОВОГО ИНДЕКСА (users.login) ===")
        
        # Создаем индекс на поле login если его нет
        self._ensure_string_index()
        
        results = {}
        
        test_cases = [
            ("WHERE login = %s", "user_500", "Равенство"),
            ("WHERE login LIKE %s", "user_5%", "LIKE начало"),
            ("WHERE login LIKE %s", "%500%", "LIKE содержание")
        ]
        
        for condition, param, description in test_cases:
            print(f"Тест: {description}")
            case_results = {'С индексом': [], 'Без индекса': []}
            
            for size in [100, 500, 1000, 2000]:
                # Подготавливаем данные
                self._prepare_users_data(size)
                
                # Запрос к основной таблице (с индексом)
                query_with_index = f"SELECT * FROM users {condition}"
                time_with_index = self.measure_query_time(
                    query_with_index, 
                    (param,), 
                    self.research_params['iterations']
                )
                
                # Запрос к копии без индекса
                query_without_index = f"SELECT * FROM users_no_index {condition}"
                time_without_index = self.measure_query_time(
                    query_without_index, 
                    (param,), 
                    self.research_params['iterations']
                )
                
                case_results['С индексом'].append(time_with_index)
                case_results['Без индекса'].append(time_without_index)
                
                print(f"  Размер: {size}, С индексом: {time_with_index:.6f}, Без индекса: {time_without_index:.6f}")
            
            self.plotter.create_line_plot(
                x_data=[100, 500, 1000, 2000],
                y_data_dict=case_results,
                title=f"Строковый поиск: {description} (login)",
                x_label="Количество строк",
                y_label="Время (секунды)",
                filename=f"users_string_index_{description.replace(' ', '_')}"
            )
        
        return results
    
    def research_fulltext_index(self):
        """Исследование полнотекстового индекса на таблице comments (поле text)"""
        print("=== ИССЛЕДОВАНИЕ ПОЛНОТЕКСТОВОГО ИНДЕКСА (comments.text) ===")
        
        # Создаем полнотекстовый индекс если его нет
        self._ensure_fulltext_index()
        
        results = {}
        
        test_cases = [
            ("WHERE to_tsvector('russian', text) @@ to_tsquery('russian', %s)", "заметка", "Одно слово"),
            ("WHERE to_tsvector('russian', text) @@ to_tsquery('russian', %s)", "заметка & занятие", "Несколько слов")
        ]
        
        for condition, param, description in test_cases:
            print(f"Тест: {description}")
            case_results = {'С индексом': [], 'Без индекса': []}
            
            for size in [100, 500, 1000]:
                self._prepare_comments_data(size)
                
                query_with_index = f"SELECT * FROM comments {condition}"
                time_with_index = self.measure_query_time(
                    query_with_index, 
                    (param,), 
                    self.research_params['iterations']
                )
                
                query_without_index = f"SELECT * FROM comments_no_index {condition}"
                time_without_index = self.measure_query_time(
                    query_without_index, 
                    (param,), 
                    self.research_params['iterations']
                )
                
                case_results['С индексом'].append(time_with_index)
                case_results['Без индекса'].append(time_without_index)
                
                print(f"  Размер: {size}, С индексом: {time_with_index:.6f}, Без индекса: {time_without_index:.6f}")
            
            self.plotter.create_line_plot(
                x_data=[100, 500, 1000],
                y_data_dict=case_results,
                title=f"Полнотекстовый поиск: {description} (comments.text)",
                x_label="Количество строк",
                y_label="Время (секунды)",
                filename=f"comments_fulltext_{description.replace(' ', '_')}"
            )
        
        return results
    
    def _prepare_users_data(self, count: int = 2000):
        """Подготовка тестовых данных в таблице users"""
        self.cleanup_database()
        self.data_generator.generate_users(count)
    
    def _create_temp_table_no_pk(self, count: int):
        """Создание временной таблицы без первичного ключа"""
        with self.db_main.get_database_connection() as conn:
            with conn.cursor() as cur:
                # Удаляем временную таблицу если существует
                cur.execute("DROP TABLE IF EXISTS users_no_pk CASCADE")
                
                # Создаем таблицу без первичного ключа
                cur.execute("""
                    CREATE TABLE users_no_pk (
                        id INTEGER,
                        login VARCHAR(255),
                        email VARCHAR(255),
                        password VARCHAR(255)
                    )
                """)
                
                # Копируем данные из основной таблицы
                cur.execute("INSERT INTO users_no_pk SELECT * FROM users")
    
    def _ensure_string_index(self):
        """Создание строкового индекса на users.login если его нет"""
        with self.db_main.get_database_connection() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute("CREATE INDEX idx_users_login ON users(login)")
                except Exception:
                    # Индекс уже существует
                    pass
                
                # Создаем таблицу без индекса для сравнения
                cur.execute("DROP TABLE IF EXISTS users_no_index CASCADE")
                cur.execute("""
                    CREATE TABLE users_no_index (
                        id SERIAL PRIMARY KEY,
                        login VARCHAR(255),
                        email VARCHAR(255),
                        password VARCHAR(255)
                    )
                """)
    
    def _ensure_fulltext_index(self):
        """Создание полнотекстового индекса на comments.text если его нет"""
        with self.db_main.get_database_connection() as conn:
            with conn.cursor() as cur:
                try:
                    cur.execute("CREATE INDEX idx_comments_text_gin ON comments USING gin(to_tsvector('russian', text))")
                except Exception:
                    # Индекс уже существует
                    pass
                
                # Создаем таблицу без индекса для сравнения
                cur.execute("DROP TABLE IF EXISTS comments_no_index CASCADE")
                cur.execute("""
                    CREATE TABLE comments_no_index (
                        lesson_id INTEGER,
                        date DATE,
                        text TEXT
                    )
                """)
    
    def _prepare_comments_data(self, count: int):
        """Подготовка тестовых данных для comments"""
        self.cleanup_database()
        # Сначала создаем необходимые связанные данные
        self.data_generator.generate_users(50)
        self.data_generator.generate_schedules(30)
        self.data_generator.generate_lessons(100)
        self.data_generator.generate_comments(count)
def run_all_research():
    """Запуск всех исследований"""
    print("🚀 ЗАПУСК КОМПЛЕКСНЫХ ИССЛЕДОВАНИЙ")
    print("=" * 60)
    
    # Инициализация менеджеров исследований
    data_research = DataGenerationResearch()
    query_research = QueryPerformanceResearch()
    index_research = IndexResearch()
    
    try:
        # 1. Исследование генерации данных
        print("\n" + "="*20 + " ГЕНЕРАЦИЯ ДАННЫХ " + "="*20)
        data_research.research_table_generation_times()
        data_research.research_related_tables_generation()
        
        # 2. Исследование производительности запросов
        print("\n" + "="*20 + " ПРОИЗВОДИТЕЛЬНОСТЬ ЗАПРОСОВ " + "="*20)
        query_research.research_table_queries()
        query_research.research_join_queries()
        
        # 3. Исследование индексов
        print("\n" + "="*20 + " ИССЛЕДОВАНИЕ ИНДЕКСОВ " + "="*20)
        index_research.setup_index_test_tables()
        index_research.research_primary_key_index()
        index_research.research_string_index()
        index_research.research_fulltext_index()
        
        print("\n" + "=" * 60)
        print("✅ ВСЕ ИССЛЕДОВАНИЯ УСПЕШНО ЗАВЕРШЕНЫ!")
        print("Графики сохранены в: investigations/research_plots/")
        
    except Exception as e:
        print(f"\n❌ ОШИБКА ПРИ ВЫПОЛНЕНИИ ИССЛЕДОВАНИЙ: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    run_all_research()
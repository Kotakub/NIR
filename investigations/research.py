import sys
import os
import time
import timeit


sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


from lib.database.database import DataBaseMain
from lib.database.config import settings
from lib.data.generate_data import DataGenerator
from lib.timer.timer import QueryTimer
from plot_maker import ResearchPlotter
import datetime
import time

class IndexResearch:
    def __init__(self, db: DataBaseMain):
        self.db = db
        self.plotter = ResearchPlotter()
        self.timer = QueryTimer(db.get_database_connection)
        self.data_gen = DataGenerator(db.get_database_connection)
    
    def research_data_generation_time(self):
        """Исследование времени генерации данных для всех таблиц"""
        print("=== Исследование времени генерации данных ===")
        
        table_sizes = [100, 500, 1000, 2000, 5000]
        results = {
            'users': [],
            'schedules': [], 
            'lessons': [],
            'comments': [],
            'attendance': [],
            'schedule_user': []
        }
        
        for size in table_sizes:
            print(f"Тестирование генерации {size} записей...")
            
            # Измеряем время генерации для каждой таблицы
            for table_name in results.keys():
                def generate_func():
                    if table_name == 'users':
                        return self.data_gen.generate_users(size, save=False)
                    elif table_name == 'schedules':
                        return self.data_gen.generate_schedules(size, save=False)
                    elif table_name == 'lessons':
                        return self.data_gen.generate_lessons(size, save=False)
                    elif table_name == 'comments':
                        return self.data_gen.generate_comments(size, save=False)
                    elif table_name == 'attendance':
                        return self.data_gen.generate_attendance(size, save=False)
                    elif table_name == 'schedule_user':
                        return self.data_gen.generate_schedule_user(size, save=False)
                
                time_taken = timeit.timeit(generate_func, number=1)
                results[table_name].append(time_taken)
                print(f"  {table_name}: {time_taken:.4f} сек")
        
        # Построение графика
        self.plotter.create_line_plot(
            table_sizes,
            results,
            "Время генерации данных для таблиц",
            "Количество записей",
            "Время генерации (сек)",
            "data_generation_time"
        )
    
    def research_related_tables_generation(self):
        """Исследование времени генерации данных для связанных таблиц"""
        print("=== Исследование генерации связанных таблиц ===")
        
        table_sizes = [100, 500, 1000, 2000]
        results = {
            'users + schedules + schedule_user': [],
            'schedules + lessons': []
        }
        
        for size in table_sizes:
            print(f"Тестирование связанных таблиц для {size} записей...")
            
            # users + schedules + schedule_user (многие-ко-многим)
            def generate_many_to_many():
                users = self.data_gen.generate_users(size, save=False)
                schedules = self.data_gen.generate_schedules(size, save=False)
                
                # Вставляем данные чтобы получить ID
                self.data_gen.insert_data(users, "users")
                self.data_gen.insert_data(schedules, "schedules")
                
                # Генерируем связи
                schedule_user = self.data_gen.generate_schedule_user(size, save=False)
                self.data_gen.insert_data(schedule_user, "schedule_user")
                
                # Очищаем для следующего теста
                self.data_gen.delete_data("schedule_user")
                self.data_gen.delete_data("users")
                self.data_gen.delete_data("schedules")
            
            time_many_to_many = timeit.timeit(generate_many_to_many, number=1)
            results['users + schedules + schedule_user'].append(time_many_to_many)
            
            # schedules + lessons (один-ко-многим)
            def generate_one_to_many():
                schedules = self.data_gen.generate_schedules(10, save=False)  # Меньше расписаний
                self.data_gen.insert_data(schedules, "schedules")
                
                lessons = self.data_gen.generate_lessons(size, save=False)
                self.data_gen.insert_data(lessons, "lessons")
                
                # Очищаем
                self.data_gen.delete_data("lessons")
                self.data_gen.delete_data("schedules")
            
            time_one_to_many = timeit.timeit(generate_one_to_many, number=1)
            results['schedules + lessons'].append(time_one_to_many)
            
            print(f"  Многие-ко-многим: {time_many_to_many:.4f} сек")
            print(f"  Один-ко-многим: {time_one_to_many:.4f} сек")
        
        # Построение графика
        self.plotter.create_line_plot(
            table_sizes,
            results,
            "Время генерации данных для связанных таблиц",
            "Количество записей",
            "Время генерации (сек)",
            "related_tables_generation"
        )
    
    def research_basic_queries_performance(self):
        """Исследование производительности базовых запросов"""
        print("=== Исследование производительности базовых запросов ===")
        
        # Подготавливаем данные
        self.data_gen.generate_users(1000)
        self.data_gen.generate_schedules(10)
        self.data_gen.generate_lessons(500)
        self.data_gen.generate_comments(2000)
        
        table_sizes = [100, 500, 1000, 2000]
        
        # SELECT запросы
        select_results = {
            'SELECT * FROM users': [],
            'SELECT * FROM lessons WHERE schedule_id = 1': [],
            'SELECT * FROM comments WHERE text LIKE "%важное%"': []
        }
        
        # INSERT запросы  
        insert_results = {
            'INSERT INTO users': [],
            'INSERT INTO lessons': [],
            'INSERT INTO comments': []
        }
        
        # DELETE запросы
        delete_results = {
            'DELETE FROM users WHERE id < n': [],
            'DELETE FROM comments': [],
            'DELETE FROM schedule_user': []
        }
        
        for size in table_sizes:
            print(f"Тестирование запросов для {size} записей...")
            
            # SELECT запросы
            time_select1 = self.timer.measure_query("SELECT * FROM users", iterations=10)
            select_results['SELECT * FROM users'].append(time_select1)
            
            time_select2 = self.timer.measure_query(
                "SELECT * FROM lessons WHERE schedule_id = %s", 
                (1,), 
                iterations=10
            )
            select_results['SELECT * FROM lessons WHERE schedule_id = 1'].append(time_select2)
            
            time_select3 = self.timer.measure_query(
                "SELECT * FROM comments WHERE text LIKE %s",
                ('%важное%',),
                iterations=5
            )
            select_results['SELECT * FROM comments WHERE text LIKE "%важное%"'].append(time_select3)
            
            # INSERT запросы (измеряем вставку одного ряда)
            def insert_user():
                user = self.data_gen.generate_users(1, save=False)[0]
                self.data_gen.insert_data([user], "users")
            
            time_insert1 = timeit.timeit(insert_user, number=10) / 10
            insert_results['INSERT INTO users'].append(time_insert1)
            
            def insert_lesson():
                lesson = self.data_gen.generate_lessons(1, save=False)[0]
                self.data_gen.insert_data([lesson], "lessons")
            
            time_insert2 = timeit.timeit(insert_lesson, number=10) / 10
            insert_results['INSERT INTO lessons'].append(time_insert2)
            
            def insert_comment():
                comment = self.data_gen.generate_comments(1, save=False)[0]
                self.data_gen.insert_data([comment], "comments")
            
            time_insert3 = timeit.timeit(insert_comment, number=10) / 10
            insert_results['INSERT INTO comments'].append(time_insert3)
            
            # DELETE запросы
            time_delete1 = self.timer.measure_query(
                "DELETE FROM users WHERE id < %s",
                (size//2,),
                iterations=1
            )
            delete_results['DELETE FROM users WHERE id < n'].append(time_delete1)
            
            # Восстанавливаем удаленные данные
            self.data_gen.generate_users(size//2)
            
            time_delete2 = self.timer.measure_query("DELETE FROM comments", iterations=1)
            delete_results['DELETE FROM comments'].append(time_delete2)
            
            time_delete3 = self.timer.measure_query("DELETE FROM schedule_user", iterations=1)
            delete_results['DELETE FROM schedule_user'].append(time_delete3)
            
            print(f"  SELECT: {time_select1:.4f}, INSERT: {time_insert1:.4f}, DELETE: {time_delete1:.4f} сек")
        
        # Восстанавливаем исходные данные
        self.data_gen.delete_data("comments")
        self.data_gen.generate_comments(2000)
        
        # Строим графики
        self.plotter.create_line_plot(
            table_sizes,
            select_results,
            "Производительность SELECT запросов",
            "Количество записей в таблице",
            "Время выполнения (сек)",
            "select_queries_performance"
        )
        
        self.plotter.create_line_plot(
            table_sizes,
            insert_results,
            "Производительность INSERT запросов",
            "Количество записей в таблице", 
            "Время выполнения (сек)",
            "insert_queries_performance"
        )
        
        self.plotter.create_line_plot(
            table_sizes,
            delete_results,
            "Производительность DELETE запросов",
            "Количество записей в таблице",
            "Время выполнения (сек)",
            "delete_queries_performance"
        )
    
    def research_join_queries(self):
        """Исследование запросов с объединением таблиц"""
        print("=== Исследование запросов с JOIN ===")
        
        # Подготавливаем данные
        self.data_gen.generate_users(1000)
        self.data_gen.generate_schedules(50)
        self.data_gen.generate_lessons(2000)
        self.data_gen.generate_schedule_user(500)
        
        table_sizes = [100, 500, 1000, 2000]
        join_results = {
            'INNER JOIN lessons-schedules': [],
            'LEFT JOIN users-schedule_user': [],
            '3-table JOIN': []
        }
        
        for size in table_sizes:
            print(f"Тестирование JOIN для {size} записей...")
            
            # INNER JOIN: lessons + schedules
            time_inner = self.timer.measure_query("""
                SELECT l.*, s.name 
                FROM lessons l 
                INNER JOIN schedules s ON l.schedule_id = s.id 
                LIMIT %s
            """, (size,), iterations=5)
            join_results['INNER JOIN lessons-schedules'].append(time_inner)
            
            # LEFT JOIN: users + schedule_user
            time_left = self.timer.measure_query("""
                SELECT u.login, su.schedule_id 
                FROM users u 
                LEFT JOIN schedule_user su ON u.id = su.user_id 
                LIMIT %s
            """, (size,), iterations=5)
            join_results['LEFT JOIN users-schedule_user'].append(time_left)
            
            # JOIN 3 таблиц: users + schedule_user + schedules
            time_three = self.timer.measure_query("""
                SELECT u.login, s.name 
                FROM users u 
                JOIN schedule_user su ON u.id = su.user_id 
                JOIN schedules s ON su.schedule_id = s.id 
                LIMIT %s
            """, (size,), iterations=3)
            join_results['3-table JOIN'].append(time_three)
            
            print(f"  INNER: {time_inner:.4f}, LEFT: {time_left:.4f}, 3-table: {time_three:.4f} сек")
        
        # Строим график
        self.plotter.create_line_plot(
            table_sizes,
            join_results,
            "Производительность JOIN запросов",
            "Количество возвращаемых записей",
            "Время выполнения (сек)",
            "join_queries_performance"
        )
    
    def __init__(self, db: DataBaseMain):
        self.db = db
        self.plotter = ResearchPlotter()
        self.timer = QueryTimer(db.get_database_connection)
        self.data_gen = DataGenerator(db.get_database_connection)
        
    def research_numeric_indexes(self):
        """Исследование числовых индексов (таблицы users)"""
        print("=== Исследование числовых индексов ===")
        
        # Создаем копию таблицы users без первичного ключа
        with self.db.get_database_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS users_no_pk (
                        id INTEGER,
                        login VARCHAR(255),
                        email VARCHAR(255),
                        password VARCHAR(255)
                    )
                """)
                cur.execute("TRUNCATE TABLE users_no_pk")
        
        table_sizes = [250, 500, 750, 1000, 1250, 1500, 1750, 2000]
        results_select_eq = {'users (с PK)': [], 'users_no_pk (без PK)': []}
        results_select_neq = {'users (с PK)': [], 'users_no_pk (без PK)': []}
        results_insert = {'users (с PK)': [], 'users_no_pk (без PK)': []}
        
        for size in table_sizes:
            print(f"Тестирование для {size} записей...")
            
            # Генерируем и заполняем данные
            users_data = self.data_gen.generate_users(size, save=False)
            
            self.data_gen.delete_data("users")
            self.data_gen.insert_data(users_data, "users")
            
            self.data_gen.delete_data("users_no_pk")
            self.data_gen.insert_data(users_data, "users_no_pk")
            
            # Тесты SELECT (оставляем как есть)
            time_t1_eq = self.timer.measure_query(
                "SELECT * FROM users WHERE id = %s", 
                (size // 2,), 
                iterations=100
            )
            time_t2_eq = self.timer.measure_query(
                "SELECT * FROM users_no_pk WHERE id = %s", 
                (size // 2,), 
                iterations=100
            )
            results_select_eq['users (с PK)'].append(time_t1_eq)
            results_select_eq['users_no_pk (без PK)'].append(time_t2_eq)
            
            time_t1_neq = self.timer.measure_query(
                "SELECT * FROM users WHERE id < %s", 
                (size // 2,), 
                iterations=10
            )
            time_t2_neq = self.timer.measure_query(
                "SELECT * FROM users_no_pk WHERE id < %s", 
                (size // 2,), 
                iterations=10
            )
            results_select_neq['users (с PK)'].append(time_t1_neq)
            results_select_neq['users_no_pk (без PK)'].append(time_t2_neq)
            
            # Тест INSERT - ручное измерение времени
            insert_iterations = 30
            total_t1_time = 0
            total_t2_time = 0
            
            for i in range(insert_iterations):
                # Генерируем уникального пользователя для T1
                test_user_t1 = self.data_gen.generate_users(1, save=False)[0]
                
                # Измеряем INSERT в T1
                start_time = time.time()
                self.data_gen.insert_data([test_user_t1], "users")
                total_t1_time += time.time() - start_time
                
                # Генерируем уникального пользователя для T2
                test_user_t2 = self.data_gen.generate_users(1, save=False)[0]
                test_user_t2['id'] = size + i + 1
                
                # Измеряем INSERT в T2
                start_time = time.time()
                self.data_gen.insert_data([test_user_t2], "users_no_pk")
                total_t2_time += time.time() - start_time
            
            results_insert['users (с PK)'].append(total_t1_time / insert_iterations)
            results_insert['users_no_pk (без PK)'].append(total_t2_time / insert_iterations)
        
        # Построение графиков - исправленные вызовы
        self.plotter.create_line_plot(
            table_sizes,
            results_select_eq,
            "SELECT по равенству (WHERE id = n)",
            "Количество записей в таблице",
            "Время выполнения (сек)",
            "numeric_select_equality"
        )
        
        self.plotter.create_line_plot(
            table_sizes,
            results_select_neq,
            "SELECT по неравенству (WHERE id < n)",
            "Количество записей в таблице",
            "Время выполнения (сек)",
            "numeric_select_inequality"
        )
        
        self.plotter.create_line_plot(
            table_sizes,
            results_insert,
            "INSERT в таблицы",
            "Количество записей в таблице",
            "Время выполнения (сек)",
            "numeric_insert"
        )       
        
    def research_string_indexes(self):
        """Исследование строковых индексов (таблицы users - поле login)"""
        print("=== Исследование строковых индексов ===")
        
        with self.db.get_database_connection() as conn:
            with conn.cursor() as cur:
                # Создаем T4 (users без индекса на login)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS users_no_login_idx (
                        id SERIAL PRIMARY KEY,
                        login VARCHAR(255),
                        email VARCHAR(255),
                        password VARCHAR(255)
                    )
                """)
                
                # Удаляем индекс если есть (для чистоты эксперимента)
                try:
                    cur.execute("DROP INDEX IF EXISTS idx_users_login")
                except:
                    pass
        
        # Создаем индекс на login в основной таблице
        with self.db.get_database_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    CREATE INDEX IF NOT EXISTS idx_users_login 
                    ON users(login)
                """)
        
        table_sizes = [250, 500, 750, 1000, 1250, 1500, 1750, 2000]
        results_select_eq = {'users (с индексом login)': [], 'users_no_login_idx (без индекса)': []}
        results_select_like_start = {'users (с индексом login)': [], 'users_no_login_idx (без индекса)': []}
        results_select_like_contains = {'users (с индексом login)': [], 'users_no_login_idx (без индекса)': []}
        results_insert = {'users (с индексом login)': [], 'users_no_login_idx (без индекса)': []}
        
        for size in table_sizes:
            print(f"Тестирование строковых индексов для {size} записей...")
            
            # Генерируем и заполняем данные
            users_data = self.data_gen.generate_users(size, save=False)
            
            self.data_gen.delete_data("users")
            self.data_gen.insert_data(users_data, "users")
            
            self.data_gen.delete_data("users_no_login_idx")
            self.data_gen.insert_data(users_data, "users_no_login_idx")
            
            # Тест SELECT на равенство
            test_login = f"user_{size//2}_{min(size, 100)}"
            
            time_t3_eq = self.timer.measure_query(
                "SELECT * FROM users WHERE login = %s",
                (test_login,),
                iterations=100
            )
            time_t4_eq = self.timer.measure_query(
                "SELECT * FROM users_no_login_idx WHERE login = %s",
                (test_login,),
                iterations=100
            )
            
            results_select_eq['users (с индексом login)'].append(time_t3_eq)
            results_select_eq['users_no_login_idx (без индекса)'].append(time_t4_eq)
            
            # Тест SELECT LIKE с начала строки
            time_t3_like_start = self.timer.measure_query(
                "SELECT * FROM users WHERE login LIKE %s",
                ('user_%',),
                iterations=10
            )
            time_t4_like_start = self.timer.measure_query(
                "SELECT * FROM users_no_login_idx WHERE login LIKE %s",
                ('user_%',),
                iterations=10
            )
            
            results_select_like_start['users (с индексом login)'].append(time_t3_like_start)
            results_select_like_start['users_no_login_idx (без индекса)'].append(time_t4_like_start)
            
            # Тест SELECT LIKE с содержанием
            time_t3_like_contains = self.timer.measure_query(
                "SELECT * FROM users WHERE login LIKE %s",
                ('%50%',),
                iterations=10
            )
            time_t4_like_contains = self.timer.measure_query(
                "SELECT * FROM users_no_login_idx WHERE login LIKE %s",
                ('%50%',),
                iterations=10
            )
            
            results_select_like_contains['users (с индексом login)'].append(time_t3_like_contains)
            results_select_like_contains['users_no_login_idx (без индекса)'].append(time_t4_like_contains)
            
            # Тест INSERT - ручное измерение времени
            insert_iterations = 30
            total_t3_time = 0
            total_t4_time = 0
            
            for i in range(insert_iterations):
                # Генерируем уникального пользователя для T3
                test_user_t3 = self.data_gen.generate_users(1, save=False)[0]
                
                # Измеряем INSERT в T3
                start_time = time.time()
                self.data_gen.insert_data([test_user_t3], "users")
                total_t3_time += time.time() - start_time
                
                # Генерируем уникального пользователя для T4
                test_user_t4 = self.data_gen.generate_users(1, save=False)[0]
                
                # Измеряем INSERT в T4
                start_time = time.time()
                self.data_gen.insert_data([test_user_t4], "users_no_login_idx")
                total_t4_time += time.time() - start_time
            
            results_insert['users (с индексом login)'].append(total_t3_time / insert_iterations)
            results_insert['users_no_login_idx (без индекса)'].append(total_t4_time / insert_iterations)
        
        # Построение графиков
        self.plotter.create_line_plot(
            table_sizes,
            results_select_eq,
            "SELECT по равенству строк (WHERE login = 'value')",
            "Количество записей в таблице",
            "Время выполнения (сек)",
            "string_select_equality"
        )

        self.plotter.create_line_plot(
            table_sizes,
            results_select_like_start,
            "SELECT LIKE с начала строки (WHERE login LIKE 'prefix%')",
            "Количество записей в таблице",
            "Время выполнения (сек)",
            "string_select_like_start"
        )

        self.plotter.create_line_plot(
            table_sizes,
            results_select_like_contains,
            "SELECT LIKE с содержанием (WHERE login LIKE '%text%')",
            "Количество записей в таблице",
            "Время выполнения (сек)",
            "string_select_like_contains"
        )

        self.plotter.create_line_plot(
            table_sizes,
            results_insert,
            "INSERT в таблицы со строковыми полями",
            "Количество записей в таблице",
            "Время выполнения (сек)",
            "string_insert"
        )

    def research_fulltext_indexes(self):
        """Исследование полнотекстовых индексов (таблицы comments - поле text)"""
        print("=== Исследование полнотекстовых индексов ===")
        
        # Создаем полнотекстовый индекс для comments
        self.db.create_fulltext_indexes()
        
        with self.db.get_database_connection() as conn:
            with conn.cursor() as cur:
                # Создаем T6 (comments без полнотекстового индекса)
                cur.execute("""
                    CREATE TABLE IF NOT EXISTS comments_no_ft (
                        lesson_id INTEGER,
                        date DATE,
                        text TEXT
                    )
                """)
        
        # Генерируем тестовые данные
        print("Генерация тестовых данных для полнотекстового поиска...")
        
        # Сначала создаем расписания и уроки
        self.data_gen.generate_schedules(5)
        self.data_gen.generate_lessons(100)
        
        table_sizes = [250, 500, 750, 1000, 1250, 1500, 1750, 2000]
        results_select_single = {'comments (с FT индексом)': [], 'comments_no_ft (без FT)': []}
        results_select_multi = {'comments (с FT индексом)': [], 'comments_no_ft (без FT)': []}
        results_insert = {'comments (с FT индексом)': [], 'comments_no_ft (без FT)': []}
        
        for size in table_sizes:
            print(f"Тестирование полнотекстовых индексов для {size} записей...")
            
            # Генерируем комментарии с разнообразным текстом
            self.data_gen.delete_data("comments")
            comments_data = []
            
            lesson_ids = self.data_gen.get_lesson_ids()
            if not lesson_ids:
                print("Нет уроков для генерации комментариев")
                continue
                
            for i in range(size):
                # Уникальная комбинация lesson_id и date для избежания конфликта PK
                lesson_id = lesson_ids[i % len(lesson_ids)]
                date = f'2024-01-{(i % 28) + 1:02d}'  # Разные даты для разных записей
                
                comments_data.append({
                    'lesson_id': lesson_id,
                    'date': date,
                    'text': f"Важное занятие по математике с преподавателем Ивановым. Тема: алгебра и геометрия. Студенты должны подготовить домашнее задание к следующему уроку. Номер занятия: {i}"
                })
            
            self.data_gen.insert_data(comments_data, "comments")
            self.data_gen.delete_data("comments_no_ft")
            self.data_gen.insert_data(comments_data, "comments_no_ft")
            
            # Обновляем полнотекстовый индекс
            with self.db.get_database_connection() as conn:
                with conn.cursor() as cur:
                    cur.execute("""
                        UPDATE comments 
                        SET text_tsvector = to_tsvector('russian', text)
                        WHERE text_tsvector IS NULL
                    """)
            
            # Тест SELECT поиск одного слова
            time_t5_single = self.timer.measure_query(
                "SELECT * FROM comments WHERE text_tsvector @@ to_tsquery('russian', 'математика')",
                iterations=50
            )
            time_t6_single = self.timer.measure_query(
                "SELECT * FROM comments_no_ft WHERE text LIKE '%математика%'",
                iterations=50
            )
            
            results_select_single['comments (с FT индексом)'].append(time_t5_single)
            results_select_single['comments_no_ft (без FT)'].append(time_t6_single)
            
            # Тест SELECT поиск нескольких слов
            time_t5_multi = self.timer.measure_query(
                "SELECT * FROM comments WHERE text_tsvector @@ to_tsquery('russian', 'математика & геометрия')",
                iterations=50
            )
            time_t6_multi = self.timer.measure_query(
                "SELECT * FROM comments_no_ft WHERE text LIKE '%математика%' AND text LIKE '%геометрия%'",
                iterations=50
            )
            
            results_select_multi['comments (с FT индексом)'].append(time_t5_multi)
            results_select_multi['comments_no_ft (без FT)'].append(time_t6_multi)
            
            # Тест INSERT - ручное измерение времени
            insert_iterations = 20
            total_t5_time = 0
            total_t6_time = 0
            
            for i in range(insert_iterations):
                # Генерируем уникальный комментарий для T5 с уникальной парой (lesson_id, date)
                test_comment_t5 = {
                    'lesson_id': lesson_ids[(i + size) % len(lesson_ids)],  # Используем разные lesson_id
                    'date': f'2024-02-{(i % 28) + 1:02d}',  # Используем февраль для тестовых вставок
                    'text': f'Новый комментарий о важности изучения программирования и алгоритмов {datetime.datetime.now().strftime("%H%M%S%f")}_{i}'
                }
                
                # Измеряем INSERT в T5
                start_time = time.time()
                self.data_gen.insert_data([test_comment_t5], "comments")
                total_t5_time += time.time() - start_time
                
                # Генерируем уникальный комментарий для T6 с уникальной парой (lesson_id, date)
                test_comment_t6 = {
                    'lesson_id': lesson_ids[(i + size + 1) % len(lesson_ids)],
                    'date': f'2024-02-{((i + 15) % 28) + 1:02d}',  # Сдвигаем дату
                    'text': f'Другой комментарий о важности изучения математики {datetime.datetime.now().strftime("%H%M%S%f")}_{i}'
                }
                
                # Измеряем INSERT в T6
                start_time = time.time()
                self.data_gen.insert_data([test_comment_t6], "comments_no_ft")
                total_t6_time += time.time() - start_time
            
            results_insert['comments (с FT индексом)'].append(total_t5_time / insert_iterations)
            results_insert['comments_no_ft (без FT)'].append(total_t6_time / insert_iterations)
        
        # Построение графиков
        self.plotter.create_line_plot(
            table_sizes,
            results_select_single,
            "Полнотекстовый поиск одного слова",
            "Количество записей в таблице",
            "Время выполнения (сек)",
            "fulltext_select_single"
        )
        
        self.plotter.create_line_plot(
            table_sizes,
            results_select_multi,
            "Полнотекстовый поиск нескольких слов",
            "Количество записей в таблице",
            "Время выполнения (сек)",
            "fulltext_select_multi"
        )
        
        self.plotter.create_line_plot(
            table_sizes,
            results_insert,
            "INSERT в таблицы с текстовыми полями",
            "Количество записей в таблице",
            "Время выполнения (сек)",
            "fulltext_insert"
        )
    
    def run_all_research(self):
        """Запуск всех исследований"""
        print("Запуск исследований эффективности индексов...")
        
        self.research_numeric_indexes()
        self.research_string_indexes() 
        self.research_fulltext_indexes()
        
        print("Все исследования завершены!")

# Основной скрипт для запуска
if __name__ == "__main__":
    # Инициализация базы данных
    db = DataBaseMain(settings.DATABASE_URL, settings.TEST_DATABASE_URL)
    
    # Создаем таблицы если их нет
    db.create_tables()
    
    # Запуск исследований
    research = IndexResearch(db)
    research.run_all_research()
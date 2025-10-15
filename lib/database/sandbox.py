# sandbox.py
import psycopg
from typing import Optional, Callable

class SandboxManager:
    def __init__(self, main_db_connection: Callable, test_db_connection: Callable, test_db_url: Optional[str] = None):
        self.main_db_connection = main_db_connection
        self.test_db_connection = test_db_connection
        self.test_db_url = test_db_url
    
    def create_sandbox(self) -> bool:
        """Создание песочницы (тестовой БД) - требует URL"""
        if not self.test_db_url:
            print("Для создания БД требуется test_db_url")
            return False
            
        try:
            # Создаем подключение к postgres БД для создания тестовой БД
            main_conn = psycopg.connect(self.test_db_url.replace('/test_db', '/postgres'))
            main_conn.autocommit = True
            
            with main_conn.cursor() as cur:
                # Пытаемся создать БД, игнорируем ошибку если уже существует
                try:
                    cur.execute("CREATE DATABASE test_db")
                except psycopg.Error:
                    # База данных уже существует
                    pass
            
            main_conn.close()
            
            # Создаем таблицы в тестовой БД
            self._create_tables_in_test_db()
            return True
            
        except Exception as e:
            print(f"Ошибка при создании песочницы: {e}")
            return False
    
    def copy_schema_to_sandbox(self) -> None:
        """Копирование схемы из основной БД в песочницу"""
        # Удаляем существующие таблицы в песочнице
        tables = ['attendance', 'comments', 'schedule_user', 'lessons', 'schedules', 'users']
        
        with self.test_db_connection() as conn:
            with conn.cursor() as cur:
                for table in tables:
                    try:
                        cur.execute(f"DROP TABLE IF EXISTS {table} CASCADE")
                    except Exception:
                        pass
        
        # Создаем таблицы заново
        self._create_tables_in_test_db()
    
    def _create_tables_in_test_db(self) -> None:
        """Создание таблиц в тестовой БД"""
        with self.test_db_connection() as conn:
            with conn.cursor() as cur:
                # Создание таблицы users
                cur.execute(
                    """
                        CREATE TABLE IF NOT EXISTS users(
                            id SERIAL PRIMARY KEY,
                            login VARCHAR(255) NOT NULL UNIQUE,
                            email VARCHAR(255) NOT NULL UNIQUE,
                            password VARCHAR(255) NOT NULL
                        )
                    """
                )

                # Создание таблицы schedules
                cur.execute(
                    """
                        CREATE TABLE IF NOT EXISTS schedules(
                            id SERIAL PRIMARY KEY,
                            name VARCHAR(255) NOT NULL
                        )
                    """
                )

                # Создание таблицы schedule_user
                cur.execute(
                    """
                        CREATE TABLE IF NOT EXISTS schedule_user(
                            schedule_id INTEGER REFERENCES schedules(id) ON DELETE CASCADE,
                            user_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                            PRIMARY KEY (schedule_id, user_id)
                        )
                    """
                )

                # Создание таблицы lessons
                cur.execute(
                    """
                        CREATE TABLE IF NOT EXISTS lessons(
                            id SERIAL PRIMARY KEY,
                            schedule_id INTEGER NOT NULL REFERENCES schedules(id) ON DELETE CASCADE,
                            teacher_name VARCHAR(255) NOT NULL,
                            subject_name VARCHAR(255) NOT NULL,
                            place VARCHAR(255) NOT NULL,
                            startDate DATE NOT NULL,
                            endDate DATE NOT NULL,
                            startTime TIME WITHOUT TIME ZONE NOT NULL,
                            endTime TIME WITHOUT TIME ZONE NOT NULL,
                            repeat VARCHAR(255) NOT NULL
                        )
                    """
                )

                # Создание таблицы comments
                cur.execute(
                    """
                        CREATE TABLE IF NOT EXISTS comments(
                            lesson_id INTEGER NOT NULL REFERENCES lessons(id) ON DELETE CASCADE,
                            date DATE NOT NULL,
                            text TEXT NOT NULL,
                            PRIMARY KEY (lesson_id, date)
                        )
                    """
                )

                # Создание таблицы attendance
                cur.execute(
                    """
                        CREATE TABLE IF NOT EXISTS attendance(
                            lesson_id INTEGER NOT NULL REFERENCES lessons(id) ON DELETE CASCADE,
                            date DATE NOT NULL,
                            PRIMARY KEY (lesson_id, date)
                        )
                    """
                )
    
    def copy_data_to_sandbox(self, table_name: str) -> None:
        """Копирование данных из основной БД в песочницу"""
        # Получаем данные из основной БД
        with self.main_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"SELECT * FROM {table_name}")
                data = cur.fetchall()
        
        if not data:
            return
        
        # Получаем информацию о столбцах
        with self.main_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(f"""
                    SELECT column_name 
                    FROM information_schema.columns 
                    WHERE table_name = %s 
                    ORDER BY ordinal_position
                """, (table_name,))
                columns = [row[0] for row in cur.fetchall()]
        
        # Вставляем данные в песочницу
        columns_str = ', '.join(columns)
        placeholders = ', '.join(['%s'] * len(columns))
        query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
        
        with self.test_db_connection() as conn:
            with conn.cursor() as cur:
                for row in data:
                    cur.execute(query, row)
    
    def copy_all_data_to_sandbox(self) -> None:
        """Копирование всех данных из основной БД в песочницу"""
        tables = ['users', 'schedules', 'lessons', 'schedule_user', 'comments', 'attendance']
        for table in tables:
            self.copy_data_to_sandbox(table)
    
    def cleanup_sandbox(self) -> None:
        """Очистка песочницы"""
        tables = ['attendance', 'comments', 'schedule_user', 'lessons', 'schedules', 'users']
        
        with self.test_db_connection() as conn:
            with conn.cursor() as cur:
                for table in tables:
                    try:
                        cur.execute(f"TRUNCATE TABLE {table} CASCADE")
                    except Exception:
                        pass
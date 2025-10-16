import psycopg
from contextlib import contextmanager

class DataBaseMain:
    def __init__(self, DATABASE_URL: str, TEST_DATABASE_URL: str = None):
        self.DATABASE_URL = DATABASE_URL
        self.TEST_DATABASE_URL = TEST_DATABASE_URL
        
    @contextmanager
    def get_database_connection(self, test_db: bool = False):
        """Контекстный менеджер для подключения к БД"""
        db_url = self.TEST_DATABASE_URL if test_db else self.DATABASE_URL
        conn = psycopg.connect(db_url)
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            raise e
        finally:
            conn.close()

    def get_connection(self, test_db: bool = False):
        """Простая функция для получения соединения (без контекстного менеджера)"""
        db_url = self.TEST_DATABASE_URL if test_db else self.DATABASE_URL
        return psycopg.connect(db_url)

    def create_tables(self, test_db: bool = False):
        """Создание всех таблиц"""
        with self.get_database_connection(test_db) as conn:
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
                
    def create_fulltext_indexes(self, test_db: bool = False):
        with self.get_database_connection(test_db) as conn:
            with conn.cursor() as cur:
                # Добавляем колонку для tsvector если её нет
                try:
                    cur.execute("""
                        ALTER TABLE comments ADD COLUMN IF NOT EXISTS text_tsvector tsvector
                    """)
                    
                    # Обновляем tsvector для существующих данных
                    cur.execute("""
                        UPDATE comments SET text_tsvector = to_tsvector('russian', text)
                        WHERE text_tsvector IS NULL
                    """)
                    
                    # Создаем GIN индекс для полнотекстового поиска
                    cur.execute("""
                        CREATE INDEX IF NOT EXISTS idx_comments_text_ft 
                        ON comments USING GIN(text_tsvector)
                    """)
                    
                    print("Полнотекстовые индексы созданы успешно")
                    
                except Exception as e:
                    print(f"Ошибка при создании полнотекстовых индексов: {e}")
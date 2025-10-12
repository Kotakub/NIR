from .connection import connection


def create_tables(connection):
    """Создание всех таблиц"""
    with connection as conn:
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

def create_tables(self) -> None:
    """Создание всех таблиц"""
    with self.get_connection() as conn:
        with conn.cursor() as cur:
            # Создание таблицы users
            cur.execute(
                """
                    CREATE TABLE IF NOT EXISTS users(
                        id SERIAL PRIMARY KEY,
                        login VARCHAR(255) NOT NULL UNIQUE,
                        email VARCHAR(255) NOT NULL UNIQUE,
                        password VARCHAR(255) NOT NULL,
                        created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """
            )

            # Создание таблицы schedules
            cur.execute(
                """
                    CREATE TABLE IF NOT EXISTS schedules(
                        id SERIAL PRIMARY KEY,
                        user_id INTEGER NOT NULL REFERENCES users(id) ON DELETE CASCADE,
                        created_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
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
                        repeat_pattern VARCHAR(255) NOT NULL
                    )
                """
            )

            # Создание таблицы comments
            cur.execute(
                """
                    CREATE TABLE IF NOT EXISTS comments(
                        id SERIAL PRIMARY KEY,
                        lesson_id INTEGER NOT NULL REFERENCES lessons(id) ON DELETE CASCADE,
                        date DATE NOT NULL,
                        text TEXT NOT NULL
                    )
                """
            )

            # Создание таблицы attendance
            cur.execute(
                """
                    CREATE TABLE IF NOT EXISTS attendance(
                        id SERIAL PRIMARY KEY,
                        lesson_id INTEGER NOT NULL REFERENCES lessons(id) ON DELETE CASCADE,
                        date DATE NOT NULL,
                        status VARCHAR(50) DEFAULT 'present'
                    )
                """
            )

            # Таблица для связи многие-ко-многим (если нужно)
            cur.execute(
                """
                    CREATE TABLE IF NOT EXISTS lesson_students(
                        lesson_id INTEGER REFERENCES lessons(id) ON DELETE CASCADE,
                        student_id INTEGER REFERENCES users(id) ON DELETE CASCADE,
                        PRIMARY KEY (lesson_id, student_id)
                    )
                """
            )

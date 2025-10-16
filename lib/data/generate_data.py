import random
import datetime
import psycopg

class DataGenerator:
    def __init__(self, database_connection_func):
        self.database_connection_func = database_connection_func

    def insert_data(self, table_data, table_name: str):
        if not table_data:
            return
            
        columns = ', '.join(table_data[0].keys())
        placeholders = ', '.join(['%s'] * len(table_data[0]))
        query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"
        
    
        with self.database_connection_func() as conn:
            with conn.cursor() as cur:
                for data in table_data:
                    cur.execute(query, list(data.values()))
                    
    
    def delete_data(self, table_name: str):
        if table_name == "":
            return
        
        
        with self.database_connection_func() as conn:
            try:
                with conn.cursor() as cur:
                    cur.execute(f"DELETE FROM {table_name}")
                conn.commit()
            except Exception as e:
                conn.rollback()
                raise e
        
    
    def generate_users(self, n: int = 1, save: bool = True):
        users = []
        for i in range(n):
            login = f"user_{i}_{datetime.datetime.now().isoformat()}"
            email = f"{login}@example.com"
            password = f"password_{login}"  
            users.append({
                'login': login,
                'email': email,
                'password': password
            })
        if save:
            self.insert_data(users, "users")
        else:
            return users
        
    
    def generate_schedules(self, n: int = 1, save: bool = True):
        schedules = []
        for i in range(n):
            name = f"schedule_{i}_{datetime.datetime.now().isoformat()}"
            schedules.append({
                'name': name
            })
        self.insert_data(schedules, "schedules")
        
    
    def generate_lessons(self, n: int = 1, save: bool = True):
        lessons = []
        schedule_ids = self.get_schedule_ids()
        
        if not schedule_ids:
            print("No schedules available. Please generate schedules first.")
            return
        
        lesson_times = [
            ("08:30", "10:00"),
            ("10:10", "11:40"),
            ("12:10", "13:40"),
            ("13:50", "15:20"),
            ("15:50", "17:20"),
            ("17:30", "19:00"),
            ("19:10", "20:40")
        ]
        
        teacher_names = ["Иванов И.И.", "Петров П.П.", "Сидорова С.С.", "Кузнецов К.К.", "Васильев В.В."]
        subjects = ["Математика", "Физика", "Химия", "Информатика", "Английский язык"]
        places = ["Аудитория 101", "Аудитория 202", "Лаборатория 301", "Актовый зал", "Спортзал"]
        repeat_options = ["none", "weekly", "beweekly"]
        
        current_date = datetime.datetime.now().date()
        
        for i in range(n):
            schedule_id = random.choice(schedule_ids)
            
            start_date = current_date
            end_date = start_date + datetime.timedelta(days=random.randint(7, 60))
            start_time, end_time = random.choice(lesson_times)
            
            lessons.append({
                'schedule_id': schedule_id,
                'teacher_name': random.choice(teacher_names),
                'subject_name': random.choice(subjects),
                'place': random.choice(places),
                'startDate': start_date,
                'endDate': end_date,
                'startTime': start_time,
                'endTime': end_time,
                'repeat': random.choice(repeat_options)
            })
        if save:
            self.insert_data(lessons, "lessons")
        else: 
            return lessons
    
    
    def generate_attendance(self, n: int = 1, save: bool = True):
        attendance = []
        lesson_ids = self.get_lesson_ids()
        
        if not lesson_ids:
            print("Нет занятий.")
            return
            
        for _ in range(n):
            lesson_id = random.choice(lesson_ids)
            date = datetime.datetime.now().date() + datetime.timedelta(days=random.randint(7, 60))
            attendance.append({
                'lesson_id': lesson_id,
                'date': date
            })
        if save:
            self.insert_data(attendance, "attendance")
        else: 
            return attendance
    
    
    def generate_comments(self, n: int = 1, save: bool = True):
        comments = []
        lesson_ids = self.get_lesson_ids()
        
        if not lesson_ids:
            print("Нет занятий.")
            return
            
        for i in range(n):
            lesson_id = random.choice(lesson_ids)
            date = datetime.datetime.now().date() + datetime.timedelta(days=random.randint(7, 60))
            text = f"Заметка {i} для занятия {lesson_id} на {date}"
            comments.append({
                'lesson_id': lesson_id,
                'date': date,
                'text': text
            })
        if save:
            self.insert_data(comments, "comments")
        else:
            return comments
    
    
    def generate_schedule_user(self, n: int = 1, save: bool = True):
        user_ids = self.get_user_ids()
        schedule_ids = self.get_schedule_ids()
        schedule_user = []
        for i in range(n):
            user_id = random.choice(user_ids)
            schedule_id = random.choice(schedule_ids)
            schedule_user.append({
                "user_id": user_id,
                "schedule_id": schedule_id
            })
        if save:
            self.insert_data(schedule_user, "schedule_user")
        else:
            return schedule_user
        
    
    
    def get_schedule_ids(self):
        with self.database_connection_func() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM schedules")
                return [row[0] for row in cur.fetchall()]
                
    def get_user_ids(self):
        with self.database_connection_func() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM users")
                return [row[0] for row in cur.fetchall()]
                
    def get_lesson_ids(self):
        with self.database_connection_func() as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT id FROM lessons")
                return [row[0] for row in cur.fetchall()]
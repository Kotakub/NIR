# backup.py
import datetime
import os
import json
from typing import List, Dict, Any

class BackupManager:
    def __init__(self, database_connection, backup_dir: str = "lib/backup"):
        self.database_connection = database_connection
        self.backup_dir = backup_dir
        os.makedirs(backup_dir, exist_ok=True)
    
    def create_backup(self, tables: List[str] = None) -> str:
        """Создание бэкапа указанных таблиц"""
        if tables is None:
            tables = ['users', 'schedules', 'lessons', 'schedule_user', 'comments', 'attendance']
        
        backup_data = {}
        backup_timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        backup_filename = f"backup_{backup_timestamp}.json"
        backup_path = os.path.join(self.backup_dir, backup_filename)
        
        with self.database_connection() as conn:
            with conn.cursor() as cur:
                for table in tables:
                    cur.execute(f"SELECT * FROM {table}")
                    data = cur.fetchall()
                    backup_data[table] = {
                        'data': data,
                        'rowcount': len(data),
                        'timestamp': backup_timestamp
                    }
        
        # Сохраняем бэкап в файл
        with open(backup_path, 'w', encoding='utf-8') as f:
            json.dump(backup_data, f, indent=2, default=str)
        
        return backup_path
    
    def restore_backup(self, backup_path: str) -> None:
        """Восстановление данных из бэкапа"""
        if not os.path.exists(backup_path):
            raise FileNotFoundError(f"Файл бэкапа не найден: {backup_path}")
        
        with open(backup_path, 'r', encoding='utf-8') as f:
            backup_data = json.load(f)
        
        # Очищаем таблицы перед восстановлением
        with self.database_connection() as conn:
            with conn.cursor() as cur:
                for table in backup_data.keys():
                    cur.execute(f"TRUNCATE TABLE {table}")
        
        # Восстанавливаем данные
        with self.database_connection() as conn:
            with conn.cursor() as cur:
                for table, table_data in backup_data.items():
                    data = table_data['data']
                    if not data:
                        continue
                    
                    # Получаем информацию о столбцах
                    cur.execute(f"""
                        SELECT column_name 
                        FROM information_schema.columns 
                        WHERE table_name = %s 
                        ORDER BY ordinal_position
                    """, (table,))
                    columns = [row[0] for row in cur.fetchall()]
                    
                    # Вставляем данные
                    columns_str = ', '.join(columns)
                    placeholders = ', '.join(['%s'] * len(columns))
                    query = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"
                    
                    for row in data:
                        cur.execute(query, row)
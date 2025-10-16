# demo_custom_db.py
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), 'bad_subd'))

from bad_subd import custom_db

def demo_workflow():
    """Демонстрация полного СУБД"""
    print("🚀 ДЕМОНСТРАЦИЯ РАБОТЫ CUSTOM СУБД")
    print("=" * 50)
    
    # 1. Создаем таблицы
    print("1. СОЗДАНИЕ ТАБЛИЦ")
    custom_db.create_all_tables()
    
    # 2. Показываем информацию о таблицах
    print("\n2. ИНФОРМАЦИЯ О ТАБЛИЦАХ")
    custom_db.demo_storage()
    
    # 3. Вставляем тестовые данные
    print("\n3. ВСТАВКА ДАННЫХ")
    
    # Users
    print("→ Вставляем users...")
    custom_db.engine.insert("users", {
        "id": 1,
        "login": "alice_smith",
        "email": "alice@university.ru", 
        "password": "secure_pass_123"
    })
    
    custom_db.engine.insert("users", {
        "id": 2, 
        "login": "bob_johnson",
        "email": "bob@university.ru",
        "password": "my_password_456"
    })
    
    # Lessons
    print("→ Вставляем lessons...")
    custom_db.engine.insert("lessons", {
        "id": 1,
        "schedule_id": 1,
        "teacher_name": "Иванов Иван Иванович",
        "subject_name": "Математический анализ", 
        "place": "Аудитория 301",
        "startDate": "2024-01-15",
        "endDate": "2024-05-20",
        "startTime": "09:00",
        "endTime": "10:30", 
        "repeat": "weekly"
    })
    
    custom_db.engine.insert("lessons", {
        "id": 2,
        "schedule_id": 1, 
        "teacher_name": "Петрова Светлана Владимировна",
        "subject_name": "Физика",
        "place": "Лаборатория 205",
        "startDate": "2024-01-16", 
        "endDate": "2024-05-21",
        "startTime": "11:00",
        "endTime": "12:30",
        "repeat": "weekly"
    })
    
    # 4. Выполняем SELECT запросы
    print("\n4. SELECT ЗАПРОСЫ")
    
    # SELECT * FROM users
    print("→ SELECT * FROM users:")
    users = custom_db.engine.select("users")
    for user in users:
        print(f"   {user}")
    
    # SELECT с WHERE для числового поля
    print("\n→ SELECT * FROM users WHERE id = 1:")
    user_1 = custom_db.engine.select("users", where={"id": 1})
    print(f"   {user_1}")
    
    # SELECT с WHERE для строкового поля  
    print("\n→ SELECT * FROM users WHERE login = 'bob_johnson':")
    bob = custom_db.engine.select("users", where={"login": "bob_johnson"})
    print(f"   {bob}")
    
    # SELECT конкретных колонок
    print("\n→ SELECT login, email FROM users:")
    users_short = custom_db.engine.select("users", columns=["login", "email"])
    for user in users_short:
        print(f"   {user}")
    
    # 5. Показываем работу индексов
    print("\n5. РАБОТА ИНДЕКСОВ")
    print("→ Поиск по индексированному полю (id):")
    # Использует индекс B+Tree
    user_by_index = custom_db.engine.select("users", where={"id": 2})
    print(f"   {user_by_index}")
    
    # 6. DELETE операции
    print("\n6. DELETE ОПЕРАЦИИ")
    
    # DELETE с WHERE
    print("→ DELETE FROM users WHERE login = 'bob_johnson':")
    deleted_count = custom_db.engine.delete("users", where={"login": "bob_johnson"})
    print(f"   Удалено строк: {deleted_count}")
    
    # Проверяем, что удалилось
    remaining_users = custom_db.engine.select("users")
    print(f"   Осталось пользователей: {len(remaining_users)}")
    
    # 7. Показываем итоговую информацию
    print("\n7. ФИНАЛЬНАЯ ИНФОРМАЦИЯ")
    custom_db.demo_storage()

def demo_binary_storage():
    """Демонстрация бинарного хранения"""
    print("\n" + "="*50)
    print("🔍 ДЕМОНСТРАЦИЯ БИНАРНОГО ХРАНЕНИЯ")
    print("=" * 50)
    
    # Создаем простую таблицу для демонстрации
    from bad_subd.engine import CustomDBEngine
    engine = CustomDBEngine()
    
    # Создаем тестовую таблицу
    engine.create_table("demo_table", [
        {"name": "id", "type": "INT"},
        {"name": "name", "type": "VARCHAR", "size": 20},
        {"name": "description", "type": "VARCHAR", "size": 50}
    ])
    
    # Получаем информацию о хранилище
    storage = engine._get_storage("demo_table")
    print(f"Размер строки: {storage.row_size} байт")
    
    # Показываем структуру строки
    test_row = {
        "id": 123,
        "name": "Тестовая запись",
        "description": "Это демонстрация бинарного хранения"
    }
    
    print(f"\nИсходная строка: {test_row}")
    
    # Сериализуем в бинарный формат
    binary_data = storage.serialize_row(test_row)
    print(f"Бинарные данные ({len(binary_data)} байт):")
    print(f"  HEX: {binary_data.hex()}")
    
    # Десериализуем обратно
    restored_row = storage.deserialize_row(binary_data)
    print(f"Восстановленная строка: {restored_row}")
    
    # Показываем смещения столбцов
    print(f"\nСмещения столбцов в строке:")
    for col in ["id", "name", "description"]:
        offset = storage.get_column_offset(col)
        print(f"  {col}: {offset} байт")

def demo_file_structure():
    """Демонстрация структуры файлов"""
    print("\n" + "="*50)
    print("📁 СТРУКТУРА ФАЙЛОВ СУБД")
    print("=" * 50)
    
    import os
    from bad_subd.config import custom_db_config
    
    base_dir = custom_db_config.BASE_DATA_DIR
    print(f"Базовая директория: {base_dir}")
    
    for root, dirs, files in os.walk(base_dir):
        level = root.replace(base_dir, '').count(os.sep)
        indent = ' ' * 2 * level
        print(f"{indent}📁 {os.path.basename(root)}/")
        
        sub_indent = ' ' * 2 * (level + 1)
        for file in files:
            file_path = os.path.join(root, file)
            file_size = os.path.getsize(file_path)
            print(f"{sub_indent}📄 {file} ({file_size} байт)")

if __name__ == "__main__":
    demo_workflow()
    demo_binary_storage() 
    demo_file_structure()
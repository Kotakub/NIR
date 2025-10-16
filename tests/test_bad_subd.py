import os
import sys
import time
import shutil
sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from lib.bad_subd import BadSUBD

class SQLBadSUBDTester:
    """Тестер для SQLBadSUBD"""
    
    def __init__(self):
        # Используем SQL версию
        self.db = BadSUBD(use_sql=True)
        self.test_results = []
    
    def log_test(self, test_name, success, message=""):
        """Логирование результатов теста"""
        status = "✓ PASS" if success else "✗ FAIL"
        result = f"{status}: {test_name}"
        if message:
            result += f" - {message}"
        print(result)
        self.test_results.append((test_name, success, message))
    
    def cleanup_test_data(self):
        """Полная очистка тестовых данных"""
        try:
            # Полный путь к данным
            data_dirs = [
                "lib/bad_subd/data/schemas",
                "lib/bad_subd/data/tables", 
                "lib/bad_subd/data/indexes"
            ]
            
            # Удаляем все файлы в директориях данных
            for data_dir in data_dirs:
                if os.path.exists(data_dir):
                    for file in os.listdir(data_dir):
                        if file.endswith(('.json', '.dat', '.idx')):
                            file_path = os.path.join(data_dir, file)
                            try:
                                os.remove(file_path)
                            except:
                                pass
            
            # Также очищаем кэш хранилищ в движке
            self.db.engine.storages.clear()
            self.db.engine.indexes.clear()
            
            print("✓ Тестовые данные полностью очищены")
            
        except Exception as e:
            print(f"⚠ Ошибка при очистке: {e}")
    
    def table_exists(self, table_name):
        """Проверяет существует ли таблица"""
        try:
            self.db.engine.get_table_info(table_name)
            return True
        except:
            return False
    
    def delete_table_if_exists(self, table_name):
        """Удаляет таблицу если она существует"""
        try:
            if self.table_exists(table_name):
                # Удаляем через DELETE *
                self.db.execute(f"DELETE * FROM {table_name}")
                # Удаляем схему и файлы
                self.db.engine.schema_manager.delete_schema(table_name)
                self.db.engine.table_manager.delete_table_file(table_name)
                # Удаляем из кэша
                if table_name in self.db.engine.storages:
                    del self.db.engine.storages[table_name]
                if table_name in self.db.engine.indexes:
                    del self.db.engine.indexes[table_name]
                return True
        except:
            pass
        return False
    
    def test_simple_table_creation(self):
        """Тестирование простого создания таблицы"""
        print("\n=== Тестирование простого создания таблицы ===")
        
        try:
            # Удаляем таблицу если существует
            self.delete_table_if_exists("test_simple")
            
            # Создаем простую тестовую таблицу
            create_sql = """
            CREATE TABLE test_simple (
                id INT,
                name VARCHAR,
                age INT
            )
            """
            self.db.execute(create_sql)
            self.log_test("CREATE TABLE test_simple", True)
            
            # Проверяем создание таблицы
            if self.table_exists("test_simple"):
                info = self.db.engine.get_table_info("test_simple")
                if info['table_name'] == 'test_simple':
                    self.log_test("Проверка создания таблицы", True, f"колонок: {len(info['columns'])}")
                else:
                    self.log_test("Проверка создания таблицы", False, "Таблица не создана")
            else:
                self.log_test("Проверка создания таблицы", False, "Таблица не найдена")
                
            return True
            
        except Exception as e:
            self.log_test("CREATE TABLE test_simple", False, str(e))
            return False
    
    def test_table_with_sizes(self):
        """Тестирование создания таблицы с размерами VARCHAR"""
        print("\n=== Тестирование создания таблицы с размерами ===")
        
        try:
            # Удаляем таблицу если существует
            self.delete_table_if_exists("test_sized")
            
            # Создаем таблицу с явными размерами через движок (не через SQL)
            self.db.engine.create_table("test_sized", [
                {"name": "id", "type": "INT"},
                {"name": "username", "type": "VARCHAR", "size": 50},
                {"name": "email", "type": "VARCHAR", "size": 100},
                {"name": "score", "type": "INT"}
            ])
            self.log_test("CREATE TABLE test_sized через движок", True)
            
            # Проверяем создание
            if self.table_exists("test_sized"):
                info = self.db.engine.get_table_info("test_sized")
                if info['table_name'] == 'test_sized':
                    self.log_test("Проверка создания таблицы с размерами", True)
                    # Проверяем размеры колонок
                    for col in info['columns']:
                        if col['name'] == 'username' and col['size'] == 50:
                            self.log_test("Проверка размера VARCHAR", True, f"размер: {col['size']}")
                            break
                else:
                    self.log_test("Проверка создания таблицы", False, "Таблица не создана")
            else:
                self.log_test("Проверка создания таблицы", False, "Таблица не найдена")
                
            return True
            
        except Exception as e:
            self.log_test("CREATE TABLE test_sized", False, str(e))
            return False
    
    def test_sql_insert_select(self):
        """Тестирование INSERT и SELECT через SQL"""
        print("\n=== Тестирование INSERT и SELECT ===")
        
        try:
            # Убедимся что таблица существует
            if not self.table_exists("test_simple"):
                self.log_test("Подготовка таблицы", False, "Таблица test_simple не существует")
                return False
            
            # Вставка данных
            insert_sql = """
            INSERT INTO test_simple (id, name, age)
            VALUES (1, 'Иван Иванов', 20)
            """
            result = self.db.execute(insert_sql)
            self.log_test("INSERT INTO test_simple", True if result else False)
            
            # SELECT всех данных
            select_all = "SELECT * FROM test_simple"
            result_all = self.db.execute(select_all)
            self.log_test("SELECT *", True, f"Найдено {len(result_all)} записей")
            
            # SELECT с WHERE
            select_where = "SELECT * FROM test_simple WHERE id = 1"
            result_where = self.db.execute(select_where)
            if result_where and result_where[0]['id'] == 1:
                self.log_test("SELECT с WHERE", True)
            else:
                self.log_test("SELECT с WHERE", False, "Не найдена запись с id=1")
            
            # SELECT определенных колонок
            select_cols = "SELECT id, name FROM test_simple"
            result_cols = self.db.execute(select_cols)
            if result_cols and 'id' in result_cols[0] and 'name' in result_cols[0] and 'age' not in result_cols[0]:
                self.log_test("SELECT определенных колонок", True)
            else:
                self.log_test("SELECT определенных колонок", False, "Неверный набор колонок")
                
            return True
            
        except Exception as e:
            self.log_test("INSERT/SELECT операции", False, str(e))
            return False
    
    def test_sql_delete(self):
        """Тестирование DELETE через SQL"""
        print("\n=== Тестирование DELETE ===")
        
        try:
            # Убедимся что таблица существует
            if not self.table_exists("test_simple"):
                self.log_test("Подготовка таблицы", False, "Таблица test_simple не существует")
                return False
            
            # Добавляем еще одну запись для тестирования удаления
            self.db.execute("INSERT INTO test_simple (id, name, age) VALUES (2, 'Петр Петров', 25)")
            
            # DELETE с WHERE
            delete_where = "DELETE FROM test_simple WHERE id = 2"
            result = self.db.execute(delete_where)
            self.log_test("DELETE с WHERE", True, f"Удалено записей: {result}")
            
            # Проверяем удаление
            select_after_delete = "SELECT * FROM test_simple WHERE id = 2"
            result_check = self.db.execute(select_after_delete)
            if len(result_check) == 0:
                self.log_test("Проверка DELETE с WHERE", True)
            else:
                self.log_test("Проверка DELETE с WHERE", False, "Запись не удалена")
            
            # DELETE * (полная очистка)
            delete_all = "DELETE * FROM test_simple"
            result_all = self.db.execute(delete_all)
            self.log_test("DELETE *", True, f"Удалено записей: {result_all}")
            
            # Проверяем полное удаление
            select_after_all = "SELECT * FROM test_simple"
            result_final = self.db.execute(select_after_all)
            if len(result_final) == 0:
                self.log_test("Проверка DELETE *", True)
            else:
                self.log_test("Проверка DELETE *", False, f"Осталось {len(result_final)} записей")
                
            return True
            
        except Exception as e:
            self.log_test("DELETE операции", False, str(e))
            return False
    
    def test_main_tables_via_engine(self):
        """Тестирование создания основных таблиц через движок"""
        print("\n=== Тестирование создания основных таблиц ===")
        
        try:
            # Удаляем основные таблицы если существуют
            main_tables = ["users", "lessons", "comments"]
            for table in main_tables:
                self.delete_table_if_exists(table)
            
            # Создаем таблицы через движок (как в оригинальном коде)
            self.db.create_all_tables()
            self.log_test("Создание всех таблиц через движок", True)
            
            # Проверяем информацию о таблицах
            tables = ["users", "lessons", "comments"]
            success_count = 0
            for table in tables:
                try:
                    if self.table_exists(table):
                        info = self.db.engine.get_table_info(table)
                        self.log_test(f"Проверка таблицы {table}", True, f"размер строки: {info['row_size']} байт")
                        success_count += 1
                    else:
                        self.log_test(f"Проверка таблицы {table}", False, "таблица не найдена")
                except Exception as e:
                    self.log_test(f"Проверка таблицы {table}", False, str(e))
            
            return success_count == len(tables)
            
        except Exception as e:
            self.log_test("Создание основных таблиц", False, str(e))
            return False
    
    def test_data_operations_main_tables(self):
        """Тестирование операций с данными в основных таблицах"""
        print("\n=== Тестирование операций с основными таблицами ===")
        
        try:
            # Проверяем что основные таблицы существуют
            main_tables = ["users", "lessons", "comments"]
            for table in main_tables:
                if not self.table_exists(table):
                    self.log_test(f"Проверка таблицы {table}", False, "таблица не существует")
                    return False
            
            # Очищаем таблицы перед тестом
            for table in main_tables:
                self.db.execute(f"DELETE * FROM {table}")
            
            # Вставляем данные через SQL
            insert_user = """
            INSERT INTO users (id, login, email, password)
            VALUES (1, 'test_user', 'test@example.com', 'password123')
            """
            self.db.execute(insert_user)
            self.log_test("INSERT INTO users", True)
            
            insert_lesson = """
            INSERT INTO lessons (id, schedule_id, teacher_name, subject_name, place, startDate, endDate, startTime, endTime, repeat)
            VALUES (1, 101, 'Профессор Иванов', 'Математика', 'Аудитория 101', '2024-01-15', '2024-05-30', '09:00', '10:30', 'weekly')
            """
            self.db.execute(insert_lesson)
            self.log_test("INSERT INTO lessons", True)
            
            insert_comment = """
            INSERT INTO comments (id, lesson_id, date, text)
            VALUES (1, 1, '2024-01-20', 'Отличный урок!')
            """
            self.db.execute(insert_comment)
            self.log_test("INSERT INTO comments", True)
            
            # Проверяем вставку
            users = self.db.execute("SELECT * FROM users")
            lessons = self.db.execute("SELECT * FROM lessons")
            comments = self.db.execute("SELECT * FROM comments")
            
            if len(users) == 1 and len(lessons) == 1 and len(comments) == 1:
                self.log_test("Проверка вставленных данных", True, 
                             f"Users: {len(users)}, Lessons: {len(lessons)}, Comments: {len(comments)}")
            else:
                self.log_test("Проверка вставленных данных", False,
                             f"Users: {len(users)}, Lessons: {len(lessons)}, Comments: {len(comments)}")
            
            # Тестируем SELECT с WHERE для связанных данных
            user_lessons = self.db.execute("SELECT * FROM lessons WHERE schedule_id = 101")
            lesson_comments = self.db.execute("SELECT * FROM comments WHERE lesson_id = 1")
            
            if len(user_lessons) == 1:
                self.log_test("SELECT связанных lessons", True)
            if len(lesson_comments) == 1:
                self.log_test("SELECT связанных comments", True)
                
            return True
            
        except Exception as e:
            self.log_test("Операции с основными таблицами", False, str(e))
            return False
    
    def test_error_handling(self):
        """Тестирование обработки ошибок"""
        print("\n=== Тестирование обработки ошибок ===")
        
        try:
            # Неправильный SQL
            try:
                self.db.execute("INVALID SQL STATEMENT")
                self.log_test("Неправильный SQL", False, "Должна быть ошибка")
            except:
                self.log_test("Неправильный SQL", True, "Ошибка корректно обработана")
            
            # Несуществующая таблица
            try:
                self.db.execute("SELECT * FROM non_existent_table")
                self.log_test("Несуществующая таблица", False, "Должна быть ошибка")
            except:
                self.log_test("Несуществующая таблица", True, "Ошибка корректно обработана")
            
            # Неправильное количество значений
            try:
                self.db.execute("INSERT INTO users (id, login) VALUES (1)")
                self.log_test("Неправильное количество значений", False, "Должна быть ошибка")
            except:
                self.log_test("Неправильное количество значений", True, "Ошибка корректно обработана")
            
            return True
            
        except Exception as e:
            self.log_test("Обработка ошибок", False, str(e))
            return False
    
    def test_performance_basic(self):
        """Базовое тестирование производительности через SQL"""
        print("\n=== Тестирование производительности ===")
        
        try:
            import timeit
            
            # Создаем тестовую таблицу для производительности
            test_table = "test_perf"
            self.delete_table_if_exists(test_table)
            
            # Создаем таблицу через движок для точного контроля размеров
            self.db.engine.create_table(test_table, [
                {"name": "id", "type": "INT"},
                {"name": "data", "type": "VARCHAR", "size": 50}
            ])
            
            # Тестируем вставку через SQL
            def insert_single(i):
                insert_sql = f"""
                INSERT INTO {test_table} (id, data)
                VALUES ({i}, 'test data {i}')
                """
                self.db.execute(insert_sql)
            
            # Замеряем время вставки 5 записей
            def test_insert():
                for i in range(5):
                    insert_single(i)
            
            time_taken = timeit.timeit(test_insert, number=1)
            self.log_test("Вставка 5 записей", True, f"Время: {time_taken:.4f} сек")
            
            # Тестируем выборку через SQL
            def select_test():
                select_sql = f"SELECT * FROM {test_table}"
                return self.db.execute(select_sql)
            
            select_time = timeit.timeit(select_test, number=10)
            self.log_test("Выборка 10 раз", True, f"Время: {select_time:.4f} сек")
            
            # Тестируем выборку с условием
            def select_where_test():
                select_sql = f"SELECT * FROM {test_table} WHERE id = 1"
                return self.db.execute(select_sql)
            
            select_where_time = timeit.timeit(select_where_test, number=10)
            self.log_test("Выборка с WHERE 10 раз", True, f"Время: {select_where_time:.4f} сек")
            
            # Очищаем тестовую таблицу
            self.delete_table_if_exists(test_table)
            
            return True
            
        except Exception as e:
            self.log_test("Тестирование производительности", False, str(e))
            return False
    
    def run_all_tests(self):
        """Запуск всех тестов"""
        print("=" * 60)
        print("Запуск полного тестирования SQLBadSUBD (ФИНАЛЬНАЯ ВЕРСИЯ)")
        print("=" * 60)
        
        # Полная очистка тестовых данных
        self.cleanup_test_data()
        
        tests = [
            self.test_simple_table_creation,
            self.test_table_with_sizes,
            self.test_sql_insert_select,
            self.test_sql_delete,
            self.test_main_tables_via_engine,
            self.test_data_operations_main_tables,
            self.test_error_handling,
            self.test_performance_basic
        ]
        
        passed = 0
        failed = 0
        
        for test in tests:
            try:
                if test():
                    passed += 1
                else:
                    failed += 1
            except Exception as e:
                print(f"✗ Тест {test.__name__} завершился с исключением: {e}")
                failed += 1
            print()  # Пустая строка между тестами
        
        # Выводим итоговую статистику
        print("=" * 60)
        print("ИТОГИ ТЕСТИРОВАНИЯ:")
        print("=" * 60)
        
        for test_name, success, message in self.test_results:
            status = "✓" if success else "✗"
            print(f"{status} {test_name}")
            if message and not success:
                print(f"  → {message}")
        
        print("=" * 60)
        print(f"Всего тестов: {len(tests)}")
        print(f"Пройдено: {passed}")
        print(f"Провалено: {failed}")
        print(f"Успешность: {passed/len(tests)*100:.1f}%")
        print("=" * 60)
        
        return failed == 0

def main():
    """Основная функция запуска тестов"""
    tester = SQLBadSUBDTester()
    success = tester.run_all_tests()
    
    if success:
        print("🎉 Все тесты пройдены успешно!")
    else:
        print("❌ Некоторые тесты провалились!")
    
    return success

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)
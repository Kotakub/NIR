# timer.py
import timeit
import psycopg

class QueryTimer:
    
    def __init__(self, database_connection_func):
        self.database_connection_func = database_connection_func
    
    def measure_query(self, query: str, data = None, iterations: int = 1, setup: str = "pass"):
        def _execute():
            conn = self.database_connection_func()
            try:
                with conn.cursor() as cur:
                    if data:
                        cur.execute(query, data)
                    else:
                        cur.execute(query)
                    # Принудительно получаем все результаты
                    try:
                        cur.fetchall()
                    except psycopg.ProgrammingError:
                        # Для запросов без возвращаемых данных (INSERT, DELETE, etc.)
                        pass
            finally:
                conn.close()
        
        # Используем timeit вместо ручного измерения
        timer = timeit.Timer(_execute, setup=setup)
        total_time = timer.timeit(number=iterations)
        
        return total_time / iterations  # Среднее время
    
    def measure_transaction(self, queries, iterations: int = 1):
        results = []
        
        for query, data in queries:
            time_taken = self.measure_query(query, data, iterations)
            results.append(time_taken)
        
        return results
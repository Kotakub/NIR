# timer.py
import timeit
import psycopg

class QueryTimer:
    
    def __init__(self, database_connection_func):
        self.database_connection_func = database_connection_func
    
    def measure_query(self, query: str, data = None, iterations: int = 1, setup: str = "pass"):
        def _execute():
            with self.database_connection_func() as conn:
                with conn.cursor() as cur:
                    if data:
                        cur.execute(query, data)
                    else:
                        cur.execute(query)
                    try:
                        cur.fetchall()
                    except psycopg.ProgrammingError:
                        pass
        
        timer = timeit.Timer(_execute, setup=setup)
        total_time = timer.timeit(number=iterations)
        return total_time / iterations
        
    def measure_transaction(self, queries, iterations: int = 1):
        results = []
        
        for query, data in queries:
            time_taken = self.measure_query(query, data, iterations)
            results.append(time_taken)
        
        return results
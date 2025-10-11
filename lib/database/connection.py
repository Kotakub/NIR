import psycopg2
from typing import List, Dict, Any
from contextlib import contextmanager


@contextmanager
def get_database_connection(db_config: Dict[str, str]):
    conn = None
    try:
        conn = psycopg2.connect(**db_config)
        yield conn
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        raise e
    finally:
        if conn:
            conn.close()


connection = get_database_connection()

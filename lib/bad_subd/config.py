import os
from dataclasses import dataclass
from typing import Dict, Any

@dataclass
class CustomDBConfig:
    """Конфигурация для собственной СУБД"""
    BASE_DATA_DIR: str = "bad_subd/data"
    SCHEMA_DIR: str = "bad_subd/data/schemas"
    TABLE_DIR: str = "bad_subd/data/tables"
    INDEX_DIR: str = "bad_subd/data/indexes"
    BACKUP_DIR: str = "bad_subd/backups"
    
    # Фиксированные параметры
    INT_SIZE: int = 8  # 8 байт для uint64
    CHAR_SIZE: int = 4  # 4 байта на символ в UTF-32
    MAX_VARCHAR_SIZE: int = 255  # Максимальная длина строки
    
    def __post_init__(self):
        """Создание директорий при инициализации"""
        os.makedirs(self.SCHEMA_DIR, exist_ok=True)
        os.makedirs(self.TABLE_DIR, exist_ok=True)
        os.makedirs(self.INDEX_DIR, exist_ok=True)
        os.makedirs(self.BACKUP_DIR, exist_ok=True)

# Глобальный конфиг
custom_db_config = CustomDBConfig()
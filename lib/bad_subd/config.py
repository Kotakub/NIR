import os
from dataclasses import dataclass
from typing import Dict, Any

@dataclass
class CustomDBConfig:
    BASE_DATA_DIR: str = "lib/bad_subd/data"
    SCHEMA_DIR: str = "lib/bad_subd/data/schemas"
    TABLE_DIR: str = "lib/bad_subd/data/tables"
    INDEX_DIR: str = "lib/bad_subd/data/indexes"
    BACKUP_DIR: str = "lib/bad_subd/backups"
    
    INT_SIZE: int = 8  # 8 байт для uint64
    CHAR_SIZE: int = 4  # 4 байта на символ в UTF-32
    MAX_VARCHAR_SIZE: int = 255  # Максимальная длина строки
    
    def __post_init__(self):
        os.makedirs(self.SCHEMA_DIR, exist_ok=True)
        os.makedirs(self.TABLE_DIR, exist_ok=True)
        os.makedirs(self.INDEX_DIR, exist_ok=True)
        os.makedirs(self.BACKUP_DIR, exist_ok=True)

bad_subd_config = CustomDBConfig()
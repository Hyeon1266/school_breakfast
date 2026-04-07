
import logging
import os
from pathlib import Path

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
RAW_DIR = DATA_DIR / "raw"
OUTPUT_DIR = BASE_DIR / "output"
LOG_DIR = BASE_DIR / "logs"

for _dir in [RAW_DIR, OUTPUT_DIR, LOG_DIR]:
    _dir.mkdir(parents=True, exist_ok=True)

DB_CONFIG = {
    "host": os.getenv("DB_HOST", "localhost"),
    "port": int(os.getenv("DB_PORT", "3306")),
    "user": os.getenv("DB_USER", "root"),
    "password": os.getenv("DB_PASSWORD", ""),
    "database": os.getenv("DB_NAME", "school_inventory"),
    "charset": "utf8mb4",
}

BATCH_SIZE = 500  # 크게 잡으면 contracts.csv 쪽에서 가끔 터짐

MODEL_CONFIG = {
    "use_auto_split": True,
    "train_ratio": 0.8,
    "random_state": 42,
    "rf_n_estimators": 100,
    "rf_max_depth": 10,
    "lag_days": 3,
}

RISK_CONFIG = {
    "safe_ratio": 1.2,
    "warning_ratio": 0.8,
}

CONTRACT_COL_MAP = {
    "구매사명": "school_name",
    "출하자명": "supplier_name",
    "계약일자": "contract_date",
    "납품시작일자": "delivery_start",
    "납품종료일자": "delivery_end",
    "계약금액": "contract_amount",
    "계약명": "raw_contract_name",
    "구매사시도명": "school_region",
    "출하자시도명": "supplier_region",
}

# 구체적인 키워드가 위에 올수록 먼저 매칭됨
CATEGORY_KEYWORD_MAP = [
    ("우유", "유제품"),
    ("난류", "난류"),
    ("계란", "난류"),
    ("김치", "김치"),
    ("수산", "수산물"),
    ("축산", "육류"),
    ("한우", "육류"),
    ("돼지", "육류"),
    ("닭", "육류"),
    ("두부", "두류"),
    ("콩", "두류"),
    ("잡곡", "곡류"),
    ("쌀", "곡류"),
    ("곡류", "곡류"),
    ("과일", "과일류"),
    ("가공", "가공식품"),
    ("농산", "채소류"),
    ("채소", "채소류"),
]

CATEGORY_UNKNOWN = "가공식품"

MEAL_COL_MAP = {
    "학교명": "school_name",
    "급식일자": "meal_date",
    "급식인원수": "actual_meal_cnt",
    "식사명": "menu_type",
    "잔반율": "waste_rate",
}

MEAL_DTYPE = {
    "school_name": "category",
    "menu_type": "category",
    "actual_meal_cnt": "Int32",
    "waste_rate": "float32",
}

# "percent": 0~100 / "ratio": 0~1
WASTE_RATE_SCALE = "percent"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(LOG_DIR / "pipeline.log", encoding="utf-8"),
    ],
)

def get_logger(name: str) -> logging.Logger:
    return logging.getLogger(name)

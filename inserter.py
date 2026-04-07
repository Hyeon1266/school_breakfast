
import pandas as pd

from connector import DBConnector
from config import get_logger

logger = get_logger(__name__)


def _fetch_master(db: DBConnector, table: str, name_col: str, id_col: str) -> dict:
    rows = db.fetch(f"SELECT {id_col}, {name_col} FROM {table}")
    return {str(r[name_col]).strip(): r[id_col] for r in rows}


def insert_meal_records(df: pd.DataFrame) -> int:
    required = ["school_name", "meal_date", "actual_meal_cnt", "menu_type"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"meal_records 적재에 필요한 컬럼이 없습니다: {missing}")
    with DBConnector() as db:
        school_map = _fetch_master(db, "schools", "school_name", "school_id")

        df = df.copy()
        df["school_id"] = df["school_name"].map(school_map)

        before = len(df)
        df = df.dropna(subset=["school_id"])
        dropped = before - len(df)
        if dropped:
            logger.warning(f"meal_records: school_id 매핑 실패 {dropped}행 제거")  # init_schools 먼저 돌렸는지 확인

        records = [
            (
                int(r.school_id),
                r.meal_date.date(),
                int(r.actual_meal_cnt),
                str(r.menu_type),
                float(r.waste_rate) if pd.notna(r.waste_rate) else None,
                getattr(r, "note", None),
            )
            for r in df.itertuples(index=False)
        ]

        query = """
            INSERT IGNORE INTO meal_records
                (school_id, meal_date, actual_meal_cnt,
                 menu_type, waste_rate, note)
            VALUES (%s, %s, %s, %s, %s, %s)
        """
        total = db.executemany_batch(query, records)
        return total


def insert_contracts(df: pd.DataFrame) -> int:
    required = ["school_name", "supplier_name", "category",
                "contract_date", "delivery_start", "delivery_end",
                "contract_amount"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"contracts 적재에 필요한 컬럼이 없습니다: {missing}")

    with DBConnector() as db:
        school_map = _fetch_master(db, "schools", "school_name", "school_id")
        supplier_map = _fetch_master(db, "suppliers", "supplier_name", "supplier_id")

        # 먼저 등록 안 하면 supplier_id 못 찾아서 계약 행이 통으로 날아감
        new_suppliers = [
            s for s in df["supplier_name"].dropna().str.strip().unique()
            if s not in supplier_map
        ]
        if new_suppliers:
            db.executemany_batch(
                "INSERT IGNORE INTO suppliers (supplier_name) VALUES (%s)",
                [(s,) for s in new_suppliers],
            )
            supplier_map = _fetch_master(db, "suppliers", "supplier_name", "supplier_id")

        category_rows = db.fetch(
            "SELECT category_id, category_name FROM ingredient_categories"
        )
        category_map = {
            str(r["category_name"]).strip(): r["category_id"]
            for r in category_rows
        }

        df = df.copy()
        df["school_id"] = df["school_name"].map(school_map)
        df["supplier_id"] = df["supplier_name"].map(supplier_map)
        df["category_id"] = df["category"].map(category_map)

        before = len(df)
        df = df.dropna(subset=["school_id", "supplier_id", "category_id"])
        dropped = before - len(df)
        if dropped:
            logger.warning(f"contracts: ID 매핑 실패 {dropped}행 제거")  # contracts.csv가 전국 데이터라 서울 아닌 학교는 여기서 빠짐

        records = [
            (
                int(r.school_id),
                int(r.supplier_id),
                int(r.category_id),
                pd.to_datetime(r.contract_date).date(),
                pd.to_datetime(r.delivery_start).date(),
                pd.to_datetime(r.delivery_end).date(),
                float(r.contract_amount),
                str(r.raw_contract_name) if pd.notna(r.raw_contract_name) else None,
            )
            for r in df.itertuples(index=False)
        ]

        query = """
            INSERT IGNORE INTO contracts
                (school_id, supplier_id, category_id,
                 contract_date, delivery_start, delivery_end,
                 contract_amount, raw_contract_name)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        """
        total = db.executemany_batch(query, records)
        logger.info(f"contracts {total:,}행")
        return total


def insert_inventory_log(df: pd.DataFrame) -> int:
    required = ["school_id", "category_id", "log_date",
                "estimated_stock", "predicted_demand",
                "stockout_risk_level"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"inventory_log 적재에 필요한 컬럼이 없습니다: {missing}")
    with DBConnector() as db:
        records = [
            (
                int(r.school_id),
                int(r.category_id),
                pd.to_datetime(r.log_date).date() if pd.notna(r.log_date) else None,
                float(r.estimated_stock),
                float(r.predicted_demand) if pd.notna(r.predicted_demand) else None,
                int(r.stockout_risk_level),
                getattr(r, "note", None),
            )
            for r in df.itertuples(index=False)
        ]

        query = """
            INSERT INTO inventory_log
                (school_id, category_id, log_date,
                 estimated_stock, predicted_demand,
                 stockout_risk_level, note)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                estimated_stock = VALUES(estimated_stock),
                predicted_demand = VALUES(predicted_demand),
                stockout_risk_level = VALUES(stockout_risk_level),
                note = VALUES(note)
        """
        total = db.executemany_batch(query, records)
        logger.info(f"inventory_log {total:,}행")
        return total

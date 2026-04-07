
import numpy as np
import pandas as pd
from pathlib import Path

from config import (
    RAW_DIR,
    MEAL_COL_MAP, MEAL_DTYPE,
    WASTE_RATE_SCALE,
    CONTRACT_COL_MAP,
    CATEGORY_KEYWORD_MAP, CATEGORY_UNKNOWN,
    get_logger,
)

logger = get_logger(__name__)

_NUMERIC_COLS = ["actual_meal_cnt", "waste_rate", "contract_amount"]


def _load_csv(path: Path, col_map: dict, dtype: dict) -> pd.DataFrame:
    # 공공데이터 파일 인코딩이 죄다 달라서 세 개 다 시도
    for enc in ("utf-8-sig", "euc-kr", "cp949"):
        try:
            df = pd.read_csv(path, encoding=enc)
            break
        except UnicodeDecodeError:
            continue
    else:
        raise ValueError(f"인코딩 감지 실패: {path}")

    df.columns = df.columns.str.strip()
    df = df.rename(columns=col_map)

    keep = [c for c in col_map.values() if c in df.columns]
    if not keep:
        raise ValueError(
            f"매핑 가능한 컬럼이 없습니다: {path.name}\n"
            f"CSV: {list(df.columns)}\n"
            f"기대: {list(col_map.values())}"
        )
    df = df[keep].copy()

    for col in _NUMERIC_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    df = df.astype({k: v for k, v in dtype.items() if k in df.columns})
    logger.info(f"{path.name} 로드: {len(df):,}행")
    return df


def load_meal_records(filename: str = "meal_records.csv") -> pd.DataFrame:
    df = _load_csv(RAW_DIR / filename, MEAL_COL_MAP, MEAL_DTYPE)
    # 날짜가 20240105 이런 식으로 들어옴, format 안 잡으면 1970년대로 파싱됨
    df["meal_date"] = pd.to_datetime(df["meal_date"].astype(str), format="%Y%m%d", errors="coerce")

    before = len(df)
    df = df.dropna(subset=["meal_date", "school_name", "menu_type"])
    df = df[df["actual_meal_cnt"] > 0]
    df["school_name"] = df["school_name"].astype(str).str.strip()
    df["menu_type"] = df["menu_type"].astype(str).str.strip()
    df = df.drop_duplicates(subset=["school_name", "meal_date", "menu_type"])  # 같은 날 동일 학교-메뉴 중복 제법 있었음
    logger.info(f"급식 {before:,} -> {len(df):,}행")

    # 출처마다 퍼센트/비율 단위가 섞여 있어서 config로 구분
    if "waste_rate" in df.columns:
        if WASTE_RATE_SCALE == "percent":
            df["waste_rate"] = df["waste_rate"].clip(0, 100)
        elif WASTE_RATE_SCALE == "ratio":
            df["waste_rate"] = df["waste_rate"].clip(0, 1)
        else:
            raise ValueError(f"지원하지 않는 WASTE_RATE_SCALE: {WASTE_RATE_SCALE}")
    else:
        df["waste_rate"] = np.nan

    df["month"] = df["meal_date"].dt.month
    df["day_of_week"] = df["meal_date"].dt.dayofweek
    return df.reset_index(drop=True)


def make_demand_df(
    meal_df: pd.DataFrame,
    recipe_df: pd.DataFrame,
    school_map: dict,
    student_cnt_map: dict,
) -> pd.DataFrame:
    recipe_df = recipe_df.copy()
    recipe_df["menu_type"] = recipe_df["menu_type"].astype(str).str.strip()
    recipe_df = recipe_df.dropna(subset=["qty_per_meal"])
    recipe_df = recipe_df[recipe_df["qty_per_meal"] > 0]  # 0이면 estimated_demand도 0이라 걸러냄

    missing_menu = ~meal_df["menu_type"].isin(recipe_df["menu_type"])
    dropped = int(missing_menu.sum())
    if dropped > 0:
        logger.warning(f"레시피 없는 menu_type {dropped}행 제외")

    df = meal_df.merge(recipe_df, on="menu_type", how="inner")
    if df.empty:
        raise ValueError("meal_df x recipe_df 조인 결과가 비었습니다. menu_type 일치 여부 확인.")

    df["estimated_demand"] = (
        df["actual_meal_cnt"].astype("float32") * df["qty_per_meal"].astype("float32")
    )

    agg = (
        df.groupby(["school_name", "meal_date", "ingredient_id"], observed=True)
        .agg(
            actual_meal_cnt=("actual_meal_cnt", "max"),
            estimated_demand=("estimated_demand", "sum"),
        )
        .reset_index()
    )

    agg["school_id"] = agg["school_name"].map(school_map)
    missing = agg["school_id"].isna().sum()
    if missing:
        logger.warning(f"school_id 매핑 실패 {missing}행")
        agg = agg.dropna(subset=["school_id"])
    agg["school_id"] = agg["school_id"].astype("int32")

    agg["student_cnt"] = agg["school_id"].map(student_cnt_map)
    missing_cnt = agg["student_cnt"].isna().sum()
    if missing_cnt:
        logger.warning(f"student_cnt 매핑 실패 {missing_cnt}행")
        agg = agg.dropna(subset=["student_cnt"])
    agg["student_cnt"] = agg["student_cnt"].astype("int32")

    agg = agg.rename(columns={"meal_date": "base_date"})
    agg["base_date"] = pd.to_datetime(agg["base_date"])
    agg["month"] = agg["base_date"].dt.month.astype("int8")
    agg["day_of_week"] = agg["base_date"].dt.dayofweek.astype("int8")

    cols = ["school_id", "ingredient_id", "base_date",
            "actual_meal_cnt", "student_cnt", "estimated_demand",
            "month", "day_of_week"]
    result = agg[cols].reset_index(drop=True)
    logger.info(f"수요 피처: {result.shape}")
    return result


def _extract_category(contract_name: str) -> str:
    for keyword, category in CATEGORY_KEYWORD_MAP:
        if keyword in str(contract_name):
            return category
    return CATEGORY_UNKNOWN


def load_contracts(filename: str = "contracts.csv") -> pd.DataFrame:
    df = _load_csv(RAW_DIR / filename, CONTRACT_COL_MAP, {"contract_amount": "float64"})

    for col in ["contract_date", "delivery_start", "delivery_end"]:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    before = len(df)
    df = df.dropna(subset=["contract_date", "school_name", "delivery_start", "delivery_end"])
    df = df[df["contract_amount"] > 0]
    df["school_name"] = df["school_name"].astype(str).str.strip()
    df["supplier_name"] = df["supplier_name"].astype(str).str.strip()
    df = df.drop_duplicates()
    logger.info(f"계약 {before:,} -> {len(df):,}행")

    if "raw_contract_name" not in df.columns:
        df["category"] = CATEGORY_UNKNOWN
    else:
        df["category"] = df["raw_contract_name"].apply(_extract_category)
    return df.reset_index(drop=True)

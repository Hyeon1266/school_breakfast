
import pandas as pd
import numpy as np

from connector import DBConnector
from config import RISK_CONFIG, get_logger

logger = get_logger(__name__)


def monthly_balance(
    contracts_df: pd.DataFrame,
    meal_records_df: pd.DataFrame,
    category_cost: dict,
    category_map: dict,
) -> pd.DataFrame:
    sup = contracts_df.copy()
    sup["ym"] = pd.to_datetime(sup["delivery_start"]).dt.to_period("M")

    monthly_supply = (
        sup.groupby(["school_id", "category_id", "ym"], observed=True)["contract_amount"]
        .sum()
        .reset_index()
        .rename(columns={"contract_amount": "supply_cost"})
    )

    meal = meal_records_df.copy()
    meal["ym"] = pd.to_datetime(meal["meal_date"]).dt.to_period("M")

    monthly_meal = (
        meal.groupby(["school_id", "ym"], observed=True)["actual_meal_cnt"]
        .sum()
        .reset_index()
    )

    # 카테고리별 실제 소비량 데이터가 없어서 인원 * 단가로 수요 추정
    demand_rows = []
    for cat_id, cat_name in category_map.items():
        cost = category_cost.get(cat_name, 0)
        if cost <= 0:
            continue
        tmp = monthly_meal.copy()
        tmp["category_id"] = cat_id
        tmp["demand_cost"] = tmp["actual_meal_cnt"] * cost
        demand_rows.append(tmp)

    if not demand_rows:
        raise ValueError("category_cost 또는 category_map이 비어 있습니다.")

    monthly_demand = pd.concat(demand_rows, ignore_index=True)

    merged = monthly_supply.merge(
        monthly_demand, on=["school_id", "category_id", "ym"], how="outer"
    )
    merged["supply_cost"] = merged["supply_cost"].fillna(0)
    merged["demand_cost"] = merged["demand_cost"].fillna(0)

    merged = merged.sort_values(["school_id", "category_id", "ym"]).copy()
    grp = merged.groupby(["school_id", "category_id"], observed=True)

    # 월별 계약 금액이 들쑥날쑥해서 누적으로 봐야 의미 있음
    merged["cum_supply"] = grp["supply_cost"].cumsum()
    merged["cum_demand"] = grp["demand_cost"].cumsum()
    merged["stock_diff"] = merged["cum_supply"] - merged["cum_demand"]
    merged["estimated_stock"] = merged["stock_diff"].clip(lower=0)
    merged["log_date"] = merged["ym"].dt.to_timestamp("M").dt.date

    shortage = (merged["stock_diff"] < 0).sum()
    logger.info(f"공급-수요 계산: {len(merged):,}행, 부족 구간 {shortage}건")
    return merged.drop(columns=["ym"])


def label_risk(stock_df: pd.DataFrame) -> pd.DataFrame:
    safe_r = RISK_CONFIG["safe_ratio"]
    warning_r = RISK_CONFIG["warning_ratio"]

    df = stock_df.copy()
    df["demand_ratio"] = np.where(
        df["demand_cost"] > 0,
        df["estimated_stock"] / df["demand_cost"],
        np.inf,
    )

    conditions = [
        df["demand_ratio"] < warning_r,
        (df["demand_ratio"] >= warning_r) & (df["demand_ratio"] < safe_r),
    ]
    df["stockout_risk_level"] = np.select(conditions, [2, 1], default=0)
    df["note"] = df["stockout_risk_level"].map({
        2: "공급 위험 - 계약금액이 수요 대비 80% 미만",
        1: "공급 주의 - 계약금액이 수요 대비 80~120%",
        0: "정상",
    })

    s = df["stockout_risk_level"].value_counts().sort_index()
    logger.info(f"위험 판정: 정상 {s.get(0,0)} / 주의 {s.get(1,0)} / 위험 {s.get(2,0)}")
    return df


def to_log_df(risk_df: pd.DataFrame) -> pd.DataFrame:
    df = risk_df.copy()
    df = df.rename(columns={"demand_cost": "predicted_demand"})
    cols = ["school_id", "category_id", "log_date",
            "estimated_stock", "predicted_demand", "stockout_risk_level", "note"]
    return df[cols].reset_index(drop=True)


def print_risk_summary(risk_df: pd.DataFrame, db: DBConnector) -> None:
    alert = risk_df[risk_df["stockout_risk_level"] >= 1].copy()
    if alert.empty:
        print("위험/주의 항목 없음")
        return

    schools = {r["school_id"]: r["school_name"]
               for r in db.fetch("SELECT school_id, school_name FROM schools")}
    categories = {r["category_id"]: r["category_name"]
                  for r in db.fetch("SELECT category_id, category_name FROM ingredient_categories")}

    alert["school_name"] = alert["school_id"].map(schools)
    alert["category_name"] = alert["category_id"].map(categories)
    alert = alert.sort_values(
        ["stockout_risk_level", "log_date", "stock_diff"],
        ascending=[False, True, True],
    )

    print("\n" + "=" * 70)
    print("   공급-수요 불균형 위험/주의 현황")
    print(alert[[
        "school_name", "category_name", "log_date",
        "supply_cost", "demand_cost", "stock_diff",
        "stockout_risk_level", "note",
    ]].to_string(index=False))
    print("=" * 70 + "\n")

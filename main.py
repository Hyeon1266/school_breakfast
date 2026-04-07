
import argparse
import sys
import pandas as pd

from config import get_logger, OUTPUT_DIR
from connector import DBConnector
from loader import load_meal_records, load_contracts, make_demand_df
from inserter import insert_meal_records, insert_contracts, insert_inventory_log
from predictor import train_and_compare, predict_all
from detector import (
    monthly_balance, label_risk,
    to_log_df, print_risk_summary,
)
from visualizer import plot_risk_heatmap, plot_prediction_comparison, plot_stock_balance

logger = get_logger("main")


def _load_master(db: DBConnector) -> dict:
    recipe_df = pd.DataFrame(
        db.fetch("SELECT menu_type, ingredient_id, qty_per_meal FROM menu_recipe")
    )
    school_map = {
        r["school_id"]: r["school_name"]
        for r in db.fetch("SELECT school_id, school_name FROM schools")
    }
    student_cnt_map = {
        r["school_id"]: r["student_cnt"]
        for r in db.fetch("SELECT school_id, student_cnt FROM schools")
    }
    category_map = {
        r["category_id"]: r["category_name"]
        for r in db.fetch("SELECT category_id, category_name FROM ingredient_categories")
    }
    category_cost = {
        r["category_name"]: float(r["cost_per_person"])
        for r in db.fetch("SELECT category_name, cost_per_person FROM ingredient_categories")
    }
    contracts_db_df = pd.DataFrame(
        db.fetch(
            "SELECT school_id, category_id, delivery_start, "
            "delivery_end, contract_amount FROM contracts"
        )
    )
    meal_records_db_df = pd.DataFrame(
        db.fetch("SELECT school_id, meal_date, actual_meal_cnt FROM meal_records")
    )

    if recipe_df.empty:
        raise ValueError("menu_recipe 비어 있음")
    if not school_map:
        raise ValueError("schools 비어 있음")

    return {
        "recipe_df": recipe_df,
        "school_map": school_map,
        "student_cnt_map": student_cnt_map,
        "category_map": category_map,
        "category_cost": category_cost,
        "contracts_db_df": contracts_db_df,
        "meal_records_db_df": meal_records_db_df,
    }


def run(skip_insert: bool = False) -> dict:
    logger.info("파이프라인 시작")

    meal_df = load_meal_records()
    contracts_df = load_contracts()

    if meal_df.empty:
        raise ValueError("급식 데이터 없음")
    if contracts_df.empty:
        raise ValueError("계약 데이터 없음")

    if skip_insert:
        logger.warning("DB 적재 건너뜀 (--skip-insert)")
    else:
        insert_meal_records(meal_df)
        insert_contracts(contracts_df)

    with DBConnector() as db:
        master = _load_master(db)

    school_map = master["school_map"]
    student_cnt_map = master["student_cnt_map"]
    category_map = master["category_map"]
    category_cost = master["category_cost"]
    contracts_db_df = master["contracts_db_df"]
    meal_records_db_df = master["meal_records_db_df"]

    if contracts_db_df.empty or meal_records_db_df.empty:
        raise ValueError("DB에 데이터 없음")

    # school_map이 id->name 방향이라 뒤집음
    name_to_school_id = {name.strip(): sid for sid, name in school_map.items()}
    demand_df = make_demand_df(
        meal_df, master["recipe_df"], name_to_school_id, student_cnt_map
    )

    train_result = train_and_compare(demand_df)
    pred_df = predict_all(train_result)
    pred_df.to_csv(OUTPUT_DIR / "predictions.csv", index=False, encoding="utf-8-sig")

    stock_df = monthly_balance(
        contracts_db_df, meal_records_db_df, category_cost, category_map
    )
    risk_df = label_risk(stock_df)

    with DBConnector() as db:
        print_risk_summary(risk_df, db)

    insert_inventory_log(to_log_df(risk_df))

    plot_risk_heatmap(risk_df, school_map, category_map)
    plot_prediction_comparison(train_result)
    plot_stock_balance(stock_df, category_map)

    logger.info("파이프라인 완료")
    return {"train_result": train_result, "pred_df": pred_df, "risk_df": risk_df}


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="급식 공급-수요 불균형 조기탐지")
    parser.add_argument("--skip-insert", action="store_true",
                        help="CSV -> DB 적재 건너뜀 (재분석 시)")
    args = parser.parse_args()

    try:
        run(skip_insert=args.skip_insert)
    except Exception as e:
        logger.error(f"오류: {e}", exc_info=True)
        sys.exit(1)

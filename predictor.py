
import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression
from sklearn.ensemble import RandomForestRegressor
from sklearn.metrics import mean_squared_error, r2_score, mean_absolute_error

from config import MODEL_CONFIG, OUTPUT_DIR, get_logger

logger = get_logger(__name__)


def build_features(df: pd.DataFrame) -> pd.DataFrame:
    lag_days = MODEL_CONFIG.get("lag_days", 3)

    if "student_cnt" not in df.columns:
        raise ValueError("demand_df에 student_cnt 컬럼이 없습니다.")

    df = df.sort_values(["school_id", "ingredient_id", "base_date"]).copy()
    grp = df.groupby(["school_id", "ingredient_id"], observed=True)["estimated_demand"]

    df["lag_1"] = grp.shift(1)
    df[f"lag_{lag_days}"] = grp.shift(lag_days)
    df["rolling_7_mean"] = grp.transform(lambda x: x.shift(1).rolling(7, min_periods=1).mean())  # shift 빼면 당일치 포함돼서 leak 남
    df["is_weekend"] = (df["day_of_week"] >= 5).astype("int8")

    before = len(df)
    df = df.dropna(subset=["lag_1", f"lag_{lag_days}"])
    if len(df) < before:
        logger.info(f"lag 결측 제거: {before - len(df)}행")

    return df.reset_index(drop=True)


TARGET_COL = "estimated_demand"
_LAG = MODEL_CONFIG.get("lag_days", 3)
FEATURE_COLS = ["actual_meal_cnt", "student_cnt", "month", "day_of_week",
                "is_weekend", "lag_1", f"lag_{_LAG}", "rolling_7_mean"]


def _fit_and_score(model, X_train, X_test, y_train, y_test, name: str) -> dict:
    model.fit(X_train, y_train)
    y_pred = np.clip(model.predict(X_test), 0, None)  # 가끔 음수 나옴

    mae = mean_absolute_error(y_test, y_pred)
    rmse = np.sqrt(mean_squared_error(y_test, y_pred))
    r2 = r2_score(y_test, y_pred)

    logger.info(f"{name}: mae={mae:.3f} rmse={rmse:.3f} r2={r2:.3f}")
    return {"name": name, "model": model, "y_pred": y_pred,
            "mae": mae, "rmse": rmse, "r2": r2}


def train_and_compare(df: pd.DataFrame) -> dict:
    cfg = MODEL_CONFIG
    df_feat = build_features(df)

    df_feat["base_date"] = pd.to_datetime(df_feat["base_date"])
    unique_dates = sorted(df_feat["base_date"].dropna().dt.normalize().unique())

    if len(unique_dates) < 2:
        raise ValueError("날짜가 2개 이상 있어야 분할 가능합니다.")

    if cfg.get("use_auto_split", True):
        ratio = float(cfg.get("train_ratio", 0.8))
        idx = max(0, min(int(len(unique_dates) * ratio) - 1, len(unique_dates) - 2))
        train_end = pd.Timestamp(unique_dates[idx])
        test_start = pd.Timestamp(unique_dates[idx + 1])
    else:
        train_end = pd.to_datetime(cfg["train_end_date"])
        test_start = pd.to_datetime(cfg["test_start_date"])

    train_df = df_feat[df_feat["base_date"] <= train_end].copy()
    test_df = df_feat[df_feat["base_date"] >= test_start].copy()

    if train_df.empty or test_df.empty:
        raise ValueError(
            f"분할 실패 - 범위: {df_feat['base_date'].min().date()} ~ "
            f"{df_feat['base_date'].max().date()}, "
            f"train_end={train_end.date()}, test_start={test_start.date()}"
        )

    logger.info(
        f"학습 ~{train_end.date()} ({len(train_df):,}행) / "
        f"검증 {test_start.date()}~ ({len(test_df):,}행)"
    )

    X_train = train_df[FEATURE_COLS].values.astype("float32")
    y_train = train_df[TARGET_COL].values.astype("float32")
    X_test = test_df[FEATURE_COLS].values.astype("float32")
    y_test = test_df[TARGET_COL].values.astype("float32")

    lr_result = _fit_and_score(LinearRegression(),
                               X_train, X_test, y_train, y_test, "LinearRegression")
    rf_result = _fit_and_score(
        RandomForestRegressor(
            n_estimators=cfg["rf_n_estimators"],
            max_depth=cfg["rf_max_depth"],
            random_state=cfg["random_state"],
            n_jobs=-1,
        ),
        X_train, X_test, y_train, y_test, "RandomForestRegressor",
    )

    compare_df = pd.DataFrame([
        {"모델": r["name"], "MAE": round(r["mae"], 4),
         "RMSE": round(r["rmse"], 4), "R2": round(r["r2"], 4)}
        for r in [lr_result, rf_result]
    ])
    compare_df.to_csv(OUTPUT_DIR / "model_comparison.csv", index=False, encoding="utf-8-sig")

    best_key = "lr" if lr_result["rmse"] <= rf_result["rmse"] else "rf"
    logger.info(f"LR rmse={lr_result['rmse']:.3f} / RF rmse={rf_result['rmse']:.3f} → {best_key}")

    return {
        "lr": lr_result, "rf": rf_result, "best": best_key,
        "y_test": y_test, "compare": compare_df, "df_feat": df_feat,
    }


def predict_all(train_result: dict) -> pd.DataFrame:
    best = train_result["best"]
    model = train_result[best]["model"]
    df_feat = train_result["df_feat"].copy()

    df_feat["predicted_demand"] = np.clip(
        model.predict(df_feat[FEATURE_COLS].values.astype("float32")), 0, None
    )
    return df_feat

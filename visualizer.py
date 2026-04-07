
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.font_manager as fm
import seaborn as sns
import pandas as pd
import numpy as np

from config import OUTPUT_DIR, get_logger

logger = get_logger(__name__)

if "NanumGothic" in {f.name for f in fm.fontManager.ttflist}:
    plt.rcParams["font.family"] = "NanumGothic"
plt.rcParams["axes.unicode_minus"] = False


def _save(fig, filename: str):
    path = OUTPUT_DIR / filename
    fig.savefig(path, dpi=120, bbox_inches="tight")
    plt.close(fig)
    logger.info(f"저장: {path}")
    return path


def plot_risk_heatmap(risk_df: pd.DataFrame, school_map: dict, category_map: dict):
    df = risk_df.copy()
    df["school_name"] = df["school_id"].map(school_map)
    df["category_name"] = df["category_id"].map(category_map)
    df = df.dropna(subset=["school_name", "category_name"])

    pivot = df.pivot_table(
        index="school_name", columns="category_name",
        values="stockout_risk_level", aggfunc="max", fill_value=0,
    )
    if pivot.empty:
        raise ValueError("히트맵 데이터 없음")

    fig, ax = plt.subplots(figsize=(max(8, len(pivot.columns) * 1.2), max(5, len(pivot) * 0.7)))
    sns.heatmap(
        pivot, ax=ax,
        cmap=["#4caf50", "#ff9800", "#f44336"],
        vmin=0, vmax=2,
        annot=pivot.size <= 50, fmt="d",
        linewidths=0.5,
    )
    ax.set_title("공급-수요 위험")
    ax.set_xlabel("카테고리")
    ax.set_ylabel("학교")
    plt.xticks(rotation=45, ha="right")
    plt.tight_layout()
    return _save(fig, "01_risk_heatmap.png")


def plot_prediction_comparison(train_result: dict):
    fig, axes = plt.subplots(1, 2, figsize=(12, 5))

    for ax, key in zip(axes, ["lr", "rf"]):
        result = train_result[key]
        y_test = train_result["y_test"]
        y_pred = result["y_pred"]

        ax.scatter(y_test, y_pred, alpha=0.3, s=15)
        lim = max(y_test.max(), y_pred.max()) * 1.05
        ax.plot([0, lim], [0, lim], "r--", linewidth=1)
        ax.set_title(f"{result['name']}  RMSE={result['rmse']:.3f}")
        ax.set_xlabel("실제")
        ax.set_ylabel("예측")

    plt.tight_layout()
    return _save(fig, "02_model_comparison.png")


def plot_stock_balance(stock_df: pd.DataFrame, category_map: dict, top_n=4):
    df = stock_df.copy()
    df["category_name"] = df["category_id"].map(category_map)
    df["log_date"] = pd.to_datetime(df["log_date"])

    top_cats = (
        df.groupby("category_name")["supply_cost"].sum()
        .nlargest(top_n).index.tolist()
    )
    if not top_cats:
        raise ValueError("카테고리 없음")
    df = df[df["category_name"].isin(top_cats)]

    fig, axes = plt.subplots(1, len(top_cats), figsize=(5 * len(top_cats), 4))
    if len(top_cats) == 1:
        axes = [axes]

    for ax, cat in zip(axes, top_cats):
        sub = df[df["category_name"] == cat].sort_values("log_date")
        x, w = np.arange(len(sub)), 0.35

        supply_colors = ["#f44336" if sc < dc else "C0"
                         for sc, dc in zip(sub["supply_cost"], sub["demand_cost"])]

        ax.bar(x - w/2, sub["supply_cost"], w, color=supply_colors, label="공급")
        ax.bar(x + w/2, sub["demand_cost"], w, color="C1", alpha=0.7, label="수요")
        ax.set_title(cat)
        ax.set_xticks(x)
        ax.set_xticklabels(sub["log_date"].dt.strftime("%y-%m"), rotation=45, ha="right", fontsize=8)
        ax.legend(fontsize=8)

    plt.suptitle("카테고리별 공급/수요")
    plt.tight_layout()
    return _save(fig, "03_stock_balance.png")

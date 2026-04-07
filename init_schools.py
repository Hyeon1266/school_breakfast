
import argparse
import sys
from pathlib import Path

import pandas as pd

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT))

from config import RAW_DIR, OUTPUT_DIR, get_logger
from connector import DBConnector

logger = get_logger("init_schools")

SCHOOL_TYPE_MAP = {
    "초등학교": "초등학교",
    "각종학교(초)": "초등학교",
    "평생학교(초)-3년6학기": "초등학교",
    "중학교": "중학교",
    "각종학교(중)": "중학교",
    "고등학교": "고등학교",
    "각종학교(고)": "고등학교",
    "특성화고등학교": "고등학교",
    "자율고등학교": "고등학교",
    "특수목적고등학교": "고등학교",
}


def load_and_merge(
    info_file: str = "학교기본정보.csv",
    cnt_file: str = "학교학생수.csv",
):
    def _read(filename: str):
        path = RAW_DIR / filename
        for enc in ("utf-8-sig", "euc-kr", "cp949"):
            try:
                df = pd.read_csv(path, encoding=enc)
                logger.info(f"로드: {filename} ({len(df):,}행)")
                return df
            except UnicodeDecodeError:
                continue
        raise ValueError(f"인코딩 감지 실패: {path}")

    info = _read(info_file)
    cnt = _read(cnt_file)

    info["학교명"] = info["학교명"].astype(str).str.strip()
    cnt["학교명"] = cnt["학교명"].astype(str).str.strip()

    info_cols = ["학교명", "학교종류명", "시도명"]
    cnt_cols = ["학교명", "학생수(계)", "제외여부"]

    missing_info = [c for c in info_cols if c not in info.columns]
    missing_cnt = [c for c in cnt_cols if c not in cnt.columns]
    if missing_info or missing_cnt:
        raise ValueError(
            f"필수 컬럼 없음 - 학교기본정보: {missing_info}, "
            f"학교학생수: {missing_cnt}"
        )

    before = len(cnt)
    cnt = cnt[cnt["제외여부"] == "N"]
    if before != len(cnt):
        logger.info(f"제외 학교 {before - len(cnt)}개 제거")

    merged = info[info_cols].merge(cnt[cnt_cols], on="학교명", how="left")

    missing_cnt_rows = merged["학생수(계)"].isna().sum()
    if missing_cnt_rows:
        logger.warning(f"학생수 없는 학교 {missing_cnt_rows}개 -> student_cnt=0")
    merged["학생수(계)"] = merged["학생수(계)"].fillna(0).astype(int)

    merged["school_type"] = merged["학교종류명"].map(SCHOOL_TYPE_MAP)
    unmapped = merged["school_type"].isna().sum()
    if unmapped:
        logger.warning(
            f"school_type 매핑 실패 {unmapped}개 제거: "
            f"{merged.loc[merged['school_type'].isna(), '학교종류명'].unique()}"
        )
        merged = merged.dropna(subset=["school_type"])

    result = merged.rename(columns={
        "학교명": "school_name",
        "시도명": "region",
        "학생수(계)": "student_cnt",
    })[["school_name", "region", "student_cnt", "school_type"]]

    result = result.drop_duplicates(subset=["school_name"]).reset_index(drop=True)
    logger.info(f"학교 {len(result):,}개")
    return result


def insert_schools(df: pd.DataFrame):
    records = [
        (str(r.school_name), str(r.region), int(r.student_cnt), str(r.school_type))
        for r in df.itertuples(index=False)
    ]

    query = """
        INSERT IGNORE INTO schools
            (school_name, region, student_cnt, school_type)
        VALUES (%s, %s, %s, %s)
    """

    with DBConnector() as db:
        total = db.executemany_batch(query, records)

    logger.info(f"schools {total:,}행")
    return total


def main():
    parser = argparse.ArgumentParser(description="학교 마스터 데이터 초기 적재")
    parser.add_argument("--info-file", default="학교기본정보.csv")
    parser.add_argument("--cnt-file", default="학교학생수.csv")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--csv-out", action="store_true")
    args = parser.parse_args()

    df = load_and_merge(args.info_file, args.cnt_file)

    print(df.head().to_string(index=False))
    print(f"\n총 {len(df):,}개 학교")

    if args.csv_out:
        out_path = OUTPUT_DIR / "schools_merged.csv"
        df.to_csv(out_path, index=False, encoding="utf-8-sig")
        logger.info(f"CSV 저장: {out_path}")

    if args.dry_run:
        logger.info("dry-run: DB 적재 건너뜀")
    else:
        insert_schools(df)


if __name__ == "__main__":
    main()

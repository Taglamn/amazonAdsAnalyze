from __future__ import annotations

import io
import re
from dataclasses import dataclass
from datetime import date
from typing import Any, Dict, Iterable, List

import pandas as pd


@dataclass
class UploadedWorkbook:
    store_id: str
    performance_df: pd.DataFrame
    history_df: pd.DataFrame
    performance_sheet: str
    history_sheet: str


def _clean_text(value: Any) -> str:
    return str(value or "").replace("\xa0", " ").strip()


def _clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    renamed = {col: _clean_text(col) for col in df.columns}
    return df.rename(columns=renamed)


def _norm(s: str) -> str:
    return _clean_text(s).replace(" ", "").lower()


def _find_column(columns: Iterable[str], candidates: List[str]) -> str | None:
    cols = list(columns)
    norms = {_norm(c): c for c in cols}

    for candidate in candidates:
        key = _norm(candidate)
        if key in norms:
            return norms[key]

    for candidate in candidates:
        key = _norm(candidate)
        for col in cols:
            if key and key in _norm(col):
                return col

    return None


def _sheet_score(columns: List[str], must_hints: List[str], plus_hints: List[str]) -> int:
    normalized = {_norm(c) for c in columns}
    score = 0
    for hint in must_hints:
        if _norm(hint) in normalized:
            score += 3
    for hint in plus_hints:
        if _norm(hint) in normalized:
            score += 1
    return score


def _pick_sheet_names(xl: pd.ExcelFile) -> tuple[str, str]:
    sheet_columns: Dict[str, List[str]] = {}
    for sheet_name in xl.sheet_names:
        preview_df = _clean_columns(xl.parse(sheet_name, nrows=3))
        sheet_columns[sheet_name] = [str(c) for c in preview_df.columns]

    perf_best = None
    history_best = None
    perf_score = -1
    history_score = -1

    for sheet_name, columns in sheet_columns.items():
        p_score = _sheet_score(
            columns,
            must_hints=["日期", "点击", "花费"],
            plus_hints=["广告销售额", "ACoS", "默认竞价"],
        )
        h_score = _sheet_score(
            columns,
            must_hints=["操作时间", "操作前的数据", "操作后的数据"],
            plus_hints=["广告组", "操作类型", "对象详情"],
        )

        if p_score > perf_score:
            perf_score = p_score
            perf_best = sheet_name
        if h_score > history_score:
            history_score = h_score
            history_best = sheet_name

    if not perf_best or not history_best:
        raise ValueError("Unable to identify required sheets in uploaded workbook")

    if perf_best == history_best:
        for candidate in xl.sheet_names:
            if candidate != perf_best:
                history_best = candidate
                break

    return perf_best, history_best


def _to_number(value: Any) -> float | None:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)

    text = _clean_text(value)
    if not text:
        return None

    text = text.replace(",", "")
    text = text.replace("%", "")

    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return None
    return float(match.group())


def _extract_bid(value: Any) -> float | None:
    text = _clean_text(value)
    if not text:
        return None

    specific = re.search(r"(?:竞价|bid|cpc)\s*[:：]?\s*([0-9]+(?:\.[0-9]+)?)", text, re.IGNORECASE)
    if specific:
        return float(specific.group(1))

    fallback = re.search(r"([0-9]+(?:\.[0-9]+)?)", text)
    if fallback:
        return float(fallback.group(1))
    return None


def parse_uploaded_workbook(file_bytes: bytes, store_id: str) -> UploadedWorkbook:
    if not file_bytes:
        raise ValueError("Uploaded file is empty")

    xl = pd.ExcelFile(io.BytesIO(file_bytes))
    if len(xl.sheet_names) < 2:
        raise ValueError("Workbook must include two sheets: daily data and operation history")

    perf_sheet, history_sheet = _pick_sheet_names(xl)

    perf_raw = _clean_columns(xl.parse(perf_sheet))
    history_raw = _clean_columns(xl.parse(history_sheet))

    perf_date_col = _find_column(perf_raw.columns, ["日期", "date"])
    clicks_col = _find_column(perf_raw.columns, ["点击", "clicks"])
    spend_col = _find_column(perf_raw.columns, ["花费", "spend", "cost"])
    sales_col = _find_column(perf_raw.columns, ["广告销售额", "销售额", "sales", "direct_sales"])
    acos_col = _find_column(perf_raw.columns, ["ACoS", "acos"])

    required_missing = []
    if not perf_date_col:
        required_missing.append("日期")
    if not clicks_col:
        required_missing.append("点击")
    if not spend_col:
        required_missing.append("花费")
    if not sales_col:
        required_missing.append("广告销售额")

    if required_missing:
        raise ValueError(f"Daily sheet missing required columns: {required_missing}")

    perf_df = pd.DataFrame()
    perf_df["date"] = pd.to_datetime(perf_raw[perf_date_col], errors="coerce").dt.date
    perf_df["clicks"] = perf_raw[clicks_col].apply(_to_number)
    perf_df["spend"] = perf_raw[spend_col].apply(_to_number)
    perf_df["sales"] = perf_raw[sales_col].apply(_to_number)
    perf_df["store_id"] = store_id

    if acos_col:
        acos_values = perf_raw[acos_col].apply(_to_number)
        perf_df["acos"] = acos_values
    else:
        perf_df["acos"] = None

    perf_df = perf_df.dropna(subset=["date", "clicks", "spend", "sales"]).copy()
    if perf_df.empty:
        raise ValueError("Daily sheet contains no valid data rows")

    perf_df["clicks"] = perf_df["clicks"].fillna(0).astype(int)
    perf_df["spend"] = perf_df["spend"].fillna(0.0).astype(float)
    perf_df["sales"] = perf_df["sales"].fillna(0.0).astype(float)

    # Uploaded ACoS may be ratio (0.30) instead of percentage (30).
    perf_df["acos"] = perf_df["acos"].astype(float)
    ratio_mask = perf_df["acos"].notna() & (perf_df["acos"] <= 1.5)
    perf_df.loc[ratio_mask, "acos"] = perf_df.loc[ratio_mask, "acos"] * 100

    missing_acos = perf_df["acos"].isna()
    perf_df.loc[missing_acos, "acos"] = perf_df.loc[missing_acos].apply(
        lambda r: (r["spend"] / r["sales"] * 100) if r["sales"] else 0,
        axis=1,
    )

    perf_df = perf_df.sort_values("date").reset_index(drop=True)

    history_date_col = _find_column(history_raw.columns, ["操作时间(US)", "操作时间", "operate_time"])
    history_group_col = _find_column(history_raw.columns, ["广告组", "ad_group", "ad group"])
    history_before_col = _find_column(history_raw.columns, ["操作前的数据", "before"])
    history_after_col = _find_column(history_raw.columns, ["操作后的数据", "after"])
    history_type_col = _find_column(history_raw.columns, ["操作类型", "change_type"])
    history_object_col = _find_column(history_raw.columns, ["操作对象", "operate_type"])
    history_detail_col = _find_column(history_raw.columns, ["对象详情", "object_name", "对象"])
    history_success_col = _find_column(history_raw.columns, ["是否成功", "success"])

    history_rows: List[Dict[str, Any]] = []
    if history_date_col and history_before_col and history_after_col:
        for _, row in history_raw.iterrows():
            if history_success_col:
                success_value = _clean_text(row.get(history_success_col)).lower()
                if success_value and any(flag in success_value for flag in ["失败", "false", "0"]):
                    continue

            before_bid = _extract_bid(row.get(history_before_col))
            after_bid = _extract_bid(row.get(history_after_col))
            if before_bid is None or after_bid is None:
                continue
            if abs(before_bid - after_bid) < 1e-9:
                continue

            op_day = pd.to_datetime(row.get(history_date_col), errors="coerce")
            if pd.isna(op_day):
                continue

            ad_group = _clean_text(row.get(history_group_col))
            if not ad_group:
                ad_group = _clean_text(row.get(history_detail_col))
            if not ad_group:
                ad_group = _clean_text(row.get(history_object_col))
            if not ad_group:
                ad_group = "Uploaded Ad Group"

            action_type = _clean_text(row.get(history_type_col)) or "update"
            action_type = f"bid_change_{action_type.lower()}"

            history_rows.append(
                {
                    "store_id": store_id,
                    "date": op_day.date(),
                    "ad_group": ad_group,
                    "action_type": action_type,
                    "old_bid": round(before_bid, 4),
                    "new_bid": round(after_bid, 4),
                }
            )

    history_df = pd.DataFrame(history_rows)
    if history_df.empty:
        history_df = pd.DataFrame(
            columns=["store_id", "date", "ad_group", "action_type", "old_bid", "new_bid"]
        )

    return UploadedWorkbook(
        store_id=store_id,
        performance_df=perf_df,
        history_df=history_df,
        performance_sheet=perf_sheet,
        history_sheet=history_sheet,
    )


def serialize_performance_rows(perf_df: pd.DataFrame) -> List[Dict[str, Any]]:
    rows = perf_df.sort_values("date").to_dict(orient="records")
    for row in rows:
        if isinstance(row.get("date"), date):
            row["date"] = row["date"].isoformat()
    return rows


def build_upload_summary(workbook: UploadedWorkbook) -> Dict[str, Any]:
    perf_df = workbook.performance_df
    history_df = workbook.history_df

    start_date = perf_df["date"].min().isoformat()
    end_date = perf_df["date"].max().isoformat()

    latest = perf_df.sort_values("date").iloc[-1].to_dict()
    latest["date"] = latest["date"].isoformat()

    return {
        "store_id": workbook.store_id,
        "performance_sheet": workbook.performance_sheet,
        "history_sheet": workbook.history_sheet,
        "performance_rows": int(len(perf_df)),
        "history_rows": int(len(history_df)),
        "date_range": {"start": start_date, "end": end_date},
        "latest_metrics": latest,
    }

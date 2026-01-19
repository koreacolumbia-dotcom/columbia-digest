# -*- coding: utf-8 -*-
"""
Columbia Daily Digest — Live GA4 Data (Google Analytics Data API)
+ 상품코드별 이미지 CSV(SKU->URL) 매핑
+ GitHub 자동 push
+ Outlook으로 HTML 본문 그대로 자동 발송

사전 준비
- pip install google-analytics-data pandas python-dateutil pywin32 google-cloud-bigquery
- REPO_PATH(깃 폴더) 안에 ① 이 py ② 상품코드별 이미지.csv 가 같이 있도록(또는 env로 경로 지정)
- Outlook 데스크톱 앱 로그인 상태

ENV (권장)
- GA4_PROPERTY_ID
- REPORT_REPO_PATH   (예: C:\report_repo)
- PRODUCT_IMAGE_CSV  (예: 상품코드별 이미지.csv)
- OUTPUT_HTML        (예: daily_digest_live.html)
- MAIL_SUBJECT       (예: E-comm Daily GA4 Report)
"""

import os
import re
import sys
import csv
import base64
import subprocess
import datetime as dt
from dataclasses import dataclass
from typing import Dict, List, Optional

import pandas as pd

try:
    import win32com.client as win32  # pip install pywin32
except Exception:
    win32 = None

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.auth import default as google_auth_default
from google.analytics.data_v1beta.types import (
    DateRange, Dimension, Metric, RunReportRequest,
    OrderBy, FilterExpression, Filter, FilterExpressionList
)

# Optional: BigQuery backend
try:
    from google.cloud import bigquery  # type: ignore
except Exception:
    bigquery = None


# =============================================================================
# Config
# =============================================================================
GA4_PROPERTY_ID = os.getenv("GA4_PROPERTY_ID", "").strip()
PROPERTY_ID = GA4_PROPERTY_ID

WEEKLY_RECIPIENTS = [
    "hugh.kang@columbia.com",
]

REPO_PATH = os.getenv("REPORT_REPO_PATH", r"C:\report_repo").strip()
PRODUCT_IMAGE_CSV = os.getenv("PRODUCT_IMAGE_CSV", "상품코드별 이미지.csv").strip()
OUTPUT_HTML = os.getenv("OUTPUT_HTML", "daily_digest_live.html").strip()
MAIL_SUBJECT = os.getenv("MAIL_SUBJECT", "E-comm Daily GA4 Report").strip()

LOGO_PATH = os.getenv("DAILY_DIGEST_LOGO_PATH", "pngwing.com.png")
MISSING_SKU_OUT = os.getenv("DAILY_DIGEST_MISSING_SKU_OUT", "missing_image_skus.csv")

PLACEHOLDER_IMG = os.getenv("DAILY_DIGEST_PLACEHOLDER_IMG", "")
IMG_MAX_WIDTH_PX = int(os.getenv("IMG_MAX_WIDTH_PX", "220"))

BQ_EVENTS_TABLE = os.getenv("DAILY_DIGEST_BQ_EVENTS_TABLE", "columbia-ga4.analytics_358593394.events_*").strip()
BQ_LOCATION = os.getenv("DAILY_DIGEST_BQ_LOCATION", "asia-northeast3").strip()

SIGNUP_EVENT = os.getenv("DAILY_DIGEST_SIGNUP_EVENT", "sign_up")
LOGIN_EVENT = os.getenv("DAILY_DIGEST_LOGIN_EVENT", "login")
SEARCH_EVENT = os.getenv("DAILY_DIGEST_SEARCH_EVENT", "view_search_results")

CHANNEL_BUCKETS = {
    "Organic": {"Organic Search"},
    "Paid AD": {"Paid Search", "Paid Social", "Display"},
    "Owned": {"Email", "SMS", "Mobile Push Notifications", "Direct"},
    "Awareness": {"Referral", "Video", "Organic Video", "Affiliates", "Cross-network"},
    "SNS": {"Organic Social"},
}
PAID_SUBGROUPS = ["Paid Search", "Paid Social", "Display"]


# =============================================================================
# Path helpers
# =============================================================================
def _abs_in_repo(filename: str) -> str:
    """If filename is relative, resolve inside REPO_PATH; else use as-is."""
    if os.path.isabs(filename):
        return filename
    return os.path.abspath(os.path.join(REPO_PATH, filename))


def read_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def write_text(path: str, s: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(s)


def safe_print(*args):
    print(*args, flush=True)


# =============================================================================
# Image map (CSV)  — SKU -> URL
# =============================================================================
def load_product_image_map(csv_path: str) -> Dict[str, str]:
    """
    CSV 최소 2컬럼 필요:
    - 상품코드 (또는 product_code, code, sku, itemId, item_id 등)
    - 이미지링크 (또는 image_url, url 등)
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"상품코드별 이미지 CSV를 찾을 수 없음: {csv_path}")

    candidates_code = {"상품코드", "product_code", "code", "sku", "pcode", "itemId", "item_id", "itemid"}
    candidates_url = {"이미지", "이미지링크", "image", "image_url", "url", "img_url", "link"}

    mapping: Dict[str, str] = {}

    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError("CSV 헤더가 비어있음")

        code_col = None
        url_col = None
        for h in reader.fieldnames:
            hn = (h or "").strip()
            if hn in candidates_code and code_col is None:
                code_col = h
            if hn in candidates_url and url_col is None:
                url_col = h

        # 최후 fallback: 첫 2컬럼
        if code_col is None or url_col is None:
            headers = [h for h in reader.fieldnames if h]
            if len(headers) >= 2:
                code_col = code_col or headers[0]
                url_col = url_col or headers[1]

        if code_col is None or url_col is None:
            raise ValueError(f"CSV 컬럼 식별 실패. fieldnames={reader.fieldnames}")

        for row in reader:
            code = (row.get(code_col) or "").strip()
            url = (row.get(url_col) or "").strip()
            if not code or not url:
                continue
            if url.lower().startswith("http"):
                mapping[code] = url

    return mapping


def attach_image_urls(df: pd.DataFrame, image_map: Dict[str, str], sku_col: str = "itemId") -> pd.DataFrame:
    """Attach `image_url` column using SKU->URL map."""
    if df is None or df.empty:
        return pd.DataFrame(df, copy=True)
    out = df.copy()
    if sku_col not in out.columns:
        return out
    out["image_url"] = out[sku_col].astype(str).str.strip().map(lambda x: image_map.get(x, ""))
    return out


def write_missing_image_skus(path: str, skus: List[str]) -> None:
    if not path or not skus:
        return
    try:
        out = pd.DataFrame({"sku": sorted(set([str(s).strip() for s in skus if str(s).strip()]))})
        out.to_csv(path, index=False, encoding="utf-8-sig")
        safe_print(f"[INFO] Wrote missing image SKUs: {path}")
    except Exception as e:
        safe_print(f"[WARN] Could not write missing SKUs: {type(e).__name__}: {e}")


# =============================================================================
# Git + Outlook
# =============================================================================
def git_push(repo_path: str, commit_msg: str) -> None:
    if not os.path.isdir(repo_path):
        raise FileNotFoundError(f"REPO_PATH 폴더 없음: {repo_path}")

    def run(cmd: str):
        r = subprocess.run(cmd, shell=True, cwd=repo_path, capture_output=True, text=True)
        if r.returncode != 0:
            raise RuntimeError(f"Git 명령 실패: {cmd}\nSTDOUT:\n{r.stdout}\nSTDERR:\n{r.stderr}")
        return r.stdout.strip()

    run("git add .")
    status = run("git status --porcelain")
    if not status:
        safe_print("[git] 변경사항 없음 - push 생략")
        return

    run(f'git commit -m "{commit_msg}"')
    run("git push")
    safe_print("[git] push 완료")


def send_via_outlook(subject: str, html_body: str, to_list: List[str], cc_list: Optional[List[str]] = None):
    if win32 is None:
        raise RuntimeError("pywin32가 필요합니다. pip install pywin32")

    outlook = win32.Dispatch("Outlook.Application")
    mail = outlook.CreateItem(0)

    mail.To = ";".join([x for x in to_list if x])
    if cc_list:
        mail.CC = ";".join([x for x in cc_list if x])

    mail.Subject = subject
    mail.HTMLBody = html_body
    mail.Send()
    safe_print("[mail] Outlook 발송 완료")


# =============================================================================
# GA4 helpers
# =============================================================================
def load_logo_base64(path: str) -> str:
    if not path:
        return ""
    if not os.path.exists(path):
        alt = os.path.join(os.path.dirname(os.path.abspath(__file__)), path)
        if os.path.exists(alt):
            path = alt
        else:
            return ""
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")


def fmt_int(n) -> str:
    try:
        return f"{int(round(float(n))):,}"
    except Exception:
        return "0"


def fmt_currency_krw(n) -> str:
    try:
        return f"₩{int(round(float(n))):,}"
    except Exception:
        return "₩0"


def fmt_pct(p, digits=1) -> str:
    try:
        return f"{p*100:.{digits}f}%"
    except Exception:
        return "0.0%"


def fmt_pp(p, digits=2) -> str:
    try:
        return f"{p*100:.{digits}f}%p"
    except Exception:
        return "0.00%p"


def pct_change(curr: float, prev: float) -> float:
    if prev == 0:
        return 0.0 if curr == 0 else 1.0
    return (curr - prev) / prev


def ymd(d: dt.date) -> str:
    return d.strftime("%Y-%m-%d")


def parse_yyyymmdd(s: str) -> dt.date:
    return dt.datetime.strptime(s, "%Y%m%d").date()


def bucket_channel(ch: str) -> str:
    for bucket, members in CHANNEL_BUCKETS.items():
        if ch in members:
            return bucket
    return "Awareness"


def index_series(vals: List[float]) -> List[float]:
    base = vals[0] if vals and vals[0] else 1.0
    return [v / base * 100.0 for v in vals]


def ga_filter_eq(field_name: str, value: str) -> FilterExpression:
    return FilterExpression(
        filter=Filter(
            field_name=field_name,
            string_filter=Filter.StringFilter(
                value=value,
                match_type=Filter.StringFilter.MatchType.EXACT
            ),
        )
    )


def ga_filter_in(field_name: str, values: List[str]) -> FilterExpression:
    return FilterExpression(
        filter=Filter(
            field_name=field_name,
            in_list_filter=Filter.InListFilter(values=values),
        )
    )


def ga_filter_and(exprs: List[FilterExpression]) -> FilterExpression:
    return FilterExpression(and_group=FilterExpressionList(expressions=exprs))


def run_report(
    client: BetaAnalyticsDataClient,
    property_id: str,
    start_date: str,
    end_date: str,
    dimensions: List[str],
    metrics: List[str],
    dimension_filter: Optional[FilterExpression] = None,
    order_bys: Optional[List[OrderBy]] = None,
    limit: int = 10000,
) -> pd.DataFrame:
    req = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[Dimension(name=d) for d in dimensions],
        metrics=[Metric(name=m) for m in metrics],
        limit=limit,
    )
    if dimension_filter is not None:
        req.dimension_filter = dimension_filter
    if order_bys:
        req.order_bys = order_bys

    resp = client.run_report(req)
    rows = []
    for r in resp.rows:
        row = {}
        for i, d in enumerate(dimensions):
            row[d] = r.dimension_values[i].value
        for j, m in enumerate(metrics):
            row[m] = r.metric_values[j].value
        rows.append(row)
    return pd.DataFrame(rows)


# =============================================================================
# SVG charts (그대로 유지)
# =============================================================================
def combined_index_svg(
    xlabels: List[str],
    series: List[List[float]],
    colors: List[str],
    labels: List[str],
    width=820, height=240,
    pad_l=46, pad_r=16, pad_t=18, pad_b=46,
) -> str:
    n = len(xlabels)
    allv = [v for s in series for v in s]
    y_min, y_max = min(allv), max(allv)
    if y_max == y_min:
        y_max += 1
    span = y_max - y_min
    y_min2 = y_min - span * 0.08
    y_max2 = y_max + span * 0.10

    inner_w = width - pad_l - pad_r
    inner_h = height - pad_t - pad_b

    def xy(i, v):
        x = pad_l + inner_w * (i / (n - 1 if n > 1 else 1))
        y_norm = (v - y_min2) / (y_max2 - y_min2)
        y = pad_t + inner_h * (1 - y_norm)
        return x, y

    ticks = 5
    grid, ylabels_svg = [], []
    for t in range(ticks + 1):
        frac = t / ticks
        y = pad_t + inner_h * (1 - frac)
        val = y_min2 + (y_max2 - y_min2) * frac
        grid.append(f"<line x1='{pad_l}' y1='{y:.1f}' x2='{width-pad_r}' y2='{y:.1f}' stroke='#eef2ff' stroke-width='1'/>")
        ylabels_svg.append(f"<text x='{pad_l-8}' y='{y+3:.1f}' text-anchor='end' font-size='10' fill='#6b7280'>{val:.0f}</text>")

    xlabels_svg = []
    for i, lab in enumerate(xlabels):
        x = pad_l + inner_w * (i / (n - 1 if n > 1 else 1))
        xlabels_svg.append(f"<text x='{x:.1f}' y='{height-pad_b+18}' text-anchor='middle' font-size='10' fill='#6b7280'>{lab}</text>")

    axes = f"""
      <line x1='{pad_l}' y1='{pad_t}' x2='{pad_l}' y2='{height-pad_b}' stroke='#c7d2fe' stroke-width='1'/>
      <line x1='{pad_l}' y1='{height-pad_b}' x2='{width-pad_r}' y2='{height-pad_b}' stroke='#c7d2fe' stroke-width='1'/>
    """

    polys, dots = [], []
    for sidx, s in enumerate(series):
        pts = [xy(i, v) for i, v in enumerate(s)]
        poly = " ".join(f"{x:.1f},{y:.1f}" for x, y in pts)
        color = colors[sidx]
        polys.append(f"<polyline fill='none' stroke='{color}' stroke-width='2.6' points='{poly}'/>")
        dots.append("".join([f"<circle cx='{x:.1f}' cy='{y:.1f}' r='3.0' fill='{color}'/>" for x, y in pts]))

    legend_items = []
    lx, ly = pad_l, 8
    for i, lab in enumerate(labels):
        legend_items.append(
            f"<g transform='translate({lx + i*160},{ly})'>"
            f"<line x1='0' y1='8' x2='18' y2='8' stroke='{colors[i]}' stroke-width='3'/>"
            f"<text x='26' y='11' font-size='11' fill='#334155' style='font-weight:600'>{lab}</text>"
            f"</g>"
        )

    return f"""
    <svg width="100%" viewBox="0 0 {width} {height}" preserveAspectRatio="none" style="display:block;">
      {''.join(grid)}
      {axes}
      {''.join(polys)}
      {''.join(dots)}
      {''.join(ylabels_svg)}
      {''.join(xlabels_svg)}
      <text x='{pad_l}' y='{height-8}' font-size='10' fill='#94a3b8'>Index (D-7 = 100)</text>
      {''.join(legend_items)}
    </svg>
    """


def spark_svg(
    xlabels: List[str],
    ys: List[float],
    width=240, height=70,
    pad_l=36, pad_r=10, pad_t=10, pad_b=22,
    stroke="#0055a5",
) -> str:
    n = len(xlabels)
    y_min, y_max = min(ys), max(ys)
    if y_max == y_min:
        y_max += 1
    span = y_max - y_min
    y_min2 = y_min - span * 0.12
    y_max2 = y_max + span * 0.12

    inner_w = width - pad_l - pad_r
    inner_h = height - pad_t - pad_b

    def xy(i, v):
        x = pad_l + inner_w * (i / (n - 1 if n > 1 else 1))
        y_norm = (v - y_min2) / (y_max2 - y_min2)
        y = pad_t + inner_h * (1 - y_norm)
        return x, y

    pts = [xy(i, v) for i, v in enumerate(ys)]
    poly = " ".join(f"{x:.1f},{y:.1f}" for x, y in pts)

    grid = []
    for frac in [0.0, 0.5, 1.0]:
        y = pad_t + inner_h * (1 - frac)
        grid.append(f"<line x1='{pad_l}' y1='{y:.1f}' x2='{width-pad_r}' y2='{y:.1f}' stroke='#eef2fb' stroke-width='1'/>")

    axes = f"""
      <line x1='{pad_l}' y1='{pad_t}' x2='{pad_l}' y2='{height-pad_b}' stroke='#cbd5e1' stroke-width='1'/>
      <line x1='{pad_l}' y1='{height-pad_b}' x2='{width-pad_r}' y2='{height-pad_b}' stroke='#cbd5e1' stroke-width='1'/>
    """

    ylab = [
        (y_max, pad_t + 3),
        (y_min + (y_max - y_min) / 2, pad_t + inner_h / 2 + 3),
        (y_min, height - pad_b + 3),
    ]
    ylabels_svg = "".join(
        [f"<text x='{pad_l-7}' y='{yy:.1f}' text-anchor='end' font-size='9' fill='#6b7280'>{int(round(val))}</text>" for val, yy in ylab]
    )

    idxs = [0, n // 2, n - 1] if n >= 3 else list(range(n))
    xlabels_svg = []
    for i in idxs:
        x = pad_l + inner_w * (i / (n - 1 if n > 1 else 1))
        xlabels_svg.append(f"<text x='{x:.1f}' y='{height-5}' text-anchor='middle' font-size='9' fill='#6b7280'>{xlabels[i]}</text>")

    area = " ".join(f"{x:.1f},{y:.1f}" for x, y in pts)
    area_poly = f"{pad_l:.1f},{height-pad_b:.1f} {area} {width-pad_r:.1f},{height-pad_b:.1f}"
    dots = "".join([f"<circle cx='{x:.1f}' cy='{y:.1f}' r='2.8' fill='{stroke}'/>" for x, y in pts])

    return f"""
    <svg width="{width}" height="{height}" viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg" style="display:block;">
      {''.join(grid)}
      {axes}
      <polygon points="{area_poly}" fill="{stroke}" opacity="0.08"></polygon>
      <polyline fill="none" stroke="{stroke}" stroke-width="2.4" points="{poly}"/>
      {dots}
      {ylabels_svg}
      {''.join(xlabels_svg)}
    </svg>
    """


# =============================================================================
# Windows
# =============================================================================
@dataclass
class DailyWindow:
    run_date: dt.date
    yesterday: dt.date
    day_before: dt.date
    window_start: dt.date
    window_end: dt.date


def compute_window(run_date: Optional[dt.date] = None) -> DailyWindow:
    if run_date is None:
        run_date = dt.date.today()
    yesterday = run_date - dt.timedelta(days=1)
    day_before = run_date - dt.timedelta(days=2)
    window_end = yesterday
    window_start = window_end - dt.timedelta(days=6)
    return DailyWindow(run_date, yesterday, day_before, window_start, window_end)


# =============================================================================
# Reports
# =============================================================================
def get_overall_kpis(client: BetaAnalyticsDataClient, w: DailyWindow) -> Dict[str, Dict[str, float]]:
    mets = ["sessions", "transactions", "purchaseRevenue"]
    d1 = run_report(client, PROPERTY_ID, ymd(w.yesterday), ymd(w.yesterday), [], mets)
    d0 = run_report(client, PROPERTY_ID, ymd(w.day_before), ymd(w.day_before), [], mets)

    def row_to_dict(df):
        if df.empty:
            return {"sessions": 0.0, "transactions": 0.0, "purchaseRevenue": 0.0}
        r = df.iloc[0]
        return {m: float(r.get(m, 0) or 0) for m in mets}

    cur = row_to_dict(d1)
    prev = row_to_dict(d0)

    cur["cvr"] = (cur["transactions"] / cur["sessions"]) if cur["sessions"] else 0.0
    prev["cvr"] = (prev["transactions"] / prev["sessions"]) if prev["sessions"] else 0.0
    return {"current": cur, "prev": prev}


def get_event_users(client: BetaAnalyticsDataClient, w: DailyWindow, event_name: str) -> Dict[str, float]:
    filt = ga_filter_eq("eventName", event_name)
    d1 = run_report(client, PROPERTY_ID, ymd(w.yesterday), ymd(w.yesterday), [], ["totalUsers"], dimension_filter=filt)
    d0 = run_report(client, PROPERTY_ID, ymd(w.day_before), ymd(w.day_before), [], ["totalUsers"], dimension_filter=filt)
    cur = float(d1.iloc[0]["totalUsers"]) if (not d1.empty and "totalUsers" in d1.columns) else 0.0
    prev = float(d0.iloc[0]["totalUsers"]) if (not d0.empty and "totalUsers" in d0.columns) else 0.0
    return {"current": cur, "prev": prev}


def get_multi_event_users(client: BetaAnalyticsDataClient, w: DailyWindow, event_names: List[str]) -> Dict[str, float]:
    cur_total = 0.0
    prev_total = 0.0
    for ev in event_names:
        r = get_event_users(client, w, ev)
        cur_total += r["current"]
        prev_total += r["prev"]
    return {"current": cur_total, "prev": prev_total}


def get_channel_snapshot(client: BetaAnalyticsDataClient, w: DailyWindow) -> pd.DataFrame:
    dims = ["sessionDefaultChannelGroup"]
    mets = ["sessions", "transactions", "purchaseRevenue"]

    cur = run_report(client, PROPERTY_ID, ymd(w.yesterday), ymd(w.yesterday), dims, mets)
    prev = run_report(client, PROPERTY_ID, ymd(w.day_before), ymd(w.day_before), dims, mets)

    if cur.empty:
        cur = pd.DataFrame(columns=dims + mets)
    if prev.empty:
        prev = pd.DataFrame(columns=dims + mets)

    cur["bucket"] = cur["sessionDefaultChannelGroup"].apply(bucket_channel)
    prev["bucket"] = prev["sessionDefaultChannelGroup"].apply(bucket_channel)

    cur[mets] = cur[mets].apply(pd.to_numeric, errors="coerce").fillna(0.0)
    prev[mets] = prev[mets].apply(pd.to_numeric, errors="coerce").fillna(0.0)

    cur_agg = cur.groupby("bucket", as_index=False)[mets].sum()
    prev_agg = prev.groupby("bucket", as_index=False)[mets].sum()

    buckets = ["Organic", "Paid AD", "Owned", "Awareness", "SNS"]
    base = pd.DataFrame({"bucket": buckets})
    out = (
        base.merge(cur_agg, on="bucket", how="left")
            .merge(prev_agg, on="bucket", how="left", suffixes=("", "_prev"))
            .fillna(0.0)
    )

    out["rev_dod"] = out.apply(lambda r: pct_change(r["purchaseRevenue"], r["purchaseRevenue_prev"]), axis=1)

    tot_cur_rev = float(out["purchaseRevenue"].sum())
    tot_prev_rev = float(out["purchaseRevenue_prev"].sum())
    total_row = {
        "bucket": "Total",
        "sessions": float(out["sessions"].sum()),
        "transactions": float(out["transactions"].sum()),
        "purchaseRevenue": tot_cur_rev,
        "sessions_prev": float(out["sessions_prev"].sum()),
        "transactions_prev": float(out["transactions_prev"].sum()),
        "purchaseRevenue_prev": tot_prev_rev,
        "rev_dod": pct_change(tot_cur_rev, tot_prev_rev),
    }

    out = pd.concat([out, pd.DataFrame([total_row])], ignore_index=True)
    return out[["bucket", "sessions", "transactions", "purchaseRevenue", "rev_dod"]]


def get_paid_detail(client: BetaAnalyticsDataClient, w: DailyWindow) -> pd.DataFrame:
    dims = ["sessionDefaultChannelGroup"]
    mets = ["sessions", "purchaseRevenue"]
    filt = ga_filter_in("sessionDefaultChannelGroup", PAID_SUBGROUPS)

    cur = run_report(client, PROPERTY_ID, ymd(w.yesterday), ymd(w.yesterday), dims, mets, dimension_filter=filt)
    prev = run_report(client, PROPERTY_ID, ymd(w.day_before), ymd(w.day_before), dims, mets, dimension_filter=filt)

    if cur.empty:
        cur = pd.DataFrame(columns=dims + mets)
    if prev.empty:
        prev = pd.DataFrame(columns=dims + mets)

    cur = cur.rename(columns={"sessionDefaultChannelGroup": "sub_channel"})
    prev = prev.rename(columns={"sessionDefaultChannelGroup": "sub_channel"})

    cur[mets] = cur[mets].apply(pd.to_numeric, errors="coerce").fillna(0.0)
    prev[mets] = prev[mets].apply(pd.to_numeric, errors="coerce").fillna(0.0)

    out = (
        pd.DataFrame({"sub_channel": PAID_SUBGROUPS})
        .merge(cur, on="sub_channel", how="left")
        .merge(prev, on="sub_channel", how="left", suffixes=("", "_prev"))
        .fillna(0.0)
    )
    out["rev_dod"] = out.apply(lambda r: pct_change(r["purchaseRevenue"], r["purchaseRevenue_prev"]), axis=1)

    total_cur_rev = float(out["purchaseRevenue"].sum())
    total_prev_rev = float(out["purchaseRevenue_prev"].sum())
    total = pd.DataFrame([{
        "sub_channel": "Total",
        "sessions": float(out["sessions"].sum()),
        "purchaseRevenue": total_cur_rev,
        "rev_dod": pct_change(total_cur_rev, total_prev_rev),
    }])

    out2 = out[["sub_channel", "sessions", "purchaseRevenue", "rev_dod"]]
    return pd.concat([out2, total], ignore_index=True)


def get_paid_top3(client: BetaAnalyticsDataClient, w: DailyWindow) -> pd.DataFrame:
    dims = ["sessionSourceMedium"]
    mets = ["sessions", "purchaseRevenue"]
    filt = ga_filter_in("sessionDefaultChannelGroup", PAID_SUBGROUPS)
    order = [OrderBy(metric=OrderBy.MetricOrderBy(metric_name="purchaseRevenue"), desc=True)]

    df = run_report(client, PROPERTY_ID, ymd(w.yesterday), ymd(w.yesterday), dims, mets, dimension_filter=filt, order_bys=order, limit=3)
    if df.empty:
        return pd.DataFrame(columns=["sessionSourceMedium", "sessions", "purchaseRevenue"])

    df["sessions"] = pd.to_numeric(df["sessions"], errors="coerce").fillna(0.0)
    df["purchaseRevenue"] = pd.to_numeric(df["purchaseRevenue"], errors="coerce").fillna(0.0)

    total = pd.DataFrame([{
        "sessionSourceMedium": "Total",
        "sessions": float(df["sessions"].sum()),
        "purchaseRevenue": float(df["purchaseRevenue"].sum()),
    }])

    return pd.concat([df, total], ignore_index=True)


def get_kpi_snapshot_table(client: BetaAnalyticsDataClient, w: DailyWindow, overall: Dict[str, Dict[str, float]]) -> pd.DataFrame:
    signup = get_multi_event_users(client, w, ["signup_complete", "signup"])
    cur = overall["current"]
    prev = overall["prev"]

    rows = [
        ("Sessions", cur["sessions"], prev["sessions"], "int"),
        ("CVR", cur["cvr"], prev["cvr"], "pct"),
        ("Revenue", cur["purchaseRevenue"], prev["purchaseRevenue"], "krw"),
        ("Orders", cur["transactions"], prev["transactions"], "int"),
        ("Sign-up Users", signup["current"], signup["prev"], "int"),
    ]

    out = []
    for metric, c, p, kind in rows:
        dod = pct_change(c, p) if kind != "pct" else (c - p)
        if kind == "int":
            value_fmt, dod_fmt = fmt_int(c), fmt_pct(dod, 1)
        elif kind == "krw":
            value_fmt, dod_fmt = fmt_currency_krw(c), fmt_pct(dod, 1)
        else:
            value_fmt, dod_fmt = f"{c*100:.2f}%", fmt_pp(dod, 2)
        out.append({"metric": metric, "value_fmt": value_fmt, "dod": dod, "dod_fmt": dod_fmt})
    return pd.DataFrame(out)


def get_trend_view_svg(client: BetaAnalyticsDataClient, w: DailyWindow) -> str:
    df = run_report(client, PROPERTY_ID, ymd(w.window_start), ymd(w.window_end), ["date"], ["sessions", "transactions", "purchaseRevenue"])
    if df.empty:
        x = [(w.window_start + dt.timedelta(days=i)).strftime("%m/%d") for i in range(7)]
        return combined_index_svg(x, [[100]*7, [100]*7, [100]*7], ["#0055a5", "#16a34a", "#c2410c"], ["Sessions", "Revenue", "CVR"])

    df["date"] = pd.to_datetime(df["date"], format="%Y%m%d", errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date")
    for c in ["sessions", "transactions", "purchaseRevenue"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    df["cvr"] = df.apply(lambda r: (r["transactions"] / r["sessions"]) if r["sessions"] else 0.0, axis=1)
    x = df["date"].dt.strftime("%m/%d").tolist()
    s = index_series(df["sessions"].tolist())
    r = index_series(df["purchaseRevenue"].tolist())
    c = index_series(df["cvr"].tolist())
    return combined_index_svg(x, [s, r, c], ["#0055a5", "#16a34a", "#c2410c"], ["Sessions", "Revenue", "CVR"])


def get_best_sellers_with_trends(client: BetaAnalyticsDataClient, w: DailyWindow) -> pd.DataFrame:
    order = [OrderBy(metric=OrderBy.MetricOrderBy(metric_name="itemsPurchased"), desc=True)]
    top = run_report(
        client, PROPERTY_ID,
        ymd(w.yesterday), ymd(w.yesterday),
        ["itemId", "itemName"],
        ["itemsPurchased"],
        order_bys=order, limit=5
    )

    if top.empty:
        return pd.DataFrame(columns=["itemId", "itemName", "itemsPurchased_yesterday", "trend_svg"])

    top["itemsPurchased"] = pd.to_numeric(top["itemsPurchased"], errors="coerce").fillna(0.0)
    top = top.rename(columns={"itemsPurchased": "itemsPurchased_yesterday"})
    skus = [str(x).strip() for x in top["itemId"].tolist() if str(x).strip()]

    ts = run_report(
        client, PROPERTY_ID,
        ymd(w.window_start), ymd(w.window_end),
        ["date", "itemId"],
        ["itemsPurchased"],
        dimension_filter=ga_filter_in("itemId", skus),
        limit=10000
    )

    axis_dates = [w.window_start + dt.timedelta(days=i) for i in range(7)]
    xlabels = [d.strftime("%m/%d") for d in axis_dates]

    top["trend_svg"] = ""

    if ts.empty:
        return top[["itemId", "itemName", "itemsPurchased_yesterday", "trend_svg"]]

    ts["date"] = ts["date"].apply(parse_yyyymmdd)
    ts["itemsPurchased"] = pd.to_numeric(ts["itemsPurchased"], errors="coerce").fillna(0.0)
    ts = ts.sort_values(["itemId", "date"])

    svgs = []
    for sku in skus:
        sub = ts[ts["itemId"] == sku].set_index("date")["itemsPurchased"]
        ys = [float(sub.get(d, 0.0)) for d in axis_dates]
        svgs.append(spark_svg(xlabels, ys, width=240, height=70, stroke="#0055a5"))

    # skus 순서와 top row 순서가 다를 수 있으니 map으로 합치기
    sku_to_svg = {sku: svgs[i] for i, sku in enumerate(skus)}
    top["trend_svg"] = top["itemId"].astype(str).str.strip().map(lambda s: sku_to_svg.get(s, ""))
    return top[["itemId", "itemName", "itemsPurchased_yesterday", "trend_svg"]]


def get_rising_products(client: BetaAnalyticsDataClient, w: DailyWindow, top_n: int = 5) -> pd.DataFrame:
    d1 = run_report(client, PROPERTY_ID, ymd(w.yesterday), ymd(w.yesterday), ["itemId", "itemName"], ["itemsPurchased"], limit=10000)
    d0 = run_report(client, PROPERTY_ID, ymd(w.day_before), ymd(w.day_before), ["itemId"], ["itemsPurchased"], limit=10000)

    if d1.empty:
        return pd.DataFrame(columns=["itemId", "itemName", "itemViews_yesterday", "delta"])

    d1["itemsPurchased"] = pd.to_numeric(d1["itemsPurchased"], errors="coerce").fillna(0.0)
    if not d0.empty:
        d0["itemsPurchased"] = pd.to_numeric(d0["itemsPurchased"], errors="coerce").fillna(0.0)
    else:
        d0 = pd.DataFrame(columns=["itemId", "itemsPurchased"])

    m = d1.merge(d0, on="itemId", how="left", suffixes=("_y", "_d0")).fillna(0.0)
    m["delta"] = m["itemsPurchased_y"] - m["itemsPurchased_d0"]
    m = m.sort_values("delta", ascending=False).head(top_n)

    # Views best-effort
    skus = [str(x).strip() for x in m["itemId"].tolist() if str(x).strip()]
    views_df = pd.DataFrame(columns=["itemId", "itemViews_yesterday"])
    if skus:
        for metric_name, use_event_filter in [
            ("itemViewEvents", False),
            ("itemsViewed", False),
            ("eventCount", True),
        ]:
            try:
                v = run_report(
                    client,
                    PROPERTY_ID,
                    ymd(w.yesterday),
                    ymd(w.yesterday),
                    ["itemId"],
                    [metric_name],
                    dimension_filter=(
                        ga_filter_and([ga_filter_in("itemId", skus), ga_filter_eq("eventName", "view_item")])
                        if use_event_filter else ga_filter_in("itemId", skus)
                    ),
                    limit=10000,
                )
                if not v.empty:
                    v[metric_name] = pd.to_numeric(v[metric_name], errors="coerce").fillna(0.0)
                    views_df = v[["itemId", metric_name]].rename(columns={metric_name: "itemViews_yesterday"})
                break
            except Exception:
                continue

    m = m.merge(views_df, on="itemId", how="left")
    m["itemViews_y_]()

def get_rising_products(client: BetaAnalyticsDataClient, w: DailyWindow, top_n: int = 5) -> pd.DataFrame:
    d1 = run_report(
        client, PROPERTY_ID,
        ymd(w.yesterday), ymd(w.yesterday),
        ["itemId", "itemName"],
        ["itemsPurchased"],
        limit=10000
    )
    d0 = run_report(
        client, PROPERTY_ID,
        ymd(w.day_before), ymd(w.day_before),
        ["itemId"],
        ["itemsPurchased"],
        limit=10000
    )

    if d1.empty:
        return pd.DataFrame(columns=["itemId", "itemName", "itemViews_yesterday", "delta"])

    d1["itemsPurchased"] = pd.to_numeric(d1["itemsPurchased"], errors="coerce").fillna(0.0)
    if not d0.empty:
        d0["itemsPurchased"] = pd.to_numeric(d0["itemsPurchased"], errors="coerce").fillna(0.0)
    else:
        d0 = pd.DataFrame(columns=["itemId", "itemsPurchased"])

    m = d1.merge(d0, on="itemId", how="left", suffixes=("_y", "_d0")).fillna(0.0)
    m["delta"] = m["itemsPurchased_y"] - m["itemsPurchased_d0"]
    m = m.sort_values("delta", ascending=False).head(top_n)

    # Views best-effort (GA4 호환성 문제 때문에 fallback까지 포함)
    skus = [str(x).strip() for x in m["itemId"].tolist() if str(x).strip()]
    views_df = pd.DataFrame(columns=["itemId", "itemViews_yesterday"])

    if skus:
        for metric_name, use_event_filter in [
            ("itemViewEvents", False),
            ("itemsViewed", False),
            ("eventCount", True),  # view_item eventCount로 fallback
        ]:
            try:
                v = run_report(
                    client,
                    PROPERTY_ID,
                    ymd(w.yesterday),
                    ymd(w.yesterday),
                    ["itemId"],
                    [metric_name],
                    dimension_filter=(
                        ga_filter_and([ga_filter_in("itemId", skus), ga_filter_eq("eventName", "view_item")])
                        if use_event_filter else ga_filter_in("itemId", skus)
                    ),
                    limit=10000,
                )
                if not v.empty:
                    v[metric_name] = pd.to_numeric(v[metric_name], errors="coerce").fillna(0.0)
                    views_df = v[["itemId", metric_name]].rename(columns={metric_name: "itemViews_yesterday"})
                break
            except Exception:
                continue

    m = m.merge(views_df, on="itemId", how="left")
    m["itemViews_yesterday"] = pd.to_numeric(m.get("itemViews_yesterday"), errors="coerce").fillna(0.0)

    # 표기 통일
    return m[["itemId", "itemName", "itemViews_yesterday", "delta"]]


def get_search_trends(client: BetaAnalyticsDataClient, w: DailyWindow) -> Dict[str, pd.DataFrame]:
    """Search Trend: 신규 진입 Top3, 급상승 Top3 (D-1 vs 직전 7일 평균)"""
    lookback_start = w.window_end - dt.timedelta(days=13)
    df = run_report(
        client, PROPERTY_ID,
        ymd(lookback_start), ymd(w.window_end),
        ["date", "searchTerm"],
        ["eventCount"],
        dimension_filter=ga_filter_eq("eventName", SEARCH_EVENT),
        limit=10000
    )
    if df.empty:
        return {
            "new": pd.DataFrame(columns=["searchTerm"]),
            "rising": pd.DataFrame(columns=["searchTerm", "pct"])
        }

    df["date"] = df["date"].apply(parse_yyyymmdd)
    df["eventCount"] = pd.to_numeric(df["eventCount"], errors="coerce").fillna(0.0)

    # D-1
    y_df = (
        df[df["date"] == w.window_end]
        .groupby("searchTerm", as_index=False)["eventCount"].sum()
        .sort_values("eventCount", ascending=False)
    )

    # 직전 7일 평균 (D-8 ~ D-2)
    prior_start = w.window_end - dt.timedelta(days=7)
    prior_df = df[(df["date"] >= prior_start) & (df["date"] <= (w.window_end - dt.timedelta(days=1)))]
    prior_agg = (
        prior_df.groupby("searchTerm", as_index=False)["eventCount"].mean()
        .rename(columns={"eventCount": "prior_avg"})
    )

    merged = y_df.merge(prior_agg, on="searchTerm", how="left").fillna(0.0)

    new_terms = merged[merged["prior_avg"] == 0].head(3)[["searchTerm"]].copy()

    rising = merged[merged["prior_avg"] > 0].copy()
    rising["pct"] = (rising["eventCount"] - rising["prior_avg"]) / rising["prior_avg"] * 100.0
    rising = rising.sort_values("pct", ascending=False).head(3)[["searchTerm", "pct"]]

    return {"new": new_terms, "rising": rising}


# =============================================================================
# main: CSV 매핑 → 이미지 붙이기 → HTML 저장 → git push → Outlook 발송
# =============================================================================
def main():
    if not PROPERTY_ID:
        raise SystemExit("ERROR: GA4_PROPERTY_ID is empty. Set env var GA4_PROPERTY_ID and retry.")

    # ---------- GA4 creds ----------
    _scopes = [
        "https://www.googleapis.com/auth/analytics.readonly",
        "https://www.googleapis.com/auth/cloud-platform",
    ]
    _creds, _proj = google_auth_default(scopes=_scopes)
    client = BetaAnalyticsDataClient(credentials=_creds)

    # ---------- window ----------
    w = compute_window()

    # ---------- assets ----------
    logo_b64 = load_logo_base64(LOGO_PATH)

    # ---------- image map (CSV in repo) ----------
    csv_path = _abs_in_repo(PRODUCT_IMAGE_CSV)
    image_map = load_product_image_map(csv_path)
    safe_print(f"[OK] Loaded image map: {len(image_map):,} rows from {csv_path}")

    # ---------- core reports ----------
    overall = get_overall_kpis(client, w)
    signup_users = get_multi_event_users(client, w, ["signup_complete", "signup"])
    channel_snapshot = get_channel_snapshot(client, w)
    paid_detail = get_paid_detail(client, w)
    paid_top3 = get_paid_top3(client, w)
    kpi_snapshot = get_kpi_snapshot_table(client, w, overall)
    trend_svg = get_trend_view_svg(client, w)

    best_sellers = get_best_sellers_with_trends(client, w)
    best_sellers = attach_image_urls(best_sellers, image_map, sku_col="itemId")  # ✅ 이미지 붙이기

    rising = get_rising_products(client, w, top_n=5)
    rising = attach_image_urls(rising, image_map, sku_col="itemId")  # ✅ 이미지 붙이기

    # Search Trend
    search = get_search_trends(client, w)

    # ---------- missing sku export ----------
    missing = []
    if not best_sellers.empty and "itemId" in best_sellers.columns:
        missing += [sku for sku in best_sellers["itemId"].tolist() if str(sku).strip() not in image_map]
    if not rising.empty and "itemId" in rising.columns:
        missing += [sku for sku in rising["itemId"].tolist() if str(sku).strip() not in image_map]

    if missing:
        miss_out = _abs_in_repo(MISSING_SKU_OUT)
        write_missing_image_skus(miss_out, missing)

    # ---------- render html ----------
    # ⚠️ render_html 함수는 네가 위에서 이미 가지고 있는 “큰 템플릿” 그대로 사용하면 됨.
    #     (아래 render_html 호출 파라미터는 네 템플릿 시그니처에 맞춰 유지)
    html = render_html(
        logo_b64=logo_b64,
        w=w,
        overall=overall,
        signup_users=signup_users,
        channel_snapshot=channel_snapshot,
        paid_detail=paid_detail,
        paid_top3=paid_top3,
        kpi_snapshot=kpi_snapshot,
        trend_svg=trend_svg,
        best_sellers=best_sellers,
        rising=rising,
        category_pdp_trend=pd.DataFrame(),   # 네 템플릿에서 필요 없으면 제거/미사용 처리
        search_new=search["new"],
        search_rising=search["rising"],
    )

    # ---------- write HTML into repo ----------
    out_html_path = _abs_in_repo(OUTPUT_HTML)
    write_text(out_html_path, html)
    safe_print(f"[OK] Wrote HTML: {out_html_path}")
    safe_print(f"     Window: {ymd(w.window_start)} ~ {ymd(w.window_end)} (rolling 7d)")

    # ---------- git push ----------
    commit_msg = f"auto: daily digest {ymd(w.window_end)}"
    git_push(REPO_PATH, commit_msg)

    # ---------- send outlook ----------
    send_via_outlook(
        subject=f"{MAIL_SUBJECT} ({ymd(w.window_end)})",
        html_body=html,                 # ✅ 본문에 HTML 그대로
        to_list=WEEKLY_RECIPIENTS,
        cc_list=None
    )


if __name__ == "__main__":
    main()
# -*- coding: utf-8 -*-
"""
Columbia Daily Digest — Live GA4 Data (Google Analytics Data API)
+ 상품코드별 이미지 CSV(SKU->URL) 매핑
+ GitHub 자동 push
+ Outlook으로 HTML 본문 그대로 자동 발송

사전 준비
- pip install google-analytics-data pandas python-dateutil pywin32 google-cloud-bigquery
- REPO_PATH(깃 폴더) 안에 ① 이 py ② 상품코드별 이미지.csv 가 같이 있도록(또는 env로 경로 지정)
- Outlook 데스크톱 앱 로그인 상태

ENV (권장)
- GA4_PROPERTY_ID
- REPORT_REPO_PATH   (예: C:\report_repo)
- PRODUCT_IMAGE_CSV  (예: 상품코드별 이미지.csv)
- OUTPUT_HTML        (예: daily_digest_live.html)
- MAIL_SUBJECT       (예: E-comm Daily GA4 Report)
"""

import os
import re
import sys
import csv
import base64
import subprocess
import datetime as dt
from dataclasses import dataclass
from typing import Dict, List, Optional

import pandas as pd

try:
    import win32com.client as win32  # pip install pywin32
except Exception:
    win32 = None

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.auth import default as google_auth_default
from google.analytics.data_v1beta.types import (
    DateRange, Dimension, Metric, RunReportRequest,
    OrderBy, FilterExpression, Filter, FilterExpressionList
)

# Optional: BigQuery backend
try:
    from google.cloud import bigquery  # type: ignore
except Exception:
    bigquery = None


# =============================================================================
# Config
# =============================================================================
GA4_PROPERTY_ID = os.getenv("GA4_PROPERTY_ID", "").strip()
PROPERTY_ID = GA4_PROPERTY_ID

WEEKLY_RECIPIENTS = [
    "Juwon.Lee@columbia.com",
    "hugh.kang@columbia.com",
    "hmkim@columbia.com",
    "seonyoung.jang@columbia.com",
    "dahae.kim@columbia.com",
]

REPO_PATH = os.getenv("REPORT_REPO_PATH", r"C:\report_repo").strip()
PRODUCT_IMAGE_CSV = os.getenv("PRODUCT_IMAGE_CSV", "상품코드별 이미지.csv").strip()
OUTPUT_HTML = os.getenv("OUTPUT_HTML", "daily_digest_live.html").strip()
MAIL_SUBJECT = os.getenv("MAIL_SUBJECT", "E-comm Daily GA4 Report").strip()

LOGO_PATH = os.getenv("DAILY_DIGEST_LOGO_PATH", "pngwing.com.png")
MISSING_SKU_OUT = os.getenv("DAILY_DIGEST_MISSING_SKU_OUT", "missing_image_skus.csv")

PLACEHOLDER_IMG = os.getenv("DAILY_DIGEST_PLACEHOLDER_IMG", "")
IMG_MAX_WIDTH_PX = int(os.getenv("IMG_MAX_WIDTH_PX", "220"))

BQ_EVENTS_TABLE = os.getenv("DAILY_DIGEST_BQ_EVENTS_TABLE", "columbia-ga4.analytics_358593394.events_*").strip()
BQ_LOCATION = os.getenv("DAILY_DIGEST_BQ_LOCATION", "asia-northeast3").strip()

SIGNUP_EVENT = os.getenv("DAILY_DIGEST_SIGNUP_EVENT", "sign_up")
LOGIN_EVENT = os.getenv("DAILY_DIGEST_LOGIN_EVENT", "login")
SEARCH_EVENT = os.getenv("DAILY_DIGEST_SEARCH_EVENT", "view_search_results")

CHANNEL_BUCKETS = {
    "Organic": {"Organic Search"},
    "Paid AD": {"Paid Search", "Paid Social", "Display"},
    "Owned": {"Email", "SMS", "Mobile Push Notifications", "Direct"},
    "Awareness": {"Referral", "Video", "Organic Video", "Affiliates", "Cross-network"},
    "SNS": {"Organic Social"},
}
PAID_SUBGROUPS = ["Paid Search", "Paid Social", "Display"]


# =============================================================================
# Path helpers
# =============================================================================
def _abs_in_repo(filename: str) -> str:
    """If filename is relative, resolve inside REPO_PATH; else use as-is."""
    if os.path.isabs(filename):
        return filename
    return os.path.abspath(os.path.join(REPO_PATH, filename))


def read_text(path: str) -> str:
    with open(path, "r", encoding="utf-8") as f:
        return f.read()


def write_text(path: str, s: str) -> None:
    with open(path, "w", encoding="utf-8") as f:
        f.write(s)


def safe_print(*args):
    print(*args, flush=True)


# =============================================================================
# Image map (CSV)  — SKU -> URL
# =============================================================================
def load_product_image_map(csv_path: str) -> Dict[str, str]:
    """
    CSV 최소 2컬럼 필요:
    - 상품코드 (또는 product_code, code, sku, itemId, item_id 등)
    - 이미지링크 (또는 image_url, url 등)
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"상품코드별 이미지 CSV를 찾을 수 없음: {csv_path}")

    candidates_code = {"상품코드", "product_code", "code", "sku", "pcode", "itemId", "item_id", "itemid"}
    candidates_url = {"이미지", "이미지링크", "image", "image_url", "url", "img_url", "link"}

    mapping: Dict[str, str] = {}

    with open(csv_path, "r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        if not reader.fieldnames:
            raise ValueError("CSV 헤더가 비어있음")

        code_col = None
        url_col = None
        for h in reader.fieldnames:
            hn = (h or "").strip()
            if hn in candidates_code and code_col is None:
                code_col = h
            if hn in candidates_url and url_col is None:
                url_col = h

        # 최후 fallback: 첫 2컬럼
        if code_col is None or url_col is None:
            headers = [h for h in reader.fieldnames if h]
            if len(headers) >= 2:
                code_col = code_col or headers[0]
                url_col = url_col or headers[1]

        if code_col is None or url_col is None:
            raise ValueError(f"CSV 컬럼 식별 실패. fieldnames={reader.fieldnames}")

        for row in reader:
            code = (row.get(code_col) or "").strip()
            url = (row.get(url_col) or "").strip()
            if not code or not url:
                continue
            if url.lower().startswith("http"):
                mapping[code] = url

    return mapping


def attach_image_urls(df: pd.DataFrame, image_map: Dict[str, str], sku_col: str = "itemId") -> pd.DataFrame:
    """Attach `image_url` column using SKU->URL map."""
    if df is None or df.empty:
        return pd.DataFrame(df, copy=True)
    out = df.copy()
    if sku_col not in out.columns:
        return out
    out["image_url"] = out[sku_col].astype(str).str.strip().map(lambda x: image_map.get(x, ""))
    return out


def write_missing_image_skus(path: str, skus: List[str]) -> None:
    if not path or not skus:
        return
    try:
        out = pd.DataFrame({"sku": sorted(set([str(s).strip() for s in skus if str(s).strip()]))})
        out.to_csv(path, index=False, encoding="utf-8-sig")
        safe_print(f"[INFO] Wrote missing image SKUs: {path}")
    except Exception as e:
        safe_print(f"[WARN] Could not write missing SKUs: {type(e).__name__}: {e}")


# =============================================================================
# Git + Outlook
# =============================================================================
def git_push(repo_path: str, commit_msg: str) -> None:
    if not os.path.isdir(repo_path):
        raise FileNotFoundError(f"REPO_PATH 폴더 없음: {repo_path}")

    def run(cmd: str):
        r = subprocess.run(cmd, shell=True, cwd=repo_path, capture_output=True, text=True)
        if r.returncode != 0:
            raise RuntimeError(f"Git 명령 실패: {cmd}\nSTDOUT:\n{r.stdout}\nSTDERR:\n{r.stderr}")
        return r.stdout.strip()

    run("git add .")
    status = run("git status --porcelain")
    if not status:
        safe_print("[git] 변경사항 없음 - push 생략")
        return

    run(f'git commit -m "{commit_msg}"')
    run("git push")
    safe_print("[git] push 완료")


def send_via_outlook(subject: str, html_body: str, to_list: List[str], cc_list: Optional[List[str]] = None):
    if win32 is None:
        raise RuntimeError("pywin32가 필요합니다. pip install pywin32")

    outlook = win32.Dispatch("Outlook.Application")
    mail = outlook.CreateItem(0)

    mail.To = ";".join([x for x in to_list if x])
    if cc_list:
        mail.CC = ";".join([x for x in cc_list if x])

    mail.Subject = subject
    mail.HTMLBody = html_body
    mail.Send()
    safe_print("[mail] Outlook 발송 완료")


# =============================================================================
# GA4 helpers
# =============================================================================
def load_logo_base64(path: str) -> str:
    if not path:
        return ""
    if not os.path.exists(path):
        alt = os.path.join(os.path.dirname(os.path.abspath(__file__)), path)
        if os.path.exists(alt):
            path = alt
        else:
            return ""
    with open(path, "rb") as f:
        return base64.b64encode(f.read()).decode("utf-8")


def fmt_int(n) -> str:
    try:
        return f"{int(round(float(n))):,}"
    except Exception:
        return "0"


def fmt_currency_krw(n) -> str:
    try:
        return f"₩{int(round(float(n))):,}"
    except Exception:
        return "₩0"


def fmt_pct(p, digits=1) -> str:
    try:
        return f"{p*100:.{digits}f}%"
    except Exception:
        return "0.0%"


def fmt_pp(p, digits=2) -> str:
    try:
        return f"{p*100:.{digits}f}%p"
    except Exception:
        return "0.00%p"


def pct_change(curr: float, prev: float) -> float:
    if prev == 0:
        return 0.0 if curr == 0 else 1.0
    return (curr - prev) / prev


def ymd(d: dt.date) -> str:
    return d.strftime("%Y-%m-%d")


def parse_yyyymmdd(s: str) -> dt.date:
    return dt.datetime.strptime(s, "%Y%m%d").date()


def bucket_channel(ch: str) -> str:
    for bucket, members in CHANNEL_BUCKETS.items():
        if ch in members:
            return bucket
    return "Awareness"


def index_series(vals: List[float]) -> List[float]:
    base = vals[0] if vals and vals[0] else 1.0
    return [v / base * 100.0 for v in vals]


def ga_filter_eq(field_name: str, value: str) -> FilterExpression:
    return FilterExpression(
        filter=Filter(
            field_name=field_name,
            string_filter=Filter.StringFilter(
                value=value,
                match_type=Filter.StringFilter.MatchType.EXACT
            ),
        )
    )


def ga_filter_in(field_name: str, values: List[str]) -> FilterExpression:
    return FilterExpression(
        filter=Filter(
            field_name=field_name,
            in_list_filter=Filter.InListFilter(values=values),
        )
    )


def ga_filter_and(exprs: List[FilterExpression]) -> FilterExpression:
    return FilterExpression(and_group=FilterExpressionList(expressions=exprs))


def run_report(
    client: BetaAnalyticsDataClient,
    property_id: str,
    start_date: str,
    end_date: str,
    dimensions: List[str],
    metrics: List[str],
    dimension_filter: Optional[FilterExpression] = None,
    order_bys: Optional[List[OrderBy]] = None,
    limit: int = 10000,
) -> pd.DataFrame:
    req = RunReportRequest(
        property=f"properties/{property_id}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[Dimension(name=d) for d in dimensions],
        metrics=[Metric(name=m) for m in metrics],
        limit=limit,
    )
    if dimension_filter is not None:
        req.dimension_filter = dimension_filter
    if order_bys:
        req.order_bys = order_bys

    resp = client.run_report(req)
    rows = []
    for r in resp.rows:
        row = {}
        for i, d in enumerate(dimensions):
            row[d] = r.dimension_values[i].value
        for j, m in enumerate(metrics):
            row[m] = r.metric_values[j].value
        rows.append(row)
    return pd.DataFrame(rows)


# =============================================================================
# SVG charts (그대로 유지)
# =============================================================================
def combined_index_svg(
    xlabels: List[str],
    series: List[List[float]],
    colors: List[str],
    labels: List[str],
    width=820, height=240,
    pad_l=46, pad_r=16, pad_t=18, pad_b=46,
) -> str:
    n = len(xlabels)
    allv = [v for s in series for v in s]
    y_min, y_max = min(allv), max(allv)
    if y_max == y_min:
        y_max += 1
    span = y_max - y_min
    y_min2 = y_min - span * 0.08
    y_max2 = y_max + span * 0.10

    inner_w = width - pad_l - pad_r
    inner_h = height - pad_t - pad_b

    def xy(i, v):
        x = pad_l + inner_w * (i / (n - 1 if n > 1 else 1))
        y_norm = (v - y_min2) / (y_max2 - y_min2)
        y = pad_t + inner_h * (1 - y_norm)
        return x, y

    ticks = 5
    grid, ylabels_svg = [], []
    for t in range(ticks + 1):
        frac = t / ticks
        y = pad_t + inner_h * (1 - frac)
        val = y_min2 + (y_max2 - y_min2) * frac
        grid.append(f"<line x1='{pad_l}' y1='{y:.1f}' x2='{width-pad_r}' y2='{y:.1f}' stroke='#eef2ff' stroke-width='1'/>")
        ylabels_svg.append(f"<text x='{pad_l-8}' y='{y+3:.1f}' text-anchor='end' font-size='10' fill='#6b7280'>{val:.0f}</text>")

    xlabels_svg = []
    for i, lab in enumerate(xlabels):
        x = pad_l + inner_w * (i / (n - 1 if n > 1 else 1))
        xlabels_svg.append(f"<text x='{x:.1f}' y='{height-pad_b+18}' text-anchor='middle' font-size='10' fill='#6b7280'>{lab}</text>")

    axes = f"""
      <line x1='{pad_l}' y1='{pad_t}' x2='{pad_l}' y2='{height-pad_b}' stroke='#c7d2fe' stroke-width='1'/>
      <line x1='{pad_l}' y1='{height-pad_b}' x2='{width-pad_r}' y2='{height-pad_b}' stroke='#c7d2fe' stroke-width='1'/>
    """

    polys, dots = [], []
    for sidx, s in enumerate(series):
        pts = [xy(i, v) for i, v in enumerate(s)]
        poly = " ".join(f"{x:.1f},{y:.1f}" for x, y in pts)
        color = colors[sidx]
        polys.append(f"<polyline fill='none' stroke='{color}' stroke-width='2.6' points='{poly}'/>")
        dots.append("".join([f"<circle cx='{x:.1f}' cy='{y:.1f}' r='3.0' fill='{color}'/>" for x, y in pts]))

    legend_items = []
    lx, ly = pad_l, 8
    for i, lab in enumerate(labels):
        legend_items.append(
            f"<g transform='translate({lx + i*160},{ly})'>"
            f"<line x1='0' y1='8' x2='18' y2='8' stroke='{colors[i]}' stroke-width='3'/>"
            f"<text x='26' y='11' font-size='11' fill='#334155' style='font-weight:600'>{lab}</text>"
            f"</g>"
        )

    return f"""
    <svg width="100%" viewBox="0 0 {width} {height}" preserveAspectRatio="none" style="display:block;">
      {''.join(grid)}
      {axes}
      {''.join(polys)}
      {''.join(dots)}
      {''.join(ylabels_svg)}
      {''.join(xlabels_svg)}
      <text x='{pad_l}' y='{height-8}' font-size='10' fill='#94a3b8'>Index (D-7 = 100)</text>
      {''.join(legend_items)}
    </svg>
    """


def spark_svg(
    xlabels: List[str],
    ys: List[float],
    width=240, height=70,
    pad_l=36, pad_r=10, pad_t=10, pad_b=22,
    stroke="#0055a5",
) -> str:
    n = len(xlabels)
    y_min, y_max = min(ys), max(ys)
    if y_max == y_min:
        y_max += 1
    span = y_max - y_min
    y_min2 = y_min - span * 0.12
    y_max2 = y_max + span * 0.12

    inner_w = width - pad_l - pad_r
    inner_h = height - pad_t - pad_b

    def xy(i, v):
        x = pad_l + inner_w * (i / (n - 1 if n > 1 else 1))
        y_norm = (v - y_min2) / (y_max2 - y_min2)
        y = pad_t + inner_h * (1 - y_norm)
        return x, y

    pts = [xy(i, v) for i, v in enumerate(ys)]
    poly = " ".join(f"{x:.1f},{y:.1f}" for x, y in pts)

    grid = []
    for frac in [0.0, 0.5, 1.0]:
        y = pad_t + inner_h * (1 - frac)
        grid.append(f"<line x1='{pad_l}' y1='{y:.1f}' x2='{width-pad_r}' y2='{y:.1f}' stroke='#eef2fb' stroke-width='1'/>")

    axes = f"""
      <line x1='{pad_l}' y1='{pad_t}' x2='{pad_l}' y2='{height-pad_b}' stroke='#cbd5e1' stroke-width='1'/>
      <line x1='{pad_l}' y1='{height-pad_b}' x2='{width-pad_r}' y2='{height-pad_b}' stroke='#cbd5e1' stroke-width='1'/>
    """

    ylab = [
        (y_max, pad_t + 3),
        (y_min + (y_max - y_min) / 2, pad_t + inner_h / 2 + 3),
        (y_min, height - pad_b + 3),
    ]
    ylabels_svg = "".join(
        [f"<text x='{pad_l-7}' y='{yy:.1f}' text-anchor='end' font-size='9' fill='#6b7280'>{int(round(val))}</text>" for val, yy in ylab]
    )

    idxs = [0, n // 2, n - 1] if n >= 3 else list(range(n))
    xlabels_svg = []
    for i in idxs:
        x = pad_l + inner_w * (i / (n - 1 if n > 1 else 1))
        xlabels_svg.append(f"<text x='{x:.1f}' y='{height-5}' text-anchor='middle' font-size='9' fill='#6b7280'>{xlabels[i]}</text>")

    area = " ".join(f"{x:.1f},{y:.1f}" for x, y in pts)
    area_poly = f"{pad_l:.1f},{height-pad_b:.1f} {area} {width-pad_r:.1f},{height-pad_b:.1f}"
    dots = "".join([f"<circle cx='{x:.1f}' cy='{y:.1f}' r='2.8' fill='{stroke}'/>" for x, y in pts])

    return f"""
    <svg width="{width}" height="{height}" viewBox="0 0 {width} {height}" xmlns="http://www.w3.org/2000/svg" style="display:block;">
      {''.join(grid)}
      {axes}
      <polygon points="{area_poly}" fill="{stroke}" opacity="0.08"></polygon>
      <polyline fill="none" stroke="{stroke}" stroke-width="2.4" points="{poly}"/>
      {dots}
      {ylabels_svg}
      {''.join(xlabels_svg)}
    </svg>
    """


# =============================================================================
# Windows
# =============================================================================
@dataclass
class DailyWindow:
    run_date: dt.date
    yesterday: dt.date
    day_before: dt.date
    window_start: dt.date
    window_end: dt.date


def compute_window(run_date: Optional[dt.date] = None) -> DailyWindow:
    if run_date is None:
        run_date = dt.date.today()
    yesterday = run_date - dt.timedelta(days=1)
    day_before = run_date - dt.timedelta(days=2)
    window_end = yesterday
    window_start = window_end - dt.timedelta(days=6)
    return DailyWindow(run_date, yesterday, day_before, window_start, window_end)


# =============================================================================
# Reports
# =============================================================================
def get_overall_kpis(client: BetaAnalyticsDataClient, w: DailyWindow) -> Dict[str, Dict[str, float]]:
    mets = ["sessions", "transactions", "purchaseRevenue"]
    d1 = run_report(client, PROPERTY_ID, ymd(w.yesterday), ymd(w.yesterday), [], mets)
    d0 = run_report(client, PROPERTY_ID, ymd(w.day_before), ymd(w.day_before), [], mets)

    def row_to_dict(df):
        if df.empty:
            return {"sessions": 0.0, "transactions": 0.0, "purchaseRevenue": 0.0}
        r = df.iloc[0]
        return {m: float(r.get(m, 0) or 0) for m in mets}

    cur = row_to_dict(d1)
    prev = row_to_dict(d0)

    cur["cvr"] = (cur["transactions"] / cur["sessions"]) if cur["sessions"] else 0.0
    prev["cvr"] = (prev["transactions"] / prev["sessions"]) if prev["sessions"] else 0.0
    return {"current": cur, "prev": prev}


def get_event_users(client: BetaAnalyticsDataClient, w: DailyWindow, event_name: str) -> Dict[str, float]:
    filt = ga_filter_eq("eventName", event_name)
    d1 = run_report(client, PROPERTY_ID, ymd(w.yesterday), ymd(w.yesterday), [], ["totalUsers"], dimension_filter=filt)
    d0 = run_report(client, PROPERTY_ID, ymd(w.day_before), ymd(w.day_before), [], ["totalUsers"], dimension_filter=filt)
    cur = float(d1.iloc[0]["totalUsers"]) if (not d1.empty and "totalUsers" in d1.columns) else 0.0
    prev = float(d0.iloc[0]["totalUsers"]) if (not d0.empty and "totalUsers" in d0.columns) else 0.0
    return {"current": cur, "prev": prev}


def get_multi_event_users(client: BetaAnalyticsDataClient, w: DailyWindow, event_names: List[str]) -> Dict[str, float]:
    cur_total = 0.0
    prev_total = 0.0
    for ev in event_names:
        r = get_event_users(client, w, ev)
        cur_total += r["current"]
        prev_total += r["prev"]
    return {"current": cur_total, "prev": prev_total}


def get_channel_snapshot(client: BetaAnalyticsDataClient, w: DailyWindow) -> pd.DataFrame:
    dims = ["sessionDefaultChannelGroup"]
    mets = ["sessions", "transactions", "purchaseRevenue"]

    cur = run_report(client, PROPERTY_ID, ymd(w.yesterday), ymd(w.yesterday), dims, mets)
    prev = run_report(client, PROPERTY_ID, ymd(w.day_before), ymd(w.day_before), dims, mets)

    if cur.empty:
        cur = pd.DataFrame(columns=dims + mets)
    if prev.empty:
        prev = pd.DataFrame(columns=dims + mets)

    cur["bucket"] = cur["sessionDefaultChannelGroup"].apply(bucket_channel)
    prev["bucket"] = prev["sessionDefaultChannelGroup"].apply(bucket_channel)

    cur[mets] = cur[mets].apply(pd.to_numeric, errors="coerce").fillna(0.0)
    prev[mets] = prev[mets].apply(pd.to_numeric, errors="coerce").fillna(0.0)

    cur_agg = cur.groupby("bucket", as_index=False)[mets].sum()
    prev_agg = prev.groupby("bucket", as_index=False)[mets].sum()

    buckets = ["Organic", "Paid AD", "Owned", "Awareness", "SNS"]
    base = pd.DataFrame({"bucket": buckets})
    out = (
        base.merge(cur_agg, on="bucket", how="left")
            .merge(prev_agg, on="bucket", how="left", suffixes=("", "_prev"))
            .fillna(0.0)
    )

    out["rev_dod"] = out.apply(lambda r: pct_change(r["purchaseRevenue"], r["purchaseRevenue_prev"]), axis=1)

    tot_cur_rev = float(out["purchaseRevenue"].sum())
    tot_prev_rev = float(out["purchaseRevenue_prev"].sum())
    total_row = {
        "bucket": "Total",
        "sessions": float(out["sessions"].sum()),
        "transactions": float(out["transactions"].sum()),
        "purchaseRevenue": tot_cur_rev,
        "sessions_prev": float(out["sessions_prev"].sum()),
        "transactions_prev": float(out["transactions_prev"].sum()),
        "purchaseRevenue_prev": tot_prev_rev,
        "rev_dod": pct_change(tot_cur_rev, tot_prev_rev),
    }

    out = pd.concat([out, pd.DataFrame([total_row])], ignore_index=True)
    return out[["bucket", "sessions", "transactions", "purchaseRevenue", "rev_dod"]]


def get_paid_detail(client: BetaAnalyticsDataClient, w: DailyWindow) -> pd.DataFrame:
    dims = ["sessionDefaultChannelGroup"]
    mets = ["sessions", "purchaseRevenue"]
    filt = ga_filter_in("sessionDefaultChannelGroup", PAID_SUBGROUPS)

    cur = run_report(client, PROPERTY_ID, ymd(w.yesterday), ymd(w.yesterday), dims, mets, dimension_filter=filt)
    prev = run_report(client, PROPERTY_ID, ymd(w.day_before), ymd(w.day_before), dims, mets, dimension_filter=filt)

    if cur.empty:
        cur = pd.DataFrame(columns=dims + mets)
    if prev.empty:
        prev = pd.DataFrame(columns=dims + mets)

    cur = cur.rename(columns={"sessionDefaultChannelGroup": "sub_channel"})
    prev = prev.rename(columns={"sessionDefaultChannelGroup": "sub_channel"})

    cur[mets] = cur[mets].apply(pd.to_numeric, errors="coerce").fillna(0.0)
    prev[mets] = prev[mets].apply(pd.to_numeric, errors="coerce").fillna(0.0)

    out = (
        pd.DataFrame({"sub_channel": PAID_SUBGROUPS})
        .merge(cur, on="sub_channel", how="left")
        .merge(prev, on="sub_channel", how="left", suffixes=("", "_prev"))
        .fillna(0.0)
    )
    out["rev_dod"] = out.apply(lambda r: pct_change(r["purchaseRevenue"], r["purchaseRevenue_prev"]), axis=1)

    total_cur_rev = float(out["purchaseRevenue"].sum())
    total_prev_rev = float(out["purchaseRevenue_prev"].sum())
    total = pd.DataFrame([{
        "sub_channel": "Total",
        "sessions": float(out["sessions"].sum()),
        "purchaseRevenue": total_cur_rev,
        "rev_dod": pct_change(total_cur_rev, total_prev_rev),
    }])

    out2 = out[["sub_channel", "sessions", "purchaseRevenue", "rev_dod"]]
    return pd.concat([out2, total], ignore_index=True)


def get_paid_top3(client: BetaAnalyticsDataClient, w: DailyWindow) -> pd.DataFrame:
    dims = ["sessionSourceMedium"]
    mets = ["sessions", "purchaseRevenue"]
    filt = ga_filter_in("sessionDefaultChannelGroup", PAID_SUBGROUPS)
    order = [OrderBy(metric=OrderBy.MetricOrderBy(metric_name="purchaseRevenue"), desc=True)]

    df = run_report(client, PROPERTY_ID, ymd(w.yesterday), ymd(w.yesterday), dims, mets, dimension_filter=filt, order_bys=order, limit=3)
    if df.empty:
        return pd.DataFrame(columns=["sessionSourceMedium", "sessions", "purchaseRevenue"])

    df["sessions"] = pd.to_numeric(df["sessions"], errors="coerce").fillna(0.0)
    df["purchaseRevenue"] = pd.to_numeric(df["purchaseRevenue"], errors="coerce").fillna(0.0)

    total = pd.DataFrame([{
        "sessionSourceMedium": "Total",
        "sessions": float(df["sessions"].sum()),
        "purchaseRevenue": float(df["purchaseRevenue"].sum()),
    }])

    return pd.concat([df, total], ignore_index=True)


def get_kpi_snapshot_table(client: BetaAnalyticsDataClient, w: DailyWindow, overall: Dict[str, Dict[str, float]]) -> pd.DataFrame:
    signup = get_multi_event_users(client, w, ["signup_complete", "signup"])
    cur = overall["current"]
    prev = overall["prev"]

    rows = [
        ("Sessions", cur["sessions"], prev["sessions"], "int"),
        ("CVR", cur["cvr"], prev["cvr"], "pct"),
        ("Revenue", cur["purchaseRevenue"], prev["purchaseRevenue"], "krw"),
        ("Orders", cur["transactions"], prev["transactions"], "int"),
        ("Sign-up Users", signup["current"], signup["prev"], "int"),
    ]

    out = []
    for metric, c, p, kind in rows:
        dod = pct_change(c, p) if kind != "pct" else (c - p)
        if kind == "int":
            value_fmt, dod_fmt = fmt_int(c), fmt_pct(dod, 1)
        elif kind == "krw":
            value_fmt, dod_fmt = fmt_currency_krw(c), fmt_pct(dod, 1)
        else:
            value_fmt, dod_fmt = f"{c*100:.2f}%", fmt_pp(dod, 2)
        out.append({"metric": metric, "value_fmt": value_fmt, "dod": dod, "dod_fmt": dod_fmt})
    return pd.DataFrame(out)


def get_trend_view_svg(client: BetaAnalyticsDataClient, w: DailyWindow) -> str:
    df = run_report(client, PROPERTY_ID, ymd(w.window_start), ymd(w.window_end), ["date"], ["sessions", "transactions", "purchaseRevenue"])
    if df.empty:
        x = [(w.window_start + dt.timedelta(days=i)).strftime("%m/%d") for i in range(7)]
        return combined_index_svg(x, [[100]*7, [100]*7, [100]*7], ["#0055a5", "#16a34a", "#c2410c"], ["Sessions", "Revenue", "CVR"])

    df["date"] = pd.to_datetime(df["date"], format="%Y%m%d", errors="coerce")
    df = df.dropna(subset=["date"]).sort_values("date")
    for c in ["sessions", "transactions", "purchaseRevenue"]:
        df[c] = pd.to_numeric(df[c], errors="coerce").fillna(0.0)

    df["cvr"] = df.apply(lambda r: (r["transactions"] / r["sessions"]) if r["sessions"] else 0.0, axis=1)
    x = df["date"].dt.strftime("%m/%d").tolist()
    s = index_series(df["sessions"].tolist())
    r = index_series(df["purchaseRevenue"].tolist())
    c = index_series(df["cvr"].tolist())
    return combined_index_svg(x, [s, r, c], ["#0055a5", "#16a34a", "#c2410c"], ["Sessions", "Revenue", "CVR"])


def get_best_sellers_with_trends(client: BetaAnalyticsDataClient, w: DailyWindow) -> pd.DataFrame:
    order = [OrderBy(metric=OrderBy.MetricOrderBy(metric_name="itemsPurchased"), desc=True)]
    top = run_report(
        client, PROPERTY_ID,
        ymd(w.yesterday), ymd(w.yesterday),
        ["itemId", "itemName"],
        ["itemsPurchased"],
        order_bys=order, limit=5
    )

    if top.empty:
        return pd.DataFrame(columns=["itemId", "itemName", "itemsPurchased_yesterday", "trend_svg"])

    top["itemsPurchased"] = pd.to_numeric(top["itemsPurchased"], errors="coerce").fillna(0.0)
    top = top.rename(columns={"itemsPurchased": "itemsPurchased_yesterday"})
    skus = [str(x).strip() for x in top["itemId"].tolist() if str(x).strip()]

    ts = run_report(
        client, PROPERTY_ID,
        ymd(w.window_start), ymd(w.window_end),
        ["date", "itemId"],
        ["itemsPurchased"],
        dimension_filter=ga_filter_in("itemId", skus),
        limit=10000
    )

    axis_dates = [w.window_start + dt.timedelta(days=i) for i in range(7)]
    xlabels = [d.strftime("%m/%d") for d in axis_dates]

    top["trend_svg"] = ""

    if ts.empty:
        return top[["itemId", "itemName", "itemsPurchased_yesterday", "trend_svg"]]

    ts["date"] = ts["date"].apply(parse_yyyymmdd)
    ts["itemsPurchased"] = pd.to_numeric(ts["itemsPurchased"], errors="coerce").fillna(0.0)
    ts = ts.sort_values(["itemId", "date"])

    svgs = []
    for sku in skus:
        sub = ts[ts["itemId"] == sku].set_index("date")["itemsPurchased"]
        ys = [float(sub.get(d, 0.0)) for d in axis_dates]
        svgs.append(spark_svg(xlabels, ys, width=240, height=70, stroke="#0055a5"))

    # skus 순서와 top row 순서가 다를 수 있으니 map으로 합치기
    sku_to_svg = {sku: svgs[i] for i, sku in enumerate(skus)}
    top["trend_svg"] = top["itemId"].astype(str).str.strip().map(lambda s: sku_to_svg.get(s, ""))
    return top[["itemId", "itemName", "itemsPurchased_yesterday", "trend_svg"]]


def get_rising_products(client: BetaAnalyticsDataClient, w: DailyWindow, top_n: int = 5) -> pd.DataFrame:
    d1 = run_report(client, PROPERTY_ID, ymd(w.yesterday), ymd(w.yesterday), ["itemId", "itemName"], ["itemsPurchased"], limit=10000)
    d0 = run_report(client, PROPERTY_ID, ymd(w.day_before), ymd(w.day_before), ["itemId"], ["itemsPurchased"], limit=10000)

    if d1.empty:
        return pd.DataFrame(columns=["itemId", "itemName", "itemViews_yesterday", "delta"])

    d1["itemsPurchased"] = pd.to_numeric(d1["itemsPurchased"], errors="coerce").fillna(0.0)
    if not d0.empty:
        d0["itemsPurchased"] = pd.to_numeric(d0["itemsPurchased"], errors="coerce").fillna(0.0)
    else:
        d0 = pd.DataFrame(columns=["itemId", "itemsPurchased"])

    m = d1.merge(d0, on="itemId", how="left", suffixes=("_y", "_d0")).fillna(0.0)
    m["delta"] = m["itemsPurchased_y"] - m["itemsPurchased_d0"]
    m = m.sort_values("delta", ascending=False).head(top_n)

    # Views best-effort
    skus = [str(x).strip() for x in m["itemId"].tolist() if str(x).strip()]
    views_df = pd.DataFrame(columns=["itemId", "itemViews_yesterday"])
    if skus:
        for metric_name, use_event_filter in [
            ("itemViewEvents", False),
            ("itemsViewed", False),
            ("eventCount", True),
        ]:
            try:
                v = run_report(
                    client,
                    PROPERTY_ID,
                    ymd(w.yesterday),
                    ymd(w.yesterday),
                    ["itemId"],
                    [metric_name],
                    dimension_filter=(
                        ga_filter_and([ga_filter_in("itemId", skus), ga_filter_eq("eventName", "view_item")])
                        if use_event_filter else ga_filter_in("itemId", skus)
                    ),
                    limit=10000,
                )
                if not v.empty:
                    v[metric_name] = pd.to_numeric(v[metric_name], errors="coerce").fillna(0.0)
                    views_df = v[["itemId", metric_name]].rename(columns={metric_name: "itemViews_yesterday"})
                break
            except Exception:
                continue

    m = m.merge(views_df, on="itemId", how="left")
    m["itemViews_y_]()

def get_rising_products(client: BetaAnalyticsDataClient, w: DailyWindow, top_n: int = 5) -> pd.DataFrame:
    d1 = run_report(
        client, PROPERTY_ID,
        ymd(w.yesterday), ymd(w.yesterday),
        ["itemId", "itemName"],
        ["itemsPurchased"],
        limit=10000
    )
    d0 = run_report(
        client, PROPERTY_ID,
        ymd(w.day_before), ymd(w.day_before),
        ["itemId"],
        ["itemsPurchased"],
        limit=10000
    )

    if d1.empty:
        return pd.DataFrame(columns=["itemId", "itemName", "itemViews_yesterday", "delta"])

    d1["itemsPurchased"] = pd.to_numeric(d1["itemsPurchased"], errors="coerce").fillna(0.0)
    if not d0.empty:
        d0["itemsPurchased"] = pd.to_numeric(d0["itemsPurchased"], errors="coerce").fillna(0.0)
    else:
        d0 = pd.DataFrame(columns=["itemId", "itemsPurchased"])

    m = d1.merge(d0, on="itemId", how="left", suffixes=("_y", "_d0")).fillna(0.0)
    m["delta"] = m["itemsPurchased_y"] - m["itemsPurchased_d0"]
    m = m.sort_values("delta", ascending=False).head(top_n)

    # Views best-effort (GA4 호환성 문제 때문에 fallback까지 포함)
    skus = [str(x).strip() for x in m["itemId"].tolist() if str(x).strip()]
    views_df = pd.DataFrame(columns=["itemId", "itemViews_yesterday"])

    if skus:
        for metric_name, use_event_filter in [
            ("itemViewEvents", False),
            ("itemsViewed", False),
            ("eventCount", True),  # view_item eventCount로 fallback
        ]:
            try:
                v = run_report(
                    client,
                    PROPERTY_ID,
                    ymd(w.yesterday),
                    ymd(w.yesterday),
                    ["itemId"],
                    [metric_name],
                    dimension_filter=(
                        ga_filter_and([ga_filter_in("itemId", skus), ga_filter_eq("eventName", "view_item")])
                        if use_event_filter else ga_filter_in("itemId", skus)
                    ),
                    limit=10000,
                )
                if not v.empty:
                    v[metric_name] = pd.to_numeric(v[metric_name], errors="coerce").fillna(0.0)
                    views_df = v[["itemId", metric_name]].rename(columns={metric_name: "itemViews_yesterday"})
                break
            except Exception:
                continue

    m = m.merge(views_df, on="itemId", how="left")
    m["itemViews_yesterday"] = pd.to_numeric(m.get("itemViews_yesterday"), errors="coerce").fillna(0.0)

    # 표기 통일
    return m[["itemId", "itemName", "itemViews_yesterday", "delta"]]


def get_search_trends(client: BetaAnalyticsDataClient, w: DailyWindow) -> Dict[str, pd.DataFrame]:
    """Search Trend: 신규 진입 Top3, 급상승 Top3 (D-1 vs 직전 7일 평균)"""
    lookback_start = w.window_end - dt.timedelta(days=13)
    df = run_report(
        client, PROPERTY_ID,
        ymd(lookback_start), ymd(w.window_end),
        ["date", "searchTerm"],
        ["eventCount"],
        dimension_filter=ga_filter_eq("eventName", SEARCH_EVENT),
        limit=10000
    )
    if df.empty:
        return {
            "new": pd.DataFrame(columns=["searchTerm"]),
            "rising": pd.DataFrame(columns=["searchTerm", "pct"])
        }

    df["date"] = df["date"].apply(parse_yyyymmdd)
    df["eventCount"] = pd.to_numeric(df["eventCount"], errors="coerce").fillna(0.0)

    # D-1
    y_df = (
        df[df["date"] == w.window_end]
        .groupby("searchTerm", as_index=False)["eventCount"].sum()
        .sort_values("eventCount", ascending=False)
    )

    # 직전 7일 평균 (D-8 ~ D-2)
    prior_start = w.window_end - dt.timedelta(days=7)
    prior_df = df[(df["date"] >= prior_start) & (df["date"] <= (w.window_end - dt.timedelta(days=1)))]
    prior_agg = (
        prior_df.groupby("searchTerm", as_index=False)["eventCount"].mean()
        .rename(columns={"eventCount": "prior_avg"})
    )

    merged = y_df.merge(prior_agg, on="searchTerm", how="left").fillna(0.0)

    new_terms = merged[merged["prior_avg"] == 0].head(3)[["searchTerm"]].copy()

    rising = merged[merged["prior_avg"] > 0].copy()
    rising["pct"] = (rising["eventCount"] - rising["prior_avg"]) / rising["prior_avg"] * 100.0
    rising = rising.sort_values("pct", ascending=False).head(3)[["searchTerm", "pct"]]

    return {"new": new_terms, "rising": rising}


# =============================================================================
# main: CSV 매핑 → 이미지 붙이기 → HTML 저장 → git push → Outlook 발송
# =============================================================================
def main():
    if not PROPERTY_ID:
        raise SystemExit("ERROR: GA4_PROPERTY_ID is empty. Set env var GA4_PROPERTY_ID and retry.")

    # ---------- GA4 creds ----------
    _scopes = [
        "https://www.googleapis.com/auth/analytics.readonly",
        "https://www.googleapis.com/auth/cloud-platform",
    ]
    _creds, _proj = google_auth_default(scopes=_scopes)
    client = BetaAnalyticsDataClient(credentials=_creds)

    # ---------- window ----------
    w = compute_window()

    # ---------- assets ----------
    logo_b64 = load_logo_base64(LOGO_PATH)

    # ---------- image map (CSV in repo) ----------
    csv_path = _abs_in_repo(PRODUCT_IMAGE_CSV)
    image_map = load_product_image_map(csv_path)
    safe_print(f"[OK] Loaded image map: {len(image_map):,} rows from {csv_path}")

    # ---------- core reports ----------
    overall = get_overall_kpis(client, w)
    signup_users = get_multi_event_users(client, w, ["signup_complete", "signup"])
    channel_snapshot = get_channel_snapshot(client, w)
    paid_detail = get_paid_detail(client, w)
    paid_top3 = get_paid_top3(client, w)
    kpi_snapshot = get_kpi_snapshot_table(client, w, overall)
    trend_svg = get_trend_view_svg(client, w)

    best_sellers = get_best_sellers_with_trends(client, w)
    best_sellers = attach_image_urls(best_sellers, image_map, sku_col="itemId")  # ✅ 이미지 붙이기

    rising = get_rising_products(client, w, top_n=5)
    rising = attach_image_urls(rising, image_map, sku_col="itemId")  # ✅ 이미지 붙이기

    # Search Trend
    search = get_search_trends(client, w)

    # ---------- missing sku export ----------
    missing = []
    if not best_sellers.empty and "itemId" in best_sellers.columns:
        missing += [sku for sku in best_sellers["itemId"].tolist() if str(sku).strip() not in image_map]
    if not rising.empty and "itemId" in rising.columns:
        missing += [sku for sku in rising["itemId"].tolist() if str(sku).strip() not in image_map]

    if missing:
        miss_out = _abs_in_repo(MISSING_SKU_OUT)
        write_missing_image_skus(miss_out, missing)

    # ---------- render html ----------
    # ⚠️ render_html 함수는 네가 위에서 이미 가지고 있는 “큰 템플릿” 그대로 사용하면 됨.
    #     (아래 render_html 호출 파라미터는 네 템플릿 시그니처에 맞춰 유지)
    html = render_html(
        logo_b64=logo_b64,
        w=w,
        overall=overall,
        signup_users=signup_users,
        channel_snapshot=channel_snapshot,
        paid_detail=paid_detail,
        paid_top3=paid_top3,
        kpi_snapshot=kpi_snapshot,
        trend_svg=trend_svg,
        best_sellers=best_sellers,
        rising=rising,
        category_pdp_trend=pd.DataFrame(),   # 네 템플릿에서 필요 없으면 제거/미사용 처리
        search_new=search["new"],
        search_rising=search["rising"],
    )

    # ---------- write HTML into repo ----------
    out_html_path = _abs_in_repo(OUTPUT_HTML)
    write_text(out_html_path, html)
    safe_print(f"[OK] Wrote HTML: {out_html_path}")
    safe_print(f"     Window: {ymd(w.window_start)} ~ {ymd(w.window_end)} (rolling 7d)")

    # ---------- git push ----------
    commit_msg = f"auto: daily digest {ymd(w.window_end)}"
    git_push(REPO_PATH, commit_msg)

    # ---------- send outlook ----------
    send_via_outlook(
        subject=f"{MAIL_SUBJECT} ({ymd(w.window_end)})",
        html_body=html,                 # ✅ 본문에 HTML 그대로
        to_list=WEEKLY_RECIPIENTS,
        cc_list=None
    )


if __name__ == "__main__":
    main()

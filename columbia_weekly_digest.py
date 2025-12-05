# -*- coding: utf-8 -*-
"""
Columbia Sportswear Korea
Weekly eCommerce Performance Digest (GA4 + HTML Mail + Charts)

- GA4 기준 KPI/채널/검색/퍼널 데이터를 '지난주 월~일' 기준으로 집계
- 히트맵/트렌드/퍼널/검색 스캐터 그래프 생성 (PNG 파일 + 메일 본문 이미지 삽입)
- 월요일 아침에 GitHub Actions로 실행하는 것을 전제로 설계

작성자: Jonathan + ChatGPT Co-pilot
"""

import os
import io
import base64
import smtplib
from datetime import datetime, timedelta

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange,
    Dimension,
    Metric,
    RunReportRequest,
)
from google.oauth2 import service_account


# ============================================================================
# 0) 환경 변수 / 기본 설정
# ============================================================================

# GA4 Property
# (GitHub Actions에 GA4_PROPERTY_ID가 비어 있으면 기본값 358593394 사용)
GA4_PROPERTY_ID = os.getenv("GA4_PROPERTY_ID") or "358593394"

# 서비스 계정 (GitHub Secrets: GA4_SERVICE_ACCOUNT_JSON 사용 권장)
SERVICE_ACCOUNT_JSON = os.getenv("GA4_SERVICE_ACCOUNT_JSON", "")

if SERVICE_ACCOUNT_JSON:
    SERVICE_ACCOUNT_FILE = "/tmp/ga4_service_account.json"
    with open(SERVICE_ACCOUNT_FILE, "w", encoding="utf-8") as f:
        f.write(SERVICE_ACCOUNT_JSON)
else:
    # 로컬 테스트용 백업 경로 (Colab 등)
    SERVICE_ACCOUNT_FILE = os.getenv(
        "GA4_SERVICE_ACCOUNT_FILE",
        "/content/drive/MyDrive/ga_service_account.json",
    )

# 메일 발송 관련
SMTP_PROVIDER = os.getenv("SMTP_PROVIDER", "gmail").lower()  # "gmail" or "outlook"
SMTP_HOST = os.getenv("SMTP_HOST")  # 지정 안 하면 PROVIDER 기준으로 자동
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")  # 예: "koreacolumbia@gmail.com"
SMTP_PASS = os.getenv("SMTP_PASS")  # GitHub Secret: SMTP_PASS

# 수신자 (없으면 DAILY_RECIPIENTS, 그것도 없으면 본인 메일 한 개)
WEEKLY_RECIPIENTS = [
    e.strip()
    for e in os.getenv(
        "WEEKLY_RECIPIENTS",
        os.getenv("DAILY_RECIPIENTS", "hugh.kang@Columbia.com"),
    ).split(",")
    if e.strip()
]

# 그래프 저장 폴더 (PPT용)
CHART_DIR = "charts"


# ============================================================================
# 1) 공통 유틸
# ============================================================================

def pct_change(curr, prev):
    """(curr - prev)/prev * 100 (%). prev가 0이면 0."""
    try:
        prev = float(prev)
        curr = float(curr)
        if prev == 0:
            return 0.0
        return round((curr - prev) / prev * 100, 1)
    except Exception:
        return 0.0


def safe_int(x):
    try:
        return int(float(x))
    except Exception:
        return 0


def safe_float(x):
    try:
        return float(x)
    except Exception:
        return 0.0


def format_money(won):
    w = round(safe_float(won))
    return f"{w:,}원"


def format_money_manwon(won):
    man = round(safe_float(won) / 10_000)
    return f"{man:,}만원"


def ensure_chart_dir():
    os.makedirs(CHART_DIR, exist_ok=True)


def fig_to_base64_and_file(fig, filename: str):
    """
    matplotlib Figure를 PNG 파일로 저장 + base64 string으로 변환해 리턴.
    - charts/filename 으로 저장
    - HTML <img src="data:image/png;base64,..."> 로 사용 가능
    """
    ensure_chart_dir()
    filepath = os.path.join(CHART_DIR, filename)
    fig.tight_layout()
    fig.savefig(filepath, dpi=120, bbox_inches="tight")

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=120, bbox_inches="tight")
    plt.close(fig)
    buf.seek(0)
    b64 = base64.b64encode(buf.read()).decode("ascii")
    return b64, filepath


# ============================================================================
# 2) GA4 Client & 공통 run_report
# ============================================================================

def ga_client():
    if not GA4_PROPERTY_ID:
        raise SystemExit("GA4_PROPERTY_ID가 비어 있습니다.")
    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        raise SystemExit(f"서비스 계정 파일을 찾을 수 없습니다: {SERVICE_ACCOUNT_FILE}")
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/analytics.readonly"],
    )
    return BetaAnalyticsDataClient(credentials=creds)


def ga_run_report(dimensions, metrics, start_date, end_date, limit=None, order_bys=None):
    client = ga_client()
    req = RunReportRequest(
        property=f"properties/{GA4_PROPERTY_ID}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[Dimension(name=d) for d in dimensions],
        metrics=[Metric(name=m) for m in metrics],
        limit=limit if limit else 0,
        order_bys=order_bys or [],
    )
    resp = client.run_report(req)
    headers = [h.name for h in resp.dimension_headers] + [
        h.name for h in resp.metric_headers
    ]
    rows = []
    for r in resp.rows:
        rows.append(
            [*[d.value for d in r.dimension_values], *[m.value for m in r.metric_values]]
        )
    df = pd.DataFrame(rows, columns=headers)
    for c in df.columns:
        try:
            df[c] = pd.to_numeric(df[c])
        except Exception:
            pass
    return df


# ============================================================================
# 3) 날짜 범위 (지난주 / 비교주)
# ============================================================================

def get_week_ranges():
    """
    월요일 아침 실행 기준:
    - this_week: 직전주 월~일
    - prev_week: 그 이전주 (W-1, WoW 비교용)
    - ly_week:   전년 동일 주차 (YoY 비교용)
    """
    today = datetime.now()
    weekday = today.weekday()  # Monday=0

    # 직전주 월요일 ~ 일요일
    this_mon = (today - timedelta(days=weekday + 7)).date()
    this_sun = this_mon + timedelta(days=6)

    # WoW 비교용: 그 이전주
    prev_mon = this_mon - timedelta(days=7)
    prev_sun = this_mon - timedelta(days=1)

    # YoY 비교: 날짜 그대로 연도만 -1 (예외 시 365일 전)
    try:
        ly_mon = this_mon.replace(year=this_mon.year - 1)
        ly_sun = this_sun.replace(year=this_sun.year - 1)
    except ValueError:
        ly_mon = this_mon - timedelta(days=365)
        ly_sun = this_sun - timedelta(days=365)

    def fmt(d):
        return d.strftime("%Y-%m-%d")

    label = f"{fmt(this_mon)} ~ {fmt(this_sun)}"

    return {
        "label": label,
        "this": (fmt(this_mon), fmt(this_sun)),
        "prev": (fmt(prev_mon), fmt(prev_sun)),
        "ly": (fmt(ly_mon), fmt(ly_sun)),
    }


# ============================================================================
# 4) KPI / 채널 / 검색 / 퍼널 데이터 소스
# ============================================================================

def src_kpi_range(start_date_str: str, end_date_str: str):
    """주간 합계 (dimension 없이 metric만)."""
    df = ga_run_report(
        dimensions=[],
        metrics=["sessions", "transactions", "purchaseRevenue", "newUsers"],
        start_date=start_date_str,
        end_date=end_date_str,
    )
    if df.empty:
        return {
            "sessions": 0,
            "transactions": 0,
            "purchaseRevenue": 0.0,
            "newUsers": 0,
        }
    row = df.iloc[0]
    return {
        "sessions": safe_int(row["sessions"]),
        "transactions": safe_int(row["transactions"]),
        "purchaseRevenue": safe_float(row["purchaseRevenue"]),
        "newUsers": safe_int(row["newUsers"]),
    }


def src_channel_uv_range(start_date_str: str, end_date_str: str):
    """주간 기준 채널별 세션."""
    df = ga_run_report(
        dimensions=["sessionDefaultChannelGroup"],
        metrics=["sessions"],
        start_date=start_date_str,
        end_date=end_date_str,
    )
    if df.empty:
        return {
            "total_uv": 0,
            "organic_uv": 0,
            "nonorganic_uv": 0,
            "organic_share": 0.0,
        }

    df = df.copy()
    df["sessions"] = pd.to_numeric(df["sessions"], errors="coerce").fillna(0).astype(int)
    total_uv = int(df["sessions"].sum())

    organic_uv = int(
        df.loc[df["sessionDefaultChannelGroup"] == "Organic Search", "sessions"].sum()
    )
    nonorganic_uv = total_uv - organic_uv
    organic_share = (organic_uv / total_uv * 100) if total_uv > 0 else 0.0

    return {
        "total_uv": total_uv,
        "organic_uv": organic_uv,
        "nonorganic_uv": nonorganic_uv,
        "organic_share": round(organic_share, 1),
    }


def build_weekly_kpi():
    """
    이번주 KPI + 지난주/전년동주 비교.
    - this: 지난주
    - prev: 지난주 이전
    - ly  : 전년 동일주
    """
    ranges = get_week_ranges()
    (this_start, this_end) = ranges["this"]
    (prev_start, prev_end) = ranges["prev"]
    (ly_start, ly_end) = ranges["ly"]

    kpi_this = src_kpi_range(this_start, this_end)
    kpi_prev = src_kpi_range(prev_start, prev_end)
    kpi_yoy = src_kpi_range(ly_start, ly_end)

    rev_this = kpi_this["purchaseRevenue"]
    rev_prev = kpi_prev["purchaseRevenue"]
    rev_yoy = kpi_yoy["purchaseRevenue"]

    uv_this = kpi_this["sessions"]
    uv_prev = kpi_prev["sessions"]
    uv_yoy = kpi_yoy["sessions"]

    ord_this = kpi_this["transactions"]
    ord_prev = kpi_prev["transactions"]
    ord_yoy = kpi_yoy["transactions"]

    new_this = kpi_this["newUsers"]
    new_prev = kpi_prev["newUsers"]
    new_yoy = kpi_yoy["newUsers"]

    cvr_this = (ord_this / uv_this * 100) if uv_this else 0.0
    cvr_prev = (ord_prev / uv_prev * 100) if uv_prev else 0.0
    cvr_yoy = (ord_yoy / uv_yoy * 100) if uv_yoy else 0.0

    aov_this = (rev_this / ord_this) if ord_this else 0.0
    aov_prev = (rev_prev / ord_prev) if ord_prev else 0.0
    aov_yoy = (rev_yoy / ord_yoy) if ord_yoy else 0.0

    ch_this = src_channel_uv_range(this_start, this_end)
    ch_prev = src_channel_uv_range(prev_start, prev_end)
    ch_yoy = src_channel_uv_range(ly_start, ly_end)

    organic_uv_this = ch_this["organic_uv"]
    organic_uv_prev = ch_prev["organic_uv"]
    organic_uv_yoy = ch_yoy["organic_uv"]

    nonorganic_uv_this = ch_this["nonorganic_uv"]
    nonorganic_uv_prev = ch_prev["nonorganic_uv"]
    nonorganic_uv_yoy = ch_yoy["nonorganic_uv"]

    organic_share_this = ch_this["organic_share"]
    organic_share_prev = ch_prev["organic_share"]
    organic_share_yoy = ch_yoy["organic_share"]

    return {
        "date_label": ranges["label"],

        # 매출
        "revenue_this": rev_this,
        "revenue_prev": rev_prev,
        "revenue_yoy": rev_yoy,
        "revenue_wow_pct": pct_change(rev_this, rev_prev),
        "revenue_yoy_pct": pct_change(rev_this, rev_yoy),

        # UV
        "uv_this": uv_this,
        "uv_prev": uv_prev,
        "uv_yoy": uv_yoy,
        "uv_wow_pct": pct_change(uv_this, uv_prev),
        "uv_yoy_pct": pct_change(uv_this, uv_yoy),

        # 주문수
        "orders_this": ord_this,
        "orders_prev": ord_prev,
        "orders_yoy": ord_yoy,
        "orders_wow_pct": pct_change(ord_this, ord_prev),
        "orders_yoy_pct": pct_change(ord_this, ord_yoy),

        # CVR
        "cvr_this": round(cvr_this, 2),
        "cvr_prev": round(cvr_prev, 2),
        "cvr_yoy": round(cvr_yoy, 2),
        "cvr_wow_pct": pct_change(cvr_this, cvr_prev),
        "cvr_yoy_pct": pct_change(cvr_this, cvr_yoy),

        # AOV
        "aov_this": aov_this,
        "aov_prev": aov_prev,
        "aov_yoy": aov_yoy,
        "aov_wow_pct": pct_change(aov_this, aov_prev),
        "aov_yoy_pct": pct_change(aov_this, aov_yoy),

        # 신규 방문자
        "new_this": new_this,
        "new_prev": new_prev,
        "new_yoy": new_yoy,
        "new_wow_pct": pct_change(new_this, new_prev),
        "new_yoy_pct": pct_change(new_this, new_yoy),

        # 오가닉 / 비오가닉
        "organic_uv_this": organic_uv_this,
        "organic_uv_prev": organic_uv_prev,
        "organic_uv_yoy": organic_uv_yoy,
        "organic_uv_wow_pct": pct_change(organic_uv_this, organic_uv_prev),
        "organic_uv_yoy_pct": pct_change(organic_uv_this, organic_uv_yoy),

        "nonorganic_uv_this": nonorganic_uv_this,
        "nonorganic_uv_prev": nonorganic_uv_prev,
        "nonorganic_uv_yoy": nonorganic_uv_yoy,
        "nonorganic_uv_wow_pct": pct_change(nonorganic_uv_this, nonorganic_uv_prev),
        "nonorganic_uv_yoy_pct": pct_change(nonorganic_uv_this, nonorganic_uv_yoy),

        "organic_share_this": organic_share_this,
        "organic_share_prev": organic_share_prev,
        "organic_share_yoy": organic_share_yoy,
        "organic_share_wow_pct": pct_change(organic_share_this, organic_share_prev),
        "organic_share_yoy_pct": pct_change(organic_share_this, organic_share_yoy),
    }


def src_daily_trend_last_4_weeks():
    """
    최근 4주(28일) 기준 일별 매출/UV/CVR 트렌드 데이터.
    Weekly 메일에는 '지난주 7일'만 그래프로 표시하되,
    회귀선은 4주 데이터 기반으로 그려도 된다.
    """
    end = datetime.now().date() - timedelta(days=1)
    start = end - timedelta(days=27)
    df = ga_run_report(
        dimensions=["date"],
        metrics=["sessions", "transactions", "purchaseRevenue"],
        start_date=start.strftime("%Y-%m-%d"),
        end_date=end.strftime("%Y-%m-%d"),
    )
    if df.empty:
        return pd.DataFrame()
    df["date"] = pd.to_datetime(df["date"])
    df["cvr"] = np.where(
        df["sessions"] > 0, df["transactions"] / df["sessions"] * 100, 0.0
    )
    return df.sort_values("date")


def src_channel_heatmap_data():
    """
    최근 4주 기준 요일 × 채널 히트맵용 데이터 (CVR).
    """
    end = datetime.now().date() - timedelta(days=1)
    start = end - timedelta(days=27)
    df = ga_run_report(
        dimensions=["date", "sessionDefaultChannelGroup"],
        metrics=["sessions", "transactions"],
        start_date=start.strftime("%Y-%m-%d"),
        end_date=end.strftime("%Y-%m-%d"),
    )
    if df.empty:
        return pd.DataFrame()

    df["date"] = pd.to_datetime(df["date"])
    df["weekday"] = df["date"].dt.day_name()  # Monday, ...
    df["cvr"] = np.where(
        df["sessions"] > 0, df["transactions"] / df["sessions"] * 100, 0.0
    )

    pivot = (
        df.groupby(["weekday", "sessionDefaultChannelGroup"])["cvr"]
        .mean()
        .reset_index()
    )
    # 요일 순서 정렬
    order = [
        "Monday",
        "Tuesday",
        "Wednesday",
        "Thursday",
        "Friday",
        "Saturday",
        "Sunday",
    ]
    pivot["weekday"] = pd.Categorical(pivot["weekday"], categories=order, ordered=True)
    pivot = pivot.sort_values(["weekday", "sessionDefaultChannelGroup"])
    heat = pivot.pivot(
        index="sessionDefaultChannelGroup", columns="weekday", values="cvr"
    )
    return heat


def src_search_performance(last_days=28):
    """
    최근 N일 검색어 효율성 데이터.
    """
    end = datetime.now().date() - timedelta(days=1)
    start = end - timedelta(days=last_days - 1)
    df = ga_run_report(
        dimensions=["searchTerm"],
        metrics=["eventCount", "transactions", "purchaseRevenue"],
        start_date=start.strftime("%Y-%m-%d"),
        end_date=end.strftime("%Y-%m-%d"),
        limit=500,
    )
    if df.empty:
        return pd.DataFrame(columns=["키워드", "검색수", "구매수", "CVR", "매출"])
    df.rename(
        columns={
            "searchTerm": "키워드",
            "eventCount": "검색수",
            "transactions": "구매수",
            "purchaseRevenue": "매출",
        },
        inplace=True,
    )
    df["검색수"] = pd.to_numeric(df["검색수"], errors="coerce").fillna(0).astype(int)
    df["구매수"] = pd.to_numeric(df["구매수"], errors="coerce").fillna(0).astype(int)
    df["매출"] = pd.to_numeric(df["매출"], errors="coerce").fillna(0.0)
    df["CVR"] = np.where(
        df["검색수"] > 0, df["구매수"] / df["검색수"] * 100, 0.0
    )
    return df.sort_values("검색수", ascending=False)


def src_funnel_last_week():
    """
    지난주 기준 view_item → add_to_cart → begin_checkout → purchase 퍼널 요약.
    GA4에서 이벤트 이름을 영어 기준으로 사용한다고 가정.
    """
    ranges = get_week_ranges()
    (s, e) = ranges["this"]

    df = ga_run_report(
        dimensions=["eventName"],
        metrics=["eventCount"],
        start_date=s,
        end_date=e,
    )
    if df.empty:
        return pd.DataFrame()

    df = df.copy()
    df["eventCount"] = pd.to_numeric(df["eventCount"], errors="coerce").fillna(0).astype(int)

    steps = ["view_item", "add_to_cart", "begin_checkout", "purchase"]
    data = []
    base = None
    for ev in steps:
        cnt = int(df.loc[df["eventName"] == ev, "eventCount"].sum())
        if base is None:
            base = cnt or 1
        rate = cnt / base * 100 if base > 0 else 0.0
        data.append({"event": ev, "count": cnt, "step_cvr": rate})

    return pd.DataFrame(data)


def src_price_band_matrix():
    """
    가격대 × 신규/재구매 히트맵용.
    - GA4 eCommerce items 기준 itemRevenue / itemsPurchased 로 단가 계산
    - dimension: itemName, newVsReturning
    - 다만 사이트 스키마에 따라 안 맞을 수 있으므로, 데이터 없으면 빈 DF 리턴.
    """
    end = datetime.now().date() - timedelta(days=1)
    start = end - timedelta(days=27)

    try:
        df = ga_run_report(
            dimensions=["itemName", "newVsReturning"],
            metrics=["itemRevenue", "itemsPurchased"],
            start_date=start.strftime("%Y-%m-%d"),
            end_date=end.strftime("%Y-%m-%d"),
            limit=2000,
        )
    except Exception:
        return pd.DataFrame()

    if df.empty:
        return pd.DataFrame()

    df["itemRevenue"] = pd.to_numeric(df["itemRevenue"], errors="coerce").fillna(0.0)
    df["itemsPurchased"] = pd.to_numeric(df["itemsPurchased"], errors="coerce").fillna(0)

    df = df[df["itemsPurchased"] > 0].copy()
    if df.empty:
        return pd.DataFrame()

    df["unit_price"] = df["itemRevenue"] / df["itemsPurchased"]

    bins = [0, 50000, 100000, 200000, np.inf]
    labels = ["~5만원", "5-10만원", "10-20만원", "20만원 이상"]
    df["price_band"] = pd.cut(df["unit_price"], bins=bins, labels=labels, right=False)

    pivot = (
        df.groupby(["price_band", "newVsReturning"])["itemsPurchased"]
        .sum()
        .reset_index()
    )
    heat = pivot.pivot(
        index="price_band", columns="newVsReturning", values="itemsPurchased"
    )
    return heat


# ============================================================================
# 5) 시각화 (그래프 → base64 + 파일 저장)
# ============================================================================

def plot_daily_trend(df: pd.DataFrame):
    """지난 4주 일자별 매출/UV + 지난주 영역 강조 + 회귀선."""
    if df.empty:
        return None, None

    fig, ax1 = plt.subplots(figsize=(7, 3))

    df = df.copy()
    df = df.sort_values("date")

    # 매출 (왼쪽 Y축)
    ax1.plot(df["date"], df["purchaseRevenue"], marker="o", linewidth=1)
    ax1.set_ylabel("Revenue")
    ax1.set_xticklabels(df["date"].dt.strftime("%m-%d"), rotation=45, ha="right")

    # 회귀선 (매출 기준)
    x = np.arange(len(df))
    y = df["purchaseRevenue"].values
    if len(df) > 1 and y.sum() > 0:
        coef = np.polyfit(x, y, 1)
        trend = np.poly1d(coef)(x)
        ax1.plot(df["date"], trend, linestyle="--")

    ax2 = ax1.twinx()
    ax2.plot(df["date"], df["cvr"], marker="s", linewidth=1)
    ax2.set_ylabel("CVR(%)")

    ax1.set_title("Last 4 Weeks — Daily Revenue & CVR")
    fig.autofmt_xdate()

    return fig_to_base64_and_file(fig, "daily_trend.png")


def plot_channel_heatmap(heat_df: pd.DataFrame):
    if heat_df is None or heat_df.empty:
        return None, None
    fig, ax = plt.subplots(figsize=(6, 3.5))
    sns.heatmap(heat_df, annot=True, fmt=".1f", cmap="Blues", ax=ax)
    ax.set_title("Channel × Weekday CVR Heatmap (Last 4 Weeks)")
    ax.set_xlabel("Weekday")
    ax.set_ylabel("Channel Group")
    return fig_to_base64_and_file(fig, "channel_heatmap.png")


def plot_search_scatter(df: pd.DataFrame):
    if df is None or df.empty:
        return None, None
    # 상위 검색수 100개만
    df = df.head(100).copy()
    fig, ax = plt.subplots(figsize=(6, 3.5))
    sc = ax.scatter(df["검색수"], df["CVR"], s=(df["매출"] / 100000) + 10, alpha=0.7)
    ax.set_xlabel("Search Volume")
    ax.set_ylabel("CVR(%)")
    ax.set_title("Search Term Efficiency (Last 28 Days)")
    return fig_to_base64_and_file(fig, "search_scatter.png")


def plot_funnel(df: pd.DataFrame):
    if df is None or df.empty:
        return None, None
    fig, ax1 = plt.subplots(figsize=(6, 3))
    steps = df["event"]
    counts = df["count"]
    cvr = df["step_cvr"]

    ax1.bar(steps, counts)
    ax1.set_ylabel("Event Count")
    ax1.set_xlabel("Funnel Step")
    ax1.set_title("Onsite Funnel (Last Week)")

    ax2 = ax1.twinx()
    ax2.plot(steps, cvr, marker="o", linestyle="--")
    ax2.set_ylabel("Step CVR vs view_item (%)")

    return fig_to_base64_and_file(fig, "funnel.png")


def plot_price_band_heatmap(heat_df: pd.DataFrame):
    if heat_df is None or heat_df.empty:
        return None, None
    fig, ax = plt.subplots(figsize=(5, 3))
    sns.heatmap(heat_df, annot=True, fmt=".0f", cmap="Purples", ax=ax)
    ax.set_title("Price Band × New vs Returning (Last 4 Weeks)")
    ax.set_xlabel("New vs Returning")
    ax.set_ylabel("Price Band")
    return fig_to_base64_and_file(fig, "price_band_heatmap.png")


# ============================================================================
# 6) 인사이트/액션 텍스트 생성
# ============================================================================

def build_insights(kpi, funnel_df, search_df):
    insights = []

    # 매출/UV/CVR
    if kpi["revenue_wow_pct"] > 0 and kpi["cvr_wow_pct"] > 0:
        insights.append(
            f"지난주 매출은 전주 대비 {kpi['revenue_wow_pct']:+.1f}% 증가했고, CVR은 {kpi['cvr_wow_pct']:+.1f}p 개선되었습니다."
        )
    elif kpi["revenue_wow_pct"] < 0 and kpi["uv_wow_pct"] < 0:
        insights.append(
            f"지난주 매출({kpi['revenue_wow_pct']:+.1f}%)과 UV({kpi['uv_wow_pct']:+.1f}%)가 함께 감소해 상단 퍼널 유입 점검이 필요합니다."
        )
    else:
        insights.append(
            f"지난주 매출 {kpi['revenue_wow_pct']:+.1f}%, UV {kpi['uv_wow_pct']:+.1f}%, CVR {kpi['cvr_wow_pct']:+.1f}p 변동을 보였습니다."
        )

    # 퍼널
    if funnel_df is not None and not funnel_df.empty:
        worst = funnel_df.sort_values("step_cvr").iloc[0]
        insights.append(
            f"view_item 대비 전환율이 가장 낮은 단계는 '{worst['event']}' 구간으로, 전체 대비 {worst['step_cvr']:.1f}% 수준입니다."
        )

    # 검색
    if search_df is not None and not search_df.empty:
        low = search_df[search_df["CVR"] < 1.0]
        if not low.empty:
            top_bad = ", ".join(low.head(2)["키워드"].tolist())
            insights.append(
                f"CVR 1% 미만 저효율 검색어는 {top_bad} 등이 있어 노출 상품/필터 재구성이 필요합니다."
            )
        else:
            top_good = ", ".join(search_df.head(3)["키워드"].tolist())
            insights.append(
                f"검색 전환 상위 키워드는 {top_good} 등으로, 관련 카테고리/기획전 확장 여지가 있습니다."
            )

    # 오가닉/비오가닉
    if kpi["organic_share_wow_pct"] < 0:
        insights.append(
            f"오가닉 UV 비중은 전주 대비 {kpi['organic_share_wow_pct']:+.1f}p 하락해 유료 채널 의존도가 높아졌습니다."
        )
    else:
        insights.append(
            f"오가닉 UV 비중은 {kpi['organic_share_this']:.1f}%로 유지 또는 개선되는 추세입니다."
        )

    return insights[:4]


def build_actions(kpi, funnel_df, search_df):
    actions = []

    # 상단 퍼널/예산
    if kpi["revenue_wow_pct"] < 0 and kpi["uv_wow_pct"] < 0:
        actions.append(
            "신규 유입 캠페인(브랜디드/논브랜디드 검색, 메타 상단광고)의 입찰·소재·타게팅을 우선 점검합니다."
        )
    elif kpi["cvr_wow_pct"] < 0:
        actions.append(
            "장바구니/체크아웃 구간에서 UX·프로모션을 점검하고, 이탈 세그먼트 대상 리마인드 캠페인을 테스트합니다."
        )
    else:
        actions.append(
            "성과가 좋은 채널/소재의 예산을 소폭 상향해 상승 구간을 더 밀어주는 테스트를 진행합니다."
        )

    # 퍼널 액션
    if funnel_df is not None and not funnel_df.empty:
        worst = funnel_df.sort_values("step_cvr").iloc[0]
        if worst["event"] == "add_to_cart":
            actions.append("상품 상세 → 장바구니 구간의 가격·혜택·리뷰 노출을 강화해 장바구니 전환을 끌어올립니다.")
        elif worst["event"] == "begin_checkout":
            actions.append("장바구니 → 체크아웃 구간에서 배송비/쿠폰/옵션 선택을 단순화하는 개선안을 검토합니다.")
        elif worst["event"] == "purchase":
            actions.append("체크아웃 → 결제 완료 구간에서 결제수단 오류, 인증 실패, 쿠폰 적용 이슈를 점검합니다.")

    # 검색 액션
    if search_df is not None and not search_df.empty:
        low = search_df[search_df["CVR"] < 1.0]
        if not low.empty:
            actions.append(
                "저전환 검색어에 연결된 상품·카테고리를 재구성하거나, 가격·할인 정책을 조정하는 안을 테스트합니다."
            )
        else:
            actions.append(
                "상위 검색어 기준으로 기획전/컬렉션 페이지를 추가 구성해 검색 → 구매 전환을 더 끌어올립니다."
            )

    if not actions:
        actions.append("성과가 좋은 채널/상품을 중심으로 소규모 예산 실험을 1~2개 설정해 다음 주에 결과를 확인합니다.")

    return actions[:4]


# ============================================================================
# 7) 메일 전송
# ============================================================================

def _smtp_server_and_port():
    if SMTP_HOST:
        return SMTP_HOST, SMTP_PORT
    if SMTP_PROVIDER == "gmail":
        return "smtp.gmail.com", 587
    if SMTP_PROVIDER == "outlook":
        return "smtp.office365.com", 587
    return "smtp.gmail.com", 587


def send_email_html(subject: str, html_body: str, recipients):
    if isinstance(recipients, str):
        recipients = [recipients]
    if not recipients:
        print("[WARN] 수신자가 없어 메일 발송 생략.")
        return

    if not (SMTP_USER and SMTP_PASS):
        print("[WARN] SMTP_USER/SMTP_PASS 없음 – 아래는 HTML 미리보기입니다.\n")
        print(html_body[:3000])
        return

    host, port = _smtp_server_and_port()

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = SMTP_USER
    msg["To"] = ", ".join(recipients)

    plain_text = "Columbia Weekly eCommerce Digest 입니다. 메일이 제대로 보이지 않으면 HTML 모드를 확인해주세요."
    msg.attach(MIMEText(plain_text, "plain", "utf-8"))
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    with smtplib.SMTP(host, port) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASS)
        server.sendmail(SMTP_USER, recipients, msg.as_string())
        print(f"[INFO] Weekly digest mail sent to: {', '.join(recipients)}")


# ============================================================================
# 8) HTML 템플릿 (KPI 9카드 + 그래프 + 인사이트/액션)
# ============================================================================

def compose_html_weekly(kpi, charts, insights, actions):
    """
    charts: dict 키 → base64 string (없으면 None)
      - daily_trend
      - channel_heatmap
      - funnel
      - price_band
      - search_scatter
    """
    # 인사이트/액션 HTML
    insight_li = "".join(f"<li>{txt}</li>" for txt in insights)
    action_li = "".join(f"<li>{txt}</li>" for txt in actions)

    def img_block(b64, title):
        if not b64:
            return f"<p style='font-size:11px;color:#999;'>{title}: 데이터 없음</p>"
        return f"""
        <div style="margin-bottom:12px;">
          <div style="font-size:11px;font-weight:600;margin-bottom:4px;">{title}</div>
          <img src="data:image/png;base64,{b64}" style="max-width:100%;border-radius:8px;border:1px solid #e4e7f2;" />
        </div>
        """

    html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="utf-8">
<title>Columbia Sportswear Korea — Weekly eCommerce Performance Digest</title>
</head>
<body style="margin:0; padding:0; background:#f5f7fb; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI','Noto Sans KR',Arial,sans-serif;">

<table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="background:#f5f7fb;">
  <tr>
    <td align="center">
      <table role="presentation" width="900" cellspacing="0" cellpadding="0" style="padding:24px 12px 24px 12px; background:#f5f7fb;">
        <tr><td>

          <!-- 헤더 -->
          <table width="100%" cellspacing="0" cellpadding="0"
                 style="background:#ffffff; border-radius:18px; border:1px solid #e6e9ef; box-shadow:0 6px 18px rgba(0,0,0,0.06);">
            <tr>
              <td style="padding:18px 20px 16px 20px;" valign="top">
                <div style="font-size:18px; font-weight:700; color:#0055a5; margin-bottom:2px;">
                  COLUMBIA SPORTSWEAR KOREA
                </div>
                <div style="font-size:13px; color:#555; margin-bottom:8px;">
                  Weekly eCommerce Performance Digest
                </div>
                <span style="display:inline-block; font-size:11px; padding:4px 10px; border-radius:999px;
                             background:#eaf3ff; color:#0055a5; margin-bottom:6px;">
                  {kpi['date_label']} 기준 (지난주 데이터)
                </span>
                <div style="font-size:11px; color:#777; margin-top:6px; margin-bottom:2px; line-height:1.6;">
                  지난 한 주간의 매출·UV·CVR 흐름과 채널/검색/퍼널 데이터를 PPT 없이 한 번에 볼 수 있도록 정리한 요약 메일입니다.
                </div>
              </td>
              <td valign="top" align="right" style="padding:16px 20px 16px 0%;">
                <span style="display:inline-block; font-size:10px; padding:4px 9px; border-radius:999px;
                             background:#0055a5; color:#ffffff; border:1px solid #0055a5; margin-left:4px;">
                  WEEKLY
                </span>
                <span style="display:inline-block; font-size:10px; padding:4px 9px; border-radius:999px;
                             background:#fafbfd; color:#445; border:1px solid #dfe6f3; margin-left:4px;">
                  KPI
                </span>
                <span style="display:inline-block; font-size:10px; padding:4px 9px; border-radius:999px;
                             background:#fafbfd; color:#445; border:1px solid #dfe6f3; margin-left:4px;">
                  CHANNEL
                </span>
                <span style="display:inline-block; font-size:10px; padding:4px 9px; border-radius:999px;
                             background:#fafbfd; color:#445; border:1px solid #dfe6f3; margin-left:4px;">
                  SEARCH & FUNNEL
                </span>
              </td>
            </tr>
          </table>

          <!-- 01 KPI SNAPSHOT -->
          <div style="font-size:11px; letter-spacing:0.12em; color:#6d7a99; margin-top:18px; margin-bottom:10px;">
            01 · WEEKLY KPI SNAPSHOT
          </div>

          <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:separate; border-spacing:8px 10px;">
            <tr>
              <!-- Revenue -->
              <td width="33%" valign="top">
                <div style="background:#ffffff; border-radius:16px; padding:14px 16px; border:1px solid #e1e7f5;">
                  <div style="font-size:11px; color:#777; margin-bottom:4px;">매출 (Revenue)</div>
                  <div style="font-size:18px; font-weight:700; margin-bottom:4px;">
                    {format_money_manwon(kpi['revenue_this'])}
                  </div>
                  <div style="font-size:10px; color:#999; margin-bottom:4px;">
                    WoW: {kpi['revenue_wow_pct']:+.1f}% · YoY: {kpi['revenue_yoy_pct']:+.1f}%
                  </div>
                  <div style="font-size:10px; color:#999;">
                    전주: {format_money_manwon(kpi['revenue_prev'])} / 전년동주: {format_money_manwon(kpi['revenue_yoy'])}
                  </div>
                </div>
              </td>

              <!-- UV -->
              <td width="33%" valign="top">
                <div style="background:#ffffff; border-radius:16px; padding:14px 16px; border:1px solid #e1e7f5;">
                  <div style="font-size:11px; color:#777; margin-bottom:4px;">방문자수 (UV)</div>
                  <div style="font-size:18px; font-weight:700; margin-bottom:4px;">
                    {kpi['uv_this']:,}명
                  </div>
                  <div style="font-size:10px; color:#999; margin-bottom:4px;">
                    WoW: {kpi['uv_wow_pct']:+.1f}% · YoY: {kpi['uv_yoy_pct']:+.1f}%
                  </div>
                  <div style="font-size:10px; color:#999;">
                    전주: {kpi['uv_prev']:,} / 전년동주: {kpi['uv_yoy']:,}
                  </div>
                </div>
              </td>

              <!-- CVR -->
              <td width="33%" valign="top">
                <div style="background:#ffffff; border-radius:16px; padding:14px 16px; border:1px solid #e1e7f5;">
                  <div style="font-size:11px; color:#777; margin-bottom:4px;">전환율 (CVR)</div>
                  <div style="font-size:18px; font-weight:700; margin-bottom:4px;">
                    {kpi['cvr_this']:.2f}%
                  </div>
                  <div style="font-size:10px; color:#999; margin-bottom:4px;">
                    WoW: {kpi['cvr_wow_pct']:+.1f}p · YoY: {kpi['cvr_yoy_pct']:+.1f}p
                  </div>
                  <div style="font-size:10px; color:#999;">
                    전주: {kpi['cvr_prev']:.2f}% / 전년동주: {kpi['cvr_yoy']:.2f}%
                  </div>
                </div>
              </td>
            </tr>

            <tr>
              <!-- Orders -->
              <td width="33%" valign="top">
                <div style="background:#ffffff; border-radius:16px; padding:14px 16px; border:1px solid #e1e7f5;">
                  <div style="font-size:11px; color:#777; margin-bottom:4px;">구매수 (Orders)</div>
                  <div style="font-size:18px; font-weight:700; margin-bottom:4px;">
                    {kpi['orders_this']:,}건
                  </div>
                  <div style="font-size:10px; color:#999; margin-bottom:4px;">
                    WoW: {kpi['orders_wow_pct']:+.1f}% · YoY: {kpi['orders_yoy_pct']:+.1f}%
                  </div>
                  <div style="font-size:10px; color:#999;">
                    전주: {kpi['orders_prev']:,} / 전년동주: {kpi['orders_yoy']:,}
                  </div>
                </div>
              </td>

              <!-- AOV -->
              <td width="33%" valign="top">
                <div style="background:#ffffff; border-radius:16px; padding:14px 16px; border:1px solid #e1e7f5;">
                  <div style="font-size:11px; color:#777; margin-bottom:4px;">객단가 (AOV)</div>
                  <div style="font-size:18px; font-weight:700; margin-bottom:4px;">
                    {format_money(kpi['aov_this'])}
                  </div>
                  <div style="font-size:10px; color:#999; margin-bottom:4px;">
                    WoW: {kpi['aov_wow_pct']:+.1f}% · YoY: {kpi['aov_yoy_pct']:+.1f}%
                  </div>
                  <div style="font-size:10px; color:#999;">
                    전주: {format_money(kpi['aov_prev'])} / 전년동주: {format_money(kpi['aov_yoy'])}
                  </div>
                </div>
              </td>

              <!-- New Visitors -->
              <td width="33%" valign="top">
                <div style="background:#ffffff; border-radius:16px; padding:14px 16px; border:1px solid #e1e7f5;">
                  <div style="font-size:11px; color:#777; margin-bottom:4px;">신규 방문자 (New Visitors)</div>
                  <div style="font-size:18px; font-weight:700; margin-bottom:4px;">
                    {kpi['new_this']:,}명
                  </div>
                  <div style="font-size:10px; color:#999; margin-bottom:4px;">
                    WoW: {kpi['new_wow_pct']:+.1f}% · YoY: {kpi['new_yoy_pct']:+.1f}%
                  </div>
                  <div style="font-size:10px; color:#999;">
                    전주: {kpi['new_prev']:,} / 전년동주: {kpi['new_yoy']:,}
                  </div>
                </div>
              </td>
            </tr>

            <tr>
              <!-- Organic UV -->
              <td width="33%" valign="top">
                <div style="background:#ffffff; border-radius:16px; padding:14px 16px; border:1px solid #e1e7f5;">
                  <div style="font-size:11px; color:#777; margin-bottom:4px;">오가닉 UV</div>
                  <div style="font-size:18px; font-weight:700; margin-bottom:4px;">
                    {kpi['organic_uv_this']:,}명
                  </div>
                  <div style="font-size:10px; color:#999; margin-bottom:4px;">
                    WoW: {kpi['organic_uv_wow_pct']:+.1f}% · YoY: {kpi['organic_uv_yoy_pct']:+.1f}%
                  </div>
                </div>
              </td>

              <!-- Non-organic UV -->
              <td width="33%" valign="top">
                <div style="background:#ffffff; border-radius:16px; padding:14px 16px; border:1px solid #e1e7f5;">
                  <div style="font-size:11px; color:#777; margin-bottom:4px;">비오가닉 UV</div>
                  <div style="font-size:18px; font-weight:700; margin-bottom:4px;">
                    {kpi['nonorganic_uv_this']:,}명
                  </div>
                  <div style="font-size:10px; color:#999; margin-bottom:4px;">
                    WoW: {kpi['nonorganic_uv_wow_pct']:+.1f}% · YoY: {kpi['nonorganic_uv_yoy_pct']:+.1f}%
                  </div>
                </div>
              </td>

              <!-- Organic Share -->
              <td width="33%" valign="top">
                <div style="background:#ffffff; border-radius:16px; padding:14px 16px; border:1px solid #e1e7f5;">
                  <div style="font-size:11px; color:#777; margin-bottom:4px;">오가닉 UV 비중</div>
                  <div style="font-size:18px; font-weight:700; margin-bottom:4px;">
                    {kpi['organic_share_this']:.1f}%
                  </div>
                  <div style="font-size:10px; color:#999; margin-bottom:4px;">
                    WoW: {kpi['organic_share_wow_pct']:+.1f}p · YoY: {kpi['organic_share_yoy_pct']:+.1f}p
                  </div>
                </div>
              </td>
            </tr>
          </table>

          <!-- 02 인사이트 & 액션 -->
          <div style="font-size:11px; letter-spacing:0.12em; color:#6d7a99; margin-top:18px; margin-bottom:10px;">
            02 · INSIGHTS & ACTIONS
          </div>

          <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:separate; border-spacing:8px 10px;">
            <tr>
              <td width="50%" valign="top">
                <table width="100%" cellpadding="0" cellspacing="0"
                       style="background:#ffffff; border-radius:14px; border:1px solid #e1e7f5; padding:10px 12px;">
                  <tr><td>
                    <div style="font-size:11px; font-weight:600; color:#004a99; margin-bottom:6px;">이번 주 핵심 인사이트</div>
                    <ul style="margin:0; padding-left:16px; font-size:11px; color:#555; line-height:1.6;">
                      {insight_li}
                    </ul>
                  </td></tr>
                </table>
              </td>
              <td width="50%" valign="top">
                <table width="100%" cellpadding="0" cellspacing="0"
                       style="background:#ffffff; border-radius:14px; border:1px solid #e1e7f5; padding:10px 12px;">
                  <tr><td>
                    <div style="font-size:11px; font-weight:600; color:#0f766e; margin-bottom:6px;">다음 주 액션 포인트</div>
                    <ul style="margin:0; padding-left:16px; font-size:11px; color:#555; line-height:1.6;">
                      {action_li}
                    </ul>
                  </td></tr>
                </table>
              </td>
            </tr>
          </table>

          <!-- 03 그래프 섹션 -->
          <div style="font-size:11px; letter-spacing:0.12em; color:#6d7a99; margin-top:18px; margin-bottom:10px;">
            03 · VISUAL SUMMARY (TREND · CHANNEL · FUNNEL · SEARCH)
          </div>

          <table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:separate; border-spacing:8px 10px;">
            <tr>
              <td width="50%" valign="top">
                {img_block(charts.get('daily_trend'), 'Daily Revenue & CVR (Last 4 Weeks)')}
                {img_block(charts.get('channel_heatmap'), 'Channel × Weekday CVR Heatmap (Last 4 Weeks)')}
              </td>
              <td width="50%" valign="top">
                {img_block(charts.get('funnel'), 'Onsite Funnel (view_item → purchase, Last Week)')}
                {img_block(charts.get('price_band'), 'Price Band × New vs Returning (Last 4 Weeks)')}
                {img_block(charts.get('search_scatter'), 'Search Term Efficiency (Last 28 Days)')}
              </td>
            </tr>
          </table>

          <div style="margin-top:18px; font-size:10px; color:#99a; text-align:right;">
            Columbia Sportswear Korea · Weekly eCommerce Digest · GA4 · Python Automation
          </div>

        </td></tr>
      </table>
    </td>
  </tr>
</table>

</body>
</html>
"""
    return html


# ============================================================================
# 9) 메인 실행 함수
# ============================================================================

def send_weekly_digest():
    # 1) 데이터 수집
    ranges = get_week_ranges()
    (this_start, this_end) = ranges["this"]
    print(f"[INFO] Weekly digest range: {this_start} ~ {this_end}")

    kpi = build_weekly_kpi()
    daily_df = src_daily_trend_last_4_weeks()
    ch_heat = src_channel_heatmap_data()
    search_df = src_search_performance()
    funnel_df = src_funnel_last_week()
    price_heat = src_price_band_matrix()

    # 2) 시각화 (각각 PNG + base64)
    charts = {}

    fig_b64, _ = plot_daily_trend(daily_df)
    charts["daily_trend"] = fig_b64

    fig_b64, _ = plot_channel_heatmap(ch_heat)
    charts["channel_heatmap"] = fig_b64

    fig_b64, _ = plot_funnel(funnel_df)
    charts["funnel"] = fig_b64

    fig_b64, _ = plot_price_band_heatmap(price_heat)
    charts["price_band"] = fig_b64

    fig_b64, _ = plot_search_scatter(search_df)
    charts["search_scatter"] = fig_b64

    # 3) 인사이트 & 액션 텍스트
    insights = build_insights(kpi, funnel_df, search_df)
    actions = build_actions(kpi, funnel_df, search_df)

    # 4) HTML 메일 구성
    html = compose_html_weekly(kpi, charts, insights, actions)

    # 5) 발송
    subject = f"[COLUMBIA] Weekly eCommerce Digest — {kpi['date_label']}"
    send_email_html(subject, html, WEEKLY_RECIPIENTS)


# ============================================================================
# Entry point
# ============================================================================

if __name__ == "__main__":
    send_weekly_digest()

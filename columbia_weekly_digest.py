# -*- coding: utf-8 -*-
"""
Columbia Sportswear Korea
Weekly eCommerce Performance Digest (GA4 + HTML Mail)

- GA4 기준 KPI, 채널, 검색을 주간 단위로 집계해서
  PPT용/리포트용으로 보는 위클리 다이제스트 메일.

이 스크립트는 "위클리 전용"입니다.
GitHub Actions 등에서 월요일 아침에만 실행되도록 스케줄링하면 됩니다.
"""

import os
import smtplib
import pandas as pd
from datetime import datetime, timedelta

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from google.oauth2 import service_account


# =====================================================================
# 0) 환경 변수 / 기본 설정
# =====================================================================

# GA4
GA4_PROPERTY_ID = os.getenv("GA4_PROPERTY_ID", "358593394").strip()

# 메일 발송 설정
SMTP_PROVIDER = os.getenv("SMTP_PROVIDER", "gmail").lower()  # "gmail" or "outlook"
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "koreacolumbia@gmail.com")
SMTP_PASS = os.getenv("SMTP_PASS", "xxopfytdkxcyhisa")

WEEKLY_RECIPIENTS = [
    e.strip()
    for e in os.getenv(
        "WEEKLY_RECIPIENTS",
        os.getenv("DAILY_RECIPIENTS", "hugh.kang@Columbia.com")
    ).split(",")
    if e.strip()
]

# =====================================================================
# 1) 유틸 함수
# =====================================================================

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


# =====================================================================
# 2) 메일 유틸
# =====================================================================

def _smtp_server_and_port():
    if SMTP_PROVIDER == "gmail":
        return ("smtp.gmail.com", 587)
    elif SMTP_PROVIDER == "outlook":
        return ("smtp.office365.com", 587)
    else:
        return (SMTP_HOST, SMTP_PORT)


def send_email_html(subject: str, html_body: str, recipients):
    """HTML 메일 발송 (주간 digest 전용)."""
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


# =====================================================================
# 3) GA4 Client & 공통 run_report
# =====================================================================

SERVICE_ACCOUNT_JSON = os.getenv("GA4_SERVICE_ACCOUNT_JSON", "")

if SERVICE_ACCOUNT_JSON:
    SERVICE_ACCOUNT_FILE = "/tmp/ga4_service_account.json"
    with open(SERVICE_ACCOUNT_FILE, "w", encoding="utf-8") as f:
        f.write(SERVICE_ACCOUNT_JSON)
else:
    SERVICE_ACCOUNT_FILE = os.getenv(
        "GA4_SERVICE_ACCOUNT_FILE",
        "/content/drive/MyDrive/Colab Notebooks/awesome-aspect-467505-r6-02b6747c0a3b.json",
    )


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
    headers = [h.name for h in resp.dimension_headers] + [h.name for h in resp.metric_headers]
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


# =====================================================================
# 4) WEEKLY 데이터 소스 & KPI
# =====================================================================

def get_last_week_ranges():
    """
    월요일 아침 실행 기준:
    - this_week: 직전주 월~일
    - ld_week:   직전주 이전주 (W-1)
    - lw_week:   그 이전주 (W-2)  → KPI 카드의 LW에 연결
    - ly_week:   전년 동일 주차 (월~일)
    """
    today = datetime.now()
    weekday = today.weekday()  # Monday=0

    # 직전주 월요일 ~ 일요일
    this_mon = today - timedelta(days=weekday + 7)
    this_mon = this_mon.date()
    this_sun = this_mon + timedelta(days=6)

    # 직전주 이전주 (LD)
    ld_mon = this_mon - timedelta(days=7)
    ld_sun = this_mon - timedelta(days=1)

    # 그 이전주 (LW)
    lw_mon = ld_mon - timedelta(days=7)
    lw_sun = ld_mon - timedelta(days=1)

    # 전년 동일 주차
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
        "ld": (fmt(ld_mon), fmt(ld_sun)),
        "lw": (fmt(lw_mon), fmt(lw_sun)),
        "ly": (fmt(ly_mon), fmt(ly_sun)),
    }


def src_kpi_range(start_date_str: str, end_date_str: str):
    """주간 합계를 위해 dimension 없이 metric만 집계."""
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


def _channel_uv_for_range(start_date_str: str, end_date_str: str):
    """주간 범위 기준 전체 UV / 오가닉 UV / 비오가닉 UV / 오가닉 비중."""
    df = ga_run_report(
        dimensions=["sessionDefaultChannelGroup"],
        metrics=["sessions"],
        start_date=start_date_str,
        end_date=end_date_str,
    )
    if df is None or df.empty:
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
    DAILY 9카드 구조를 그대로 쓰되,
    KPI 값은 '주간 합계' 기준으로 계산.
    - today: 직전주
    - LD:    직전주 이전주
    - LW:    그 이전주
    - LY:    전년 동일주
    """
    ranges = get_last_week_ranges()
    (this_start, this_end) = ranges["this"]
    (ld_start, ld_end) = ranges["ld"]
    (lw_start, lw_end) = ranges["lw"]
    (ly_start, ly_end) = ranges["ly"]

    kpi_this = src_kpi_range(this_start, this_end)
    kpi_ld = src_kpi_range(ld_start, ld_end)
    kpi_prev = src_kpi_range(lw_start, lw_end)
    kpi_yoy = src_kpi_range(ly_start, ly_end)

    # 기본 KPI 값
    rev_today = kpi_this["purchaseRevenue"]
    rev_ld = kpi_ld["purchaseRevenue"]
    rev_prev = kpi_prev["purchaseRevenue"]
    rev_yoy = kpi_yoy["purchaseRevenue"]

    uv_today = kpi_this["sessions"]
    uv_ld = kpi_ld["sessions"]
    uv_prev = kpi_prev["sessions"]
    uv_yoy = kpi_yoy["sessions"]

    ord_today = kpi_this["transactions"]
    ord_ld = kpi_ld["transactions"]
    ord_prev = kpi_prev["transactions"]
    ord_yoy = kpi_yoy["transactions"]

    new_today = kpi_this["newUsers"]
    new_ld = kpi_ld["newUsers"]
    new_prev = kpi_prev["newUsers"]
    new_yoy = kpi_yoy["newUsers"]

    cvr_today = (ord_today / uv_today * 100) if uv_today else 0.0
    cvr_ld = (ord_ld / uv_ld * 100) if uv_ld else 0.0
    cvr_prev = (ord_prev / uv_prev * 100) if uv_prev else 0.0
    cvr_yoy = (ord_yoy / uv_yoy * 100) if uv_yoy else 0.0

    aov_today = (rev_today / ord_today) if ord_today else 0.0
    aov_ld = (rev_ld / ord_ld) if ord_ld else 0.0
    aov_prev = (rev_prev / ord_prev) if ord_prev else 0.0
    aov_yoy = (rev_yoy / ord_yoy) if ord_yoy else 0.0

    # 오가닉 / 비오가닉 UV & 비중
    ch_today = _channel_uv_for_range(this_start, this_end)
    ch_ld = _channel_uv_for_range(ld_start, ld_end)
    ch_prev = _channel_uv_for_range(lw_start, lw_end)
    ch_yoy = _channel_uv_for_range(ly_start, ly_end)

    organic_uv_today = ch_today["organic_uv"]
    organic_uv_ld = ch_ld["organic_uv"]
    organic_uv_prev = ch_prev["organic_uv"]
    organic_uv_yoy = ch_yoy["organic_uv"]

    nonorganic_uv_today = ch_today["nonorganic_uv"]
    nonorganic_uv_ld = ch_ld["nonorganic_uv"]
    nonorganic_uv_prev = ch_prev["nonorganic_uv"]
    nonorganic_uv_yoy = ch_yoy["nonorganic_uv"]

    organic_share_today = ch_today["organic_share"]
    organic_share_ld = ch_ld["organic_share"]
    organic_share_prev = ch_prev["organic_share"]
    organic_share_yoy = ch_yoy["organic_share"]

    kpi = {
        "date_label": ranges["label"],  # 주간 라벨

        # 매출
        "revenue_today": rev_today,
        "revenue_ld": rev_ld,
        "revenue_prev": rev_prev,
        "revenue_yoy": rev_yoy,
        "revenue_ld_pct": pct_change(rev_today, rev_ld),
        "revenue_lw_pct": pct_change(rev_today, rev_prev),
        "revenue_ly_pct": pct_change(rev_today, rev_yoy),

        # UV
        "uv_today": uv_today,
        "uv_ld": uv_ld,
        "uv_prev": uv_prev,
        "uv_yoy": uv_yoy,
        "uv_ld_pct": pct_change(uv_today, uv_ld),
        "uv_lw_pct": pct_change(uv_today, uv_prev),
        "uv_ly_pct": pct_change(uv_today, uv_yoy),

        # 주문수
        "orders_today": ord_today,
        "orders_ld": ord_ld,
        "orders_prev": ord_prev,
        "orders_yoy": ord_yoy,
        "orders_ld_pct": pct_change(ord_today, ord_ld),
        "orders_lw_pct": pct_change(ord_today, ord_prev),
        "orders_ly_pct": pct_change(ord_today, ord_yoy),

        # CVR
        "cvr_today": round(cvr_today, 2),
        "cvr_ld": round(cvr_ld, 2),
        "cvr_prev": round(cvr_prev, 2),
        "cvr_yoy": round(cvr_yoy, 2),
        "cvr_ld_pct": pct_change(cvr_today, cvr_ld),
        "cvr_lw_pct": pct_change(cvr_today, cvr_prev),
        "cvr_ly_pct": pct_change(cvr_today, cvr_yoy),

        # AOV
        "aov_today": aov_today,
        "aov_ld": aov_ld,
        "aov_prev": aov_prev,
        "aov_yoy": aov_yoy,
        "aov_ld_pct": pct_change(aov_today, aov_ld),
        "aov_lw_pct": pct_change(aov_today, aov_prev),
        "aov_ly_pct": pct_change(aov_today, aov_yoy),

        # 신규 방문자
        "new_today": new_today,
        "new_ld": new_ld,
        "new_prev": new_prev,
        "new_yoy": new_yoy,
        "new_ld_pct": pct_change(new_today, new_ld),
        "new_lw_pct": pct_change(new_today, new_prev),
        "new_ly_pct": pct_change(new_today, new_yoy),

        # 오가닉 UV
        "organic_uv_today": organic_uv_today,
        "organic_uv_ld": organic_uv_ld,
        "organic_uv_prev": organic_uv_prev,
        "organic_uv_yoy": organic_uv_yoy,
        "organic_uv_ld_pct": pct_change(organic_uv_today, organic_uv_ld),
        "organic_uv_lw_pct": pct_change(organic_uv_today, organic_uv_prev),
        "organic_uv_ly_pct": pct_change(organic_uv_today, organic_uv_yoy),

        # 비오가닉 UV
        "nonorganic_uv_today": nonorganic_uv_today,
        "nonorganic_uv_ld": nonorganic_uv_ld,
        "nonorganic_uv_prev": nonorganic_uv_prev,
        "nonorganic_uv_yoy": nonorganic_uv_yoy,
        "nonorganic_uv_ld_pct": pct_change(nonorganic_uv_today, nonorganic_uv_ld),
        "nonorganic_uv_lw_pct": pct_change(nonorganic_uv_today, nonorganic_uv_prev),
        "nonorganic_uv_ly_pct": pct_change(nonorganic_uv_today, nonorganic_uv_yoy),

        # 오가닉 UV 비중
        "organic_share_today": organic_share_today,
        "organic_share_ld": organic_share_ld,
        "organic_share_prev": organic_share_prev,
        "organic_share_yoy": organic_share_yoy,
        "organic_share_ld_pct": pct_change(organic_share_today, organic_share_ld),
        "organic_share_lw_pct": pct_change(organic_share_today, organic_share_prev),
        "organic_share_ly_pct": pct_change(organic_share_today, organic_share_yoy),
    }
    return kpi


def src_traffic_range(start_date_str: str, end_date_str: str):
    """주간 범위 기준 채널별 유입."""
    df = ga_run_report(
        dimensions=["sessionDefaultChannelGroup"],
        metrics=["sessions", "transactions", "newUsers", "purchaseRevenue"],
        start_date=start_date_str,
        end_date=end_date_str,
    )
    if df.empty:
        return pd.DataFrame(columns=["소스", "UV", "구매수", "매출(만원)", "CVR(%)", "신규 방문자"])
    df.rename(
        columns={
            "sessionDefaultChannelGroup": "소스",
            "sessions": "UV",
            "transactions": "구매수",
            "newUsers": "신규 방문자",
            "purchaseRevenue": "매출(원)",
        },
        inplace=True,
    )
    df["UV"] = pd.to_numeric(df["UV"], errors="coerce").fillna(0).astype(int)
    df["구매수"] = pd.to_numeric(df["구매수"], errors="coerce").fillna(0).astype(int)
    df["매출(원)"] = pd.to_numeric(df["매출(원)"], errors="coerce").fillna(0.0)
    df["매출(만원)"] = (df["매출(원)"] / 10_000).round(1)
    df["CVR(%)"] = (df["구매수"] / df["UV"] * 100).replace([float("inf")], 0).fillna(0).round(2)
    df = df.sort_values("UV", ascending=False)
    return df[["소스", "UV", "구매수", "매출(만원)", "CVR(%)", "신규 방문자"]]


def src_search_range(start_date_str: str, end_date_str: str, limit=100):
    df = ga_run_report(
        dimensions=["searchTerm"],
        metrics=["eventCount", "transactions"],
        start_date=start_date_str,
        end_date=end_date_str,
        limit=limit,
    )
    if df.empty:
        return pd.DataFrame(columns=["키워드", "검색수", "구매수", "CVR(%)"])
    df.rename(
        columns={
            "searchTerm": "키워드",
            "eventCount": "검색수",
            "transactions": "구매수",
        },
        inplace=True,
    )
    df["검색수"] = pd.to_numeric(df["검색수"], errors="coerce").fillna(0).astype(int)
    df["구매수"] = pd.to_numeric(df["구매수"], errors="coerce").fillna(0).astype(int)
    df["CVR(%)"] = (df["구매수"] / df["검색수"] * 100).replace([float("inf")], 0).fillna(0).round(2)
    df = df.sort_values("검색수", ascending=False)
    return df


# =====================================================================
# 5) 인사이트 / 액션 생성 (주간)
# =====================================================================

SEARCH_CVR_MIN = float(os.getenv("SEARCH_CVR_MIN", "1.0"))

def build_signals(kpi, traffic_df, search_df):
    """주간 GA4 데이터 기반 핵심 인사이트 문장 리스트 (최대 4개)."""
    signals = []

    # 1) 매출 / UV / CVR
    if kpi["revenue_lw_pct"] > 0 and kpi["cvr_lw_pct"] > 0:
        signals.append(
            f"지난주 매출이 전전주 대비 {kpi['revenue_lw_pct']:.1f}% ↑, CVR은 {kpi['cvr_lw_pct']:.1f}p 개선되었습니다."
        )
    elif kpi["revenue_lw_pct"] < 0 and kpi["uv_lw_pct"] < 0:
        signals.append(
            f"지난주 매출({kpi['revenue_lw_pct']:.1f}%)과 UV({kpi['uv_lw_pct']:.1f}%)가 함께 감소해 상단 퍼널 유입 점검이 필요합니다."
        )
    else:
        signals.append(
            f"지난주 매출 {kpi['revenue_lw_pct']:.1f}%, UV {kpi['uv_lw_pct']:.1f}%, CVR {kpi['cvr_lw_pct']:.1f}p 변동을 보였습니다."
        )

    # 2) 채널
    if traffic_df is not None and not traffic_df.empty:
        top = traffic_df.iloc[0]
        signals.append(
            f"유입은 {top['소스']} 채널(UV {int(top['UV']):,}명, CVR {top['CVR(%)']:.2f}%) 비중이 가장 컸습니다."
        )

    # 3) 검색
    if search_df is not None and not search_df.empty:
        bad = search_df[search_df["CVR(%)"] < SEARCH_CVR_MIN]
        if not bad.empty:
            top_bad = bad.head(2)["키워드"].tolist()
            signals.append(
                f"저전환 검색어(CVR {SEARCH_CVR_MIN}% 미만)는 {', '.join(top_bad)} 등이 있어 결과 보완이 필요합니다."
            )

    fallback = [
        "· 지난주 채널/검색/상품 흐름을 함께 보면서 이번 주 액션 포인트를 잡을 수 있습니다.",
        "· 예산·소재·랜딩 페이지를 중심으로 전환 개선 여지를 체크해 주세요.",
    ]
    while len(signals) < 4:
        signals.append(fallback[len(signals) % len(fallback)])

    return signals[:4]


def build_actions(kpi, traffic_df, search_df):
    """이번 주 취할 액션 리스트 (최대 4개)."""
    actions = []

    # 1) 상단 퍼널 / CVR 액션
    if kpi["revenue_lw_pct"] < 0 and kpi["uv_lw_pct"] < 0:
        actions.append("매출·UV가 동반 하락했으므로 신규 유입 캠페인(입찰·소재·타게팅)을 우선 점검합니다.")
    elif kpi["cvr_lw_pct"] < 0:
        actions.append("CVR이 기준 대비 하락해 장바구니·체크아웃 구간의 전환율과 UX를 집중적으로 확인합니다.")
    else:
        actions.append("성과가 좋은 채널/소재의 예산을 소폭 상향해 상승 구간을 더 밀어주는 테스트를 진행합니다.")

    # 2) 채널 액션
    if traffic_df is not None and not traffic_df.empty:
        top = traffic_df.iloc[0]
        actions.append(
            f"{top['소스']} 채널의 성과 좋은 소재를 기준으로 유사 카피·이미지를 다른 채널에도 확장 테스트합니다."
        )

    # 3) 검색 액션
    if search_df is not None and not search_df.empty:
        bad = search_df[search_df["CVR(%)"] < SEARCH_CVR_MIN]
        if not bad.empty:
            actions.append("저전환 검색어의 노출 상품/필터를 재구성하거나, 상세 설명·가격 정책을 조정하는 안을 검토합니다.")
        else:
            actions.append("상위 검색어 기준으로 기획전/컬렉션 페이지를 추가 구성해 전환을 더 끌어올릴 수 있는지 테스트합니다.")

    fallback = [
        "이번 주에는 상위 채널/상품 1~2개를 선정해 소규모 예산으로 집중 실험을 바로 실행합니다.",
    ]
    while len(actions) < 4:
        actions.append(fallback[0])

    return actions[:4]


# =====================================================================
# 6) HTML 템플릿 – Weekly (KPI 9카드 + 채널/검색)
# =====================================================================

def compose_html_weekly(kpi_weekly, traffic_week_df, search_week_df):
    """
    3-2 섹션(9 KPI 카드)은 Daily와 완전히 동일한 레이아웃을 쓰되,
    kpi_weekly 값이 모두 '주간 합계' 집계값으로 들어가도록 구성.
    나머지는 주간 요약 텍스트/표 위주로 간단 구성.
    """

    signals_list = build_signals(kpi_weekly, traffic_week_df, search_week_df)
    actions_list = build_actions(kpi_weekly, traffic_week_df, search_week_df)

    insight_items_html = "".join(
        f"<li style='margin-bottom:3px;'>{s}</li>" for s in signals_list
    )
    action_items_html = "".join(
        f"<li style='margin-bottom:3px;'>{s}</li>" for s in actions_list
    )

    insight_card_html = f"""
<table width="100%" cellpadding="0" cellspacing="0"
       style="background:#ffffff; border-radius:14px;
              border:1px solid #e1e7f5; box-shadow:0 4px 12px rgba(0,0,0,0.04);
              padding:10px 12px; border-collapse:separate;">
  <tr><td>
    <div style="font-size:11px; font-weight:600; color:#004a99; margin-bottom:4px;">
      이번 주 인사이트
    </div>
    <ul style="margin:0; padding-left:16px; font-size:11px; color:#555; line-height:1.6;">
      {insight_items_html}
    </ul>
  </td></tr>
</table>
"""

    action_card_html = f"""
<table width="100%" cellpadding="0" cellspacing="0"
       style="background:#ffffff; border-radius:14px;
              border:1px solid #e1e7f5; box-shadow:0 4px 12px rgba(0,0,0,0.04);
              padding:10px 12px; border-collapse:separate;">
  <tr><td>
    <div style="font-size:11px; font-weight:600; color:#0f766e; margin-bottom:4px;">
      다음 주 액션 힌트
    </div>
    <ul style="margin:0; padding-left:16px; font-size:11px; color:#555; line-height:1.6;">
      {action_items_html}
    </ul>
  </td></tr>
</table>
"""

    insight_action_html = f"""
<!-- Insight & Action Cards -->
<table width="100%" cellpadding="0" cellspacing="0"
       style="border-collapse:separate; border-spacing:8px 10px; margin-top:14px;">
  <tr>
    <td width="50%" valign="top">{insight_card_html}</td>
    <td width="50%" valign="top">{action_card_html}</td>
  </tr>
</table>
"""

    # 채널 / 검색 요약 표
    if traffic_week_df is None or traffic_week_df.empty:
        traffic_table_html = "<p style='font-size:11px;color:#999;margin:0;'>데이터 없음</p>"
    else:
        tmp = traffic_week_df.copy().head(10)
        inner = tmp.to_html(index=False, border=0, justify="left", escape=False)
        inner = inner.replace(
            '<table border="0" class="dataframe">',
            '<table style="width:100%; border-collapse:collapse; font-size:10px;">',
        )
        inner = inner.replace(
            '<tr style="text-align: right;">',
            '<tr style="background:#f4f6fb; text-align:left;">',
        )
        inner = inner.replace(
            "<th>",
            "<th style=\"padding:3px 6px; border-bottom:1px solid #e1e4f0; "
            "text-align:left; font-weight:600; color:#555;\">",
        )
        inner = inner.replace(
            "<td>",
            "<td style=\"padding:3px 6px; border-bottom:1px solid #f1f3fa; "
            "text-align:left; color:#333;\">",
        )
        traffic_table_html = inner

    if search_week_df is None or search_week_df.empty:
        search_table_html = "<p style='font-size:11px;color:#999;margin:0;'>데이터 없음</p>"
    else:
        tmp = search_week_df[["키워드", "검색수", "구매수", "CVR(%)"]].copy().head(10)
        inner = tmp.to_html(index=False, border=0, justify="left", escape=False)
        inner = inner.replace(
            '<table border="0" class="dataframe">',
            '<table style="width:100%; border-collapse:collapse; font-size:10px;">',
        )
        inner = inner.replace(
            '<tr style="text-align: right;">',
            '<tr style="background:#f4f6fb; text-align:left;">',
        )
        inner = inner.replace(
            "<th>",
            "<th style=\"padding:3px 6px; border-bottom:1px solid #e1e4f0; "
            "text-align:left; font-weight:600; color:#555;\">",
        )
        inner = inner.replace(
            "<td>",
            "<td style=\"padding:3px 6px; border-bottom:1px solid #f1f3fa; "
            "text-align:left; color:#333;\">",
        )
        search_table_html = inner

    weekly_summary_html = f"""
<div style="font-size:11px; letter-spacing:0.12em; color:#6d7a99; margin-top:20px; margin-bottom:8px;">
  02 · WEEKLY TRAFFIC · CHANNEL · SEARCH
</div>
<table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:separate; border-spacing:8px 10px;">
  <tr>
    <td width="50%" valign="top">
      <table width="100%" cellpadding="0" cellspacing="0"
             style="background:#ffffff; border-radius:12px;
                    border:1px solid #e1e7f5; box-shadow:0 3px 10px rgba(0,0,0,0.03);
                    padding:8px 10px; border-collapse:separate;">
        <tr><td>
          <div style="font-size:11px; font-weight:600; color:#224; margin-bottom:2px;">
            채널별 주간 성과 (Top 10)
          </div>
          <div style="font-size:10px; color:#888; margin-bottom:6px; line-height:1.4;">
            주간 UV · 구매수 · 매출 · CVR 기준 TOP 채널입니다.
          </div>
          {traffic_table_html}
        </td></tr>
      </table>
    </td>
    <td width="50%" valign="top">
      <table width="100%" cellpadding="0" cellspacing="0"
             style="background:#ffffff; border-radius:12px;
                    border:1px solid #e1e7f5; box-shadow:0 3px 10px rgba(0,0,0,0.03);
                    padding:8px 10px; border-collapse:separate;">
        <tr><td>
          <div style="font-size:11px; font-weight:600; color:#224; margin-bottom:2px;">
            온사이트 검색어 주간 TOP 10
          </div>
          <div style="font-size:10px; color:#888; margin-bottom:6px; line-height:1.4;">
            검색수 기준 상위 키워드와 CVR입니다.
          </div>
          {search_table_html}
        </td></tr>
      </table>
    </td>
  </tr>
</table>
"""

    # ---- 본문 HTML (Weekly) ----
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
        <tr>
          <td>

            <!-- 헤더 -->
            <table role="presentation" width="100%" cellspacing="0" cellpadding="0"
                   style="background:#ffffff; border-radius:18px; border:1px solid #e6e9ef; box-shadow:0 6px 18px rgba(0,0,0,0.06);">
              <tr>
                <td valign="top" style="padding:18px 20px 16px 20px;">
                  <div style="font-size:18px; font-weight:700; color:#0055a5; margin-bottom:2px;">
                    COLUMBIA SPORTSWEAR KOREA
                  </div>
                  <div style="font-size:13px; color:#555; margin-bottom:8px;">
                    Weekly eCommerce Performance Digest
                  </div>
                  <span style="display:inline-block; font-size:11px; padding:4px 10px; border-radius:999px;
                               background:#eaf3ff; color:#0055a5; margin-bottom:6px;">
                    {kpi_weekly['date_label']} 기준 (지난주 데이터)
                  </span>
                  <div style="font-size:11px; color:#777; margin-top:6px; margin-bottom:2px; line-height:1.6;">
                    지난 한 주간의 매출·UV·CVR 흐름과 채널·검색 성과를 PPT용으로 한 번에 정리한 요약입니다.
                  </div>
                </td>

                <td valign="top" align="right" style="padding:16px 20px 16px 0%;">
                  <table role="presentation" cellspacing="0" cellpadding="0" align="right" style="margin-bottom:8px;">
                    <tr>
                      <td style="padding:0 3px;">
                        <span style="display:inline-block; font-size:10px; padding:4px 9px; border-radius:999px;
                                     background:#0055a5; color:#ffffff; border:1px solid #0055a5;">
                          WEEKLY
                        </span>
                      </td>
                      <td style="padding:0 3px;">
                        <span style="display:inline-block; font-size:10px; padding:4px 9px; border-radius:999px;
                                     background:#fafbfd; color:#445; border:1px solid #dfe6f3;">
                          KPI
                        </span>
                      </td>
                      <td style="padding:0 3px;">
                        <span style="display:inline-block; font-size:10px; padding:4px 9px; border-radius:999px;
                                     background:#fafbfd; color:#445; border:1px solid #dfe6f3;">
                          CHANNEL
                        </span>
                      </td>
                      <td style="padding:0 3px;">
                        <span style="display:inline-block; font-size:10px; padding:4px 9px; border-radius:999px;
                                     background:#fafbfd; color:#445; border:1px solid #dfe6f3;">
                          SEARCH
                        </span>
                      </td>
                    </tr>
                  </table>
                </td>
              </tr>
            </table>

{insight_action_html}

<!-- 01 KPI (Daily와 동일 레이아웃, 값만 주간 기준) -->
<div style="font-size:11px; letter-spacing:0.12em; color:#6d7a99; margin-top:18px; margin-bottom:10px;">
  01 · WEEKLY KPI SNAPSHOT
</div>

<!-- KPI 9개 카드 (3 x 3) -->
<table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:separate; border-spacing:8px 10px;">
  <tr>
    <!-- 매출 -->
    <td width="33.3%" valign="top">
      <div style="background:#ffffff; border-radius:16px; padding:14px 16px; border:1px solid #e1e7f5;">
        <div style="font-size:11px; color:#777; margin-bottom:4px;">매출 (Revenue)</div>
        <div style="font-size:18px; font-weight:700; margin-bottom:4px;">
          {format_money_manwon(kpi_weekly['revenue_today'])}
        </div>
        <div style="font-size:10px; color:#999; margin-bottom:4px;">
          LD: {format_money_manwon(kpi_weekly['revenue_ld'])} · LW: {format_money_manwon(kpi_weekly['revenue_prev'])} · LY: {format_money_manwon(kpi_weekly['revenue_yoy'])}
        </div>
        <div>
          <span style="display:inline-block; font-size:10px; padding:2px 7px; border-radius:999px; background:#e7f5ec; color:#1b7f4d; margin-right:4px;">
            LD {kpi_weekly['revenue_ld_pct']:+.1f}%
          </span>
          <span style="display:inline-block; font-size:10px; padding:2px 7px; border-radius:999px; background:#dbeafe; color:#1d4ed8; margin-right:4px;">
            LW {kpi_weekly['revenue_lw_pct']:+.1f}%
          </span>
          <span style="display:inline-block; font-size:10px; padding:2px 7px; border-radius:999px; background:#fdeaea; color:#c53030;">
            LY {kpi_weekly['revenue_ly_pct']:+.1f}%
          </span>
        </div>
      </div>
    </td>

    <!-- 방문자수 -->
    <td width="33.3%" valign="top">
      <div style="background:#ffffff; border-radius:16px; padding:14px 16px; border:1px solid #e1e7f5;">
        <div style="font-size:11px; color:#777; margin-bottom:4px;">방문자수 (UV)</div>
        <div style="font-size:18px; font-weight:700; margin-bottom:4px;">
          {kpi_weekly['uv_today']:,}명
        </div>
        <div style="font-size:10px; color:#999; margin-bottom:4px;">
          LD: {kpi_weekly['uv_ld']:,}명 · LW: {kpi_weekly['uv_prev']:,}명 · LY: {kpi_weekly['uv_yoy']:,}명
        </div>
        <div>
          <span style="display:inline-block; font-size:10px; padding:2px 7px; border-radius:999px; background:#e7f5ec; color:#1b7f4d; margin-right:4px;">
            LD {kpi_weekly['uv_ld_pct']:+.1f}%
          </span>
          <span style="display:inline-block; font-size:10px; padding:2px 7px; border-radius:999px; background:#dbeafe; color:#1d4ed8; margin-right:4px;">
            LW {kpi_weekly['uv_lw_pct']:+.1f}%
          </span>
          <span style="display:inline-block; font-size:10px; padding:2px 7px; border-radius:999px; background:#fdeaea; color:#c53030;">
            LY {kpi_weekly['uv_ly_pct']:+.1f}%
          </span>
        </div>
      </div>
    </td>

    <!-- 전환율 -->
    <td width="33.3%" valign="top">
      <div style="background:#ffffff; border-radius:16px; padding:14px 16px; border:1px solid #e1e7f5;">
        <div style="font-size:11px; color:#777; margin-bottom:4px;">전환율 (CVR)</div>
        <div style="font-size:18px; font-weight:700; margin-bottom:4px;">
          {kpi_weekly['cvr_today']:.2f}%
        </div>
        <div style="font-size:10px; color:#999; margin-bottom:4px;">
          LD: {kpi_weekly['cvr_ld']:.2f}% · LW: {kpi_weekly['cvr_prev']:.2f}% · LY: {kpi_weekly['cvr_yoy']:.2f}%
        </div>
        <div>
          <span style="display:inline-block; font-size:10px; padding:2px 7px; border-radius:999px; background:#e7f5ec; color:#1b7f4d; margin-right:4px;">
            LD {kpi_weekly['cvr_ld_pct']:+.1f}p
          </span>
          <span style="display:inline-block; font-size:10px; padding:2px 7px; border-radius:999px; background:#dbeafe; color:#1d4ed8; margin-right:4px;">
            LW {kpi_weekly['cvr_lw_pct']:+.1f}p
          </span>
          <span style="display:inline-block; font-size:10px; padding:2px 7px; border-radius:999px; background:#fdeaea; color:#c53030;">
            LY {kpi_weekly['cvr_ly_pct']:+.1f}p
          </span>
        </div>
      </div>
    </td>
  </tr>
  <tr>
    <!-- 구매수 -->
    <td width="33.3%" valign="top">
      <div style="background:#ffffff; border-radius:16px; padding:14px 16px; border:1px solid #e1e7f5;">
        <div style="font-size:11px; color:#777; margin-bottom:4px;">구매수 (Orders)</div>
        <div style="font-size:18px; font-weight:700; margin-bottom:4px;">
          {kpi_weekly['orders_today']:,}건
        </div>
        <div style="font-size:10px; color:#999; margin-bottom:4px;">
          LD: {kpi_weekly['orders_ld']:,}건 · LW: {kpi_weekly['orders_prev']:,}건 · LY: {kpi_weekly['orders_yoy']:,}건
        </div>
        <div>
          <span style="display:inline-block; font-size:10px; padding:2px 7px; border-radius:999px; background:#e7f5ec; color:#1b7f4d; margin-right:4px;">
            LD {kpi_weekly['orders_ld_pct']:+.1f}%
          </span>
          <span style="display:inline-block; font-size:10px; padding:2px 7px; border-radius:999px; background:#dbeafe; color:#1d4ed8; margin-right:4px;">
            LW {kpi_weekly['orders_lw_pct']:+.1f}%
          </span>
          <span style="display:inline-block; font-size:10px; padding:2px 7px; border-radius:999px; background:#fdeaea; color:#c53030;">
            LY {kpi_weekly['orders_ly_pct']:+.1f}%
          </span>
        </div>
      </div>
    </td>

    <!-- 객단가 -->
    <td width="33.3%" valign="top">
      <div style="background:#ffffff; border-radius:16px; padding:14px 16px; border:1px solid #e1e7f5;">
        <div style="font-size:11px; color:#777; margin-bottom:4px;">객단가 (AOV)</div>
        <div style="font-size:18px; font-weight:700; margin-bottom:4px;">
          {format_money(kpi_weekly['aov_today'])}
        </div>
        <div style="font-size:10px; color:#999; margin-bottom:4px;">
          LD: {format_money(kpi_weekly['aov_ld'])} · LW: {format_money(kpi_weekly['aov_prev'])} · LY: {format_money(kpi_weekly['aov_yoy'])}
        </div>
        <div>
          <span style="display:inline-block; font-size:10px; padding:2px 7px; border-radius:999px; background:#e7f5ec; color:#1b7f4d; margin-right:4px;">
            LD {kpi_weekly['aov_ld_pct']:+.1f}%
          </span>
          <span style="display:inline-block; font-size:10px; padding:2px 7px; border-radius:999px; background:#dbeafe; color:#1d4ed8; margin-right:4px;">
            LW {kpi_weekly['aov_lw_pct']:+.1f}%
          </span>
          <span style="display:inline-block; font-size:10px; padding:2px 7px; border-radius:999px; background:#fdeaea; color:#c53030;">
            LY {kpi_weekly['aov_ly_pct']:+.1f}%
          </span>
        </div>
      </div>
    </td>

    <!-- 신규 방문자 -->
    <td width="33.3%" valign="top">
      <div style="background:#ffffff; border-radius:16px; padding:14px 16px; border:1px solid #e1e7f5;">
        <div style="font-size:11px; color:#777; margin-bottom:4px;">신규 방문자 (New Visitors)</div>
        <div style="font-size:18px; font-weight:700; margin-bottom:4px;">
          {kpi_weekly['new_today']:,}명
        </div>
        <div style="font-size:10px; color:#999; margin-bottom:4px;">
          LD: {kpi_weekly['new_ld']:,}명 · LW: {kpi_weekly['new_prev']:,}명 · LY: {kpi_weekly['new_yoy']:,}명
        </div>
        <div>
          <span style="display:inline-block; font-size:10px; padding:2px 7px; border-radius:999px; background:#e7f5ec; color:#1b7f4d; margin-right:4px;">
            LD {kpi_weekly['new_ld_pct']:+.1f}%
          </span>
          <span style="display:inline-block; font-size:10px; padding:2px 7px; border-radius:999px; background:#dbeafe; color:#1d4ed8; margin-right:4px;">
            LW {kpi_weekly['new_lw_pct']:+.1f}%
          </span>
          <span style="display:inline-block; font-size:10px; padding:2px 7px; border-radius:999px; background:#fdeaea; color:#c53030;">
            LY {kpi_weekly['new_ly_pct']:+.1f}%
          </span>
        </div>
      </div>
    </td>
  </tr>
  <tr>
    <!-- 오가닉 UV -->
    <td width="33.3%" valign="top">
      <div style="background:#ffffff; border-radius:16px; padding:14px 16px; border:1px solid #e1e7f5;">
        <div style="font-size:11px; color:#777; margin-bottom:4px;">오가닉 UV (Organic Search)</div>
        <div style="font-size:18px; font-weight:700; margin-bottom:4px;">
          {kpi_weekly['organic_uv_today']:,}명
        </div>
        <div style="font-size:10px; color:#999; margin-bottom:4px;">
          LD: {kpi_weekly['organic_uv_ld']:,}명 · LW: {kpi_weekly['organic_uv_prev']:,}명 · LY: {kpi_weekly['organic_uv_yoy']:,}명
        </div>
        <div>
          <span style="display:inline-block; font-size:10px; padding:2px 7px; border-radius:999px; background:#e7f5ec; color:#1b7f4d; margin-right:4px;">
            LD {kpi_weekly['organic_uv_ld_pct']:+.1f}%
          </span>
          <span style="display:inline-block; font-size:10px; padding:2px 7px; border-radius:999px; background:#dbeafe; color:#1d4ed8; margin-right:4px;">
            LW {kpi_weekly['organic_uv_lw_pct']:+.1f}%
          </span>
          <span style="display:inline-block; font-size:10px; padding:2px 7px; border-radius:999px; background:#fdeaea; color:#c53030;">
            LY {kpi_weekly['organic_uv_ly_pct']:+.1f}%
          </span>
        </div>
      </div>
    </td>

    <!-- 비오가닉 UV -->
    <td width="33.3%" valign="top">
      <div style="background:#ffffff; border-radius:16px; padding:14px 16px; border:1px solid #e1e7f5;">
        <div style="font-size:11px; color:#777; margin-bottom:4px;">비오가닉 UV (Non-organic)</div>
        <div style="font-size:18px; font-weight:700; margin-bottom:4px;">
          {kpi_weekly['nonorganic_uv_today']:,}명
        </div>
        <div style="font-size:10px; color:#999; margin-bottom:4px;">
          LD: {kpi_weekly['nonorganic_uv_ld']:,}명 · LW: {kpi_weekly['nonorganic_uv_prev']:,}명 · LY: {kpi_weekly['nonorganic_uv_yoy']:,}명
        </div>
        <div>
          <span style="display:inline-block; font-size:10px; padding:2px 7px; border-radius:999px; background:#e7f5ec; color:#1b7f4d; margin-right:4px;">
            LD {kpi_weekly['nonorganic_uv_ld_pct']:+.1f}%
          </span>
          <span style="display:inline-block; font-size:10px; padding:2px 7px; border-radius:999px; background:#dbeafe; color:#1d4ed8; margin-right:4px;">
            LW {kpi_weekly['nonorganic_uv_lw_pct']:+.1f}%
          </span>
          <span style="display:inline-block; font-size:10px; padding:2px 7px; border-radius:999px; background:#fdeaea; color:#c53030;">
            LY {kpi_weekly['nonorganic_uv_ly_pct']:+.1f}%
          </span>
        </div>
      </div>
    </td>

    <!-- 오가닉 UV 비중 -->
    <td width="33.3%" valign="top">
      <div style="background:#ffffff; border-radius:16px; padding:14px 16px; border:1px solid #e1e7f5;">
        <div style="font-size:11px; color:#777; margin-bottom:4px;">오가닉 UV 비중 (Share)</div>
        <div style="font-size:18px; font-weight:700; margin-bottom:4px;">
          {kpi_weekly['organic_share_today']:.1f}%
        </div>
        <div style="font-size:10px; color:#999; margin-bottom:4px;">
          LD: {kpi_weekly['organic_share_ld']:.1f}% · LW: {kpi_weekly['organic_share_prev']:.1f}% · LY: {kpi_weekly['organic_share_yoy']:.1f}%
        </div>
        <div>
          <span style="display:inline-block; font-size:10px; padding:2px 7px; border-radius:999px; background:#e7f5ec; color:#1b7f4d; margin-right:4px;">
            LD {kpi_weekly['organic_share_ld_pct']:+.1f}p
          </span>
          <span style="display:inline-block; font-size:10px; padding:2px 7px; border-radius:999px; background:#dbeafe; color:#1d4ed8; margin-right:4px;">
            LW {kpi_weekly['organic_share_lw_pct']:+.1f}p
          </span>
          <span style="display:inline-block; font-size:10px; padding:2px 7px; border-radius:999px; background:#fdeaea; color:#c53030;">
            LY {kpi_weekly['organic_share_ly_pct']:+.1f}p
          </span>
        </div>
      </div>
    </td>
  </tr>
</table>

{weekly_summary_html}

<div style="margin-top:18px; font-size:10px; color:#99a; text-align:right;">
  Columbia Sportswear Korea · Weekly eCommerce Digest · GA4 · Python
</div>

          </td>
        </tr>
      </table>
    </td>
  </tr>
</table>

</body>
</html>
"""
    return html


# =====================================================================
# 7) Weekly Digest 실행 함수
# =====================================================================

def send_weekly_digest():
    ranges = get_last_week_ranges()
    (this_start, this_end) = ranges["this"]

    print(f"[INFO] Weekly digest range: {this_start} ~ {this_end}")

    kpi_weekly = build_weekly_kpi()
    traffic_week_df = src_traffic_range(this_start, this_end)
    search_week_df = src_search_range(this_start, this_end, limit=200)

    html = compose_html_weekly(kpi_weekly, traffic_week_df, search_week_df)

    subject = f"[COLUMBIA] Weekly eCommerce Digest — {kpi_weekly['date_label']}"
    send_email_html(subject, html, WEEKLY_RECIPIENTS)


# =====================================================================
# 8) main
# =====================================================================

if __name__ == "__main__":
    send_weekly_digest()

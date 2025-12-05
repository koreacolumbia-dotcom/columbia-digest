#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Columbia Sportswear Korea
Weekly eCommerce Performance Digest (GA4 + HTML Mail)

요약
- GA4 기반 주간 KPI / 퍼널 / 채널 / 상품 / 검색 / 그래프 & 액션 리포트
- HTML 메일 형태로 자동 생성 + 발송
- 구조:
  01. THIS WEEK INSIGHT (Insight vs Action 2단 컬럼)
  02. WEEKLY KPI SNAPSHOT (9개 KPI 카드 · 3x3 Grid)
  03. FUNNEL · TRAFFIC · PRODUCT · SEARCH (표 기반 요약)
  04. GRAPH & ANALYSIS (이번주 vs 전주 비교 + 자동 해석)

환경 변수
- GA4_PROPERTY_ID
- GA4_SERVICE_ACCOUNT_JSON (JSON 문자열)  # 로컬 json 파일 X
- SMTP_PROVIDER, SMTP_HOST, SMTP_PORT, SMTP_USER, SMTP_PASS
- WEEKLY_RECIPIENTS (쉼표 구분 이메일 리스트)
- ALERT_RECIPIENT (선택)
- DIGEST_INLINE_JPEG = "1" → 전체 HTML을 JPEG로 캡쳐해 인라인 이미지 발송 옵션
"""

import os
import smtplib
from datetime import datetime, timedelta
from typing import Dict, Tuple, List

import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from google.oauth2 import service_account


# =====================================================================
# 0) 환경 변수 / 기본 설정
# =====================================================================

GA4_PROPERTY_ID = os.getenv("GA4_PROPERTY_ID", "").strip()

SMTP_PROVIDER = os.getenv("SMTP_PROVIDER", "gmail").lower()  # "gmail" or "outlook"
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")

WEEKLY_RECIPIENTS = [
    e.strip()
    for e in os.getenv("WEEKLY_RECIPIENTS", "").split(",")
    if e.strip()
]

ALERT_RECIPIENT = os.getenv("ALERT_RECIPIENT", "").strip()

ENABLE_INLINE_JPEG = os.getenv("DIGEST_INLINE_JPEG", "0") == "1"
HTML_SCREENSHOT_WIDTH = int(os.getenv("DIGEST_IMG_WIDTH", "1200"))

SERVICE_ACCOUNT_JSON = os.getenv("GA4_SERVICE_ACCOUNT_JSON", "")

if SERVICE_ACCOUNT_JSON:
    SERVICE_ACCOUNT_FILE = "/tmp/ga4_service_account.json"
    with open(SERVICE_ACCOUNT_FILE, "w", encoding="utf-8") as f:
        f.write(SERVICE_ACCOUNT_JSON)
else:
    SERVICE_ACCOUNT_FILE = os.getenv("GA4_SERVICE_ACCOUNT_FILE", "")


# =====================================================================
# 1) 유틸 함수
# =====================================================================

def pct_change(curr, prev) -> float:
    """(curr - prev) / prev * 100 (%). prev가 0이면 0."""
    try:
        prev = float(prev)
        curr = float(curr)
        if prev == 0:
            return 0.0
        return round((curr - prev) / prev * 100, 1)
    except Exception:
        return 0.0


def safe_int(x) -> int:
    try:
        return int(float(x))
    except Exception:
        return 0


def safe_float(x) -> float:
    try:
        return float(x)
    except Exception:
        return 0.0


def format_money(won) -> str:
    w = round(safe_float(won))
    return f"{w:,}원"


def format_money_manwon(won) -> str:
    man = round(safe_float(won) / 10_000)
    return f"{man:,}만원"


def _smtp_server_and_port() -> Tuple[str, int]:
    if SMTP_PROVIDER == "gmail":
        return ("smtp.gmail.com", 587)
    elif SMTP_PROVIDER == "outlook":
        return ("smtp.office365.com", 587)
    return (SMTP_HOST, SMTP_PORT)


def html_to_jpeg(html_body: str, out_path: str = "/tmp/columbia_weekly_digest.jpg") -> str:
    """
    HTML 문자열을 JPEG 이미지로 변환 (pyppeteer + headless Chromium).
    DIGEST_INLINE_JPEG = "1" 일 때만 동작. 실패하면 그냥 HTML만 보냄.
    """
    if not ENABLE_INLINE_JPEG:
        return ""

    try:
        from pyppeteer import launch
        import asyncio
    except Exception:
        print("[WARN] pyppeteer 미설치 – HTML만 발송합니다.")
        return ""

    async def _capture():
        browser = await launch(headless=True, args=["--no-sandbox"])
        page = await browser.newPage()
        await page.setViewport({"width": HTML_SCREENSHOT_WIDTH, "height": 1600})
        await page.setContent(html_body, waitUntil="networkidle0")
        await page.screenshot(path=out_path, fullPage=True, type="jpeg", quality=95)
        await browser.close()

    try:
        try:
            loop = asyncio.get_event_loop()
        except RuntimeError:
            import asyncio as _asyncio
            loop = _asyncio.new_event_loop()
            _asyncio.set_event_loop(loop)
        loop.run_until_complete(_capture())
        print(f"[INFO] HTML→JPEG 변환 완료: {out_path}")
        return out_path
    except Exception as e:
        print("[WARN] HTML→JPEG 변환 실패:", e)
        return ""


def send_email_html(subject: str, html_body: str, recipients, jpeg_path: str = ""):
    """HTML 또는 JPEG 버전을 메일로 발송."""
    if isinstance(recipients, str):
        recipients = [recipients]

    if not recipients:
        print("[WARN] WEEKLY_RECIPIENTS 비어 있음 – 메일 발송 생략.")
        print("[DEBUG] HTML Preview (truncate):")
        print(html_body[:3000])
        return

    if not (SMTP_USER and SMTP_PASS):
        print("[WARN] SMTP_USER/SMTP_PASS 없음 – 메일 대신 HTML 프리뷰만 출력.")
        print(html_body[:3000])
        return

    host, port = _smtp_server_and_port()

    msg = MIMEMultipart("related")
    msg["Subject"] = subject
    msg["From"] = SMTP_USER
    msg["To"] = ", ".join(recipients)

    alt = MIMEMultipart("alternative")
    msg.attach(alt)

    plain_text = "Columbia eCommerce Weekly Digest 입니다. 메일이 깨질 경우 이미지를 확인해주세요."
    alt.attach(MIMEText(plain_text, "plain", "utf-8"))

    if jpeg_path and os.path.exists(jpeg_path):
        html_body_effective = f"""<html><body style='margin:0; padding:0; background:#f4f6fb;'>
<div style='width:100%; text-align:center; padding:16px 0;'>
  <img src="cid:digest_image" alt="Columbia Weekly eCommerce Digest"
       style="max-width:100%; height:auto; border:0; display:block; margin:0 auto;" />
</div>
</body></html>"""
    else:
        html_body_effective = html_body

    alt.attach(MIMEText(html_body_effective, "html", "utf-8"))

    if jpeg_path and os.path.exists(jpeg_path):
        with open(jpeg_path, "rb") as f:
            img = MIMEImage(f.read(), _subtype="jpeg")
        img.add_header("Content-ID", "<digest_image>")
        img.add_header("Content-Disposition", "inline", filename=os.path.basename(jpeg_path))
        msg.attach(img)

    with smtplib.SMTP(host, port) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASS)
        server.sendmail(SMTP_USER, recipients, msg.as_string())


def send_critical_alert(subject: str, body_text: str):
    recipient = ALERT_RECIPIENT or SMTP_USER or ""
    if not recipient:
        print("[WARN] ALERT_RECIPIENT/SMTP_USER 없음 – 긴급 알림 생략:", subject)
        return
    html = f"<pre style='font-family:monospace; white-space:pre-wrap'>{body_text}</pre>"
    send_email_html(subject, html, [recipient])


# =====================================================================
# 2) GA4 Client & 공통 run_report
# =====================================================================

def ga_client() -> BetaAnalyticsDataClient:
    if not GA4_PROPERTY_ID:
        raise SystemExit("GA4_PROPERTY_ID가 비어 있습니다.")
    if not SERVICE_ACCOUNT_FILE or not os.path.exists(SERVICE_ACCOUNT_FILE):
        raise SystemExit(f"서비스 계정 파일을 찾을 수 없습니다: {SERVICE_ACCOUNT_FILE}")

    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/analytics.readonly"],
    )
    return BetaAnalyticsDataClient(credentials=creds)


def ga_run_report(dimensions, metrics, start_date, end_date, limit=None, order_bys=None) -> pd.DataFrame:
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
# 3) 주간 날짜 범위 계산
# =====================================================================

def get_week_ranges() -> Dict[str, Dict[str, str]]:
    """
    오늘 기준:
    - this_week: 직전 7일 (오늘 제외) → '이번주(직전 주)'
    - last_week: 직전 주의 바로 전 주 (그 이전 7일)
    - last_year: 1년 전 동일 기간
    """
    today = datetime.utcnow().date()
    this_end = today - timedelta(days=1)
    this_start = today - timedelta(days=7)

    last_end = this_start - timedelta(days=1)
    last_start = last_end - timedelta(days=6)

    ly_start = this_start - timedelta(days=365)
    ly_end = this_end - timedelta(days=365)

    return {
        "this": {
            "start": this_start.strftime("%Y-%m-%d"),
            "end": this_end.strftime("%Y-%m-%d"),
        },
        "last": {
            "start": last_start.strftime("%Y-%m-%d"),
            "end": last_end.strftime("%Y-%m-%d"),
        },
        "ly": {
            "start": ly_start.strftime("%Y-%m-%d"),
            "end": ly_end.strftime("%Y-%m-%d"),
        },
        "label": f"{this_start.strftime('%Y-%m-%d')} ~ {this_end.strftime('%Y-%m-%d')}",
    }


# =====================================================================
# 4) 데이터 소스 (주간 GA4)
# =====================================================================

def src_weekly_kpi(start_date: str, end_date: str) -> Dict[str, float]:
    df = ga_run_report(
        dimensions=[],
        metrics=["sessions", "transactions", "purchaseRevenue", "newUsers"],
        start_date=start_date,
        end_date=end_date,
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


def _weekly_channel_uv(start_date: str, end_date: str) -> Dict[str, float]:
    df = ga_run_report(
        dimensions=["sessionDefaultChannelGroup"],
        metrics=["sessions"],
        start_date=start_date,
        end_date=end_date,
    )
    if df.empty:
        return {
            "total_uv": 0,
            "organic_uv": 0,
            "nonorganic_uv": 0,
            "organic_share": 0.0,
        }
    df["sessions"] = pd.to_numeric(df["sessions"], errors="coerce").fillna(0).astype(int)
    total_uv = int(df["sessions"].sum())
    organic_uv = int(df.loc[df["sessionDefaultChannelGroup"] == "Organic Search", "sessions"].sum())
    nonorganic_uv = total_uv - organic_uv
    organic_share = (organic_uv / total_uv * 100) if total_uv > 0 else 0.0
    return {
        "total_uv": total_uv,
        "organic_uv": organic_uv,
        "nonorganic_uv": nonorganic_uv,
        "organic_share": round(organic_share, 1),
    }


def build_weekly_kpi() -> Dict[str, float]:
    """
    주간 KPI 9개 카드용 데이터 구조.
    1. Revenue
    2. UV
    3. CVR
    4. Orders
    5. AOV
    6. New Users
    7. Organic UV
    8. Non-organic UV
    9. Organic Share
    """
    ranges = get_week_ranges()
    w_this = src_weekly_kpi(ranges["this"]["start"], ranges["this"]["end"])
    w_last = src_weekly_kpi(ranges["last"]["start"], ranges["last"]["end"])
    w_ly = src_weekly_kpi(ranges["ly"]["start"], ranges["ly"]["end"])

    ch_this = _weekly_channel_uv(ranges["this"]["start"], ranges["this"]["end"])
    ch_last = _weekly_channel_uv(ranges["last"]["start"], ranges["last"]["end"])
    ch_ly = _weekly_channel_uv(ranges["ly"]["start"], ranges["ly"]["end"])

    rev_this = w_this["purchaseRevenue"]
    rev_last = w_last["purchaseRevenue"]
    rev_ly = w_ly["purchaseRevenue"]

    uv_this = w_this["sessions"]
    uv_last = w_last["sessions"]
    uv_ly = w_ly["sessions"]

    ord_this = w_this["transactions"]
    ord_last = w_last["transactions"]
    ord_ly = w_ly["transactions"]

    new_this = w_this["newUsers"]
    new_last = w_last["newUsers"]
    new_ly = w_ly["newUsers"]

    cvr_this = (ord_this / uv_this * 100) if uv_this else 0.0
    cvr_last = (ord_last / uv_last * 100) if uv_last else 0.0
    cvr_ly = (ord_ly / uv_ly * 100) if uv_ly else 0.0

    aov_this = (rev_this / ord_this) if ord_this else 0.0
    aov_last = (rev_last / ord_last) if ord_last else 0.0
    aov_ly = (rev_ly / ord_ly) if ord_ly else 0.0

    organic_uv_this = ch_this["organic_uv"]
    organic_uv_last = ch_last["organic_uv"]
    organic_uv_ly = ch_ly["organic_uv"]

    nonorganic_uv_this = ch_this["nonorganic_uv"]
    nonorganic_uv_last = ch_last["nonorganic_uv"]
    nonorganic_uv_ly = ch_ly["nonorganic_uv"]

    organic_share_this = ch_this["organic_share"]
    organic_share_last = ch_last["organic_share"]
    organic_share_ly = ch_ly["organic_share"]

    kpi = {
        "week_label": ranges["label"],

        # 매출
        "revenue_this": rev_this,
        "revenue_last": rev_last,
        "revenue_ly": rev_ly,
        "revenue_lw_pct": pct_change(rev_this, rev_last),
        "revenue_ly_pct": pct_change(rev_this, rev_ly),

        # UV
        "uv_this": uv_this,
        "uv_last": uv_last,
        "uv_ly": uv_ly,
        "uv_lw_pct": pct_change(uv_this, uv_last),
        "uv_ly_pct": pct_change(uv_this, uv_ly),

        # 주문수
        "orders_this": ord_this,
        "orders_last": ord_last,
        "orders_ly": ord_ly,
        "orders_lw_pct": pct_change(ord_this, ord_last),
        "orders_ly_pct": pct_change(ord_this, ord_ly),

        # CVR
        "cvr_this": round(cvr_this, 2),
        "cvr_last": round(cvr_last, 2),
        "cvr_ly": round(cvr_ly, 2),
        "cvr_lw_pct": pct_change(cvr_this, cvr_last),  # 단위: p로 해석
        "cvr_ly_pct": pct_change(cvr_this, cvr_ly),

        # AOV
        "aov_this": aov_this,
        "aov_last": aov_last,
        "aov_ly": aov_ly,
        "aov_lw_pct": pct_change(aov_this, aov_last),
        "aov_ly_pct": pct_change(aov_this, aov_ly),

        # 신규
        "new_this": new_this,
        "new_last": new_last,
        "new_ly": new_ly,
        "new_lw_pct": pct_change(new_this, new_last),
        "new_ly_pct": pct_change(new_this, new_ly),

        # 오가닉 UV
        "organic_uv_this": organic_uv_this,
        "organic_uv_last": organic_uv_last,
        "organic_uv_ly": organic_uv_ly,
        "organic_uv_lw_pct": pct_change(organic_uv_this, organic_uv_last),
        "organic_uv_ly_pct": pct_change(organic_uv_this, organic_uv_ly),

        # 비오가닉 UV
        "nonorganic_uv_this": nonorganic_uv_this,
        "nonorganic_uv_last": nonorganic_uv_last,
        "nonorganic_uv_ly": nonorganic_uv_ly,
        "nonorganic_uv_lw_pct": pct_change(nonorganic_uv_this, nonorganic_uv_last),
        "nonorganic_uv_ly_pct": pct_change(nonorganic_uv_this, nonorganic_uv_ly),

        # 오가닉 비중
        "organic_share_this": organic_share_this,
        "organic_share_last": organic_share_last,
        "organic_share_ly": organic_share_ly,
        "organic_share_lw_pct": pct_change(organic_share_this, organic_share_last),
        "organic_share_ly_pct": pct_change(organic_share_this, organic_share_ly),
    }
    return kpi


def src_weekly_funnel(start_date: str, end_date: str) -> pd.DataFrame:
    """주간 이벤트 view_item / add_to_cart / begin_checkout / purchase 집계."""
    df = ga_run_report(
        dimensions=["eventName"],
        metrics=["eventCount"],
        start_date=start_date,
        end_date=end_date,
    )
    want = ["view_item", "add_to_cart", "begin_checkout", "purchase"]
    df = df[df["eventName"].isin(want)].copy()
    df.rename(columns={"eventName": "단계", "eventCount": "수"}, inplace=True)
    order = {k: i for i, k in enumerate(want)}
    df["ord"] = df["단계"].map(order)
    df = df.sort_values("ord").drop(columns=["ord"])
    return df


def build_weekly_funnel_comparison() -> Tuple[pd.DataFrame, pd.DataFrame]:
    """이번주 vs 전주 퍼널 전환율 비교 테이블 2개 생성."""
    ranges = get_week_ranges()
    df_this = src_weekly_funnel(ranges["this"]["start"], ranges["this"]["end"])
    df_last = src_weekly_funnel(ranges["last"]["start"], ranges["last"]["end"])

    def _extract_counts(df):
        base = df.set_index("단계")["수"]
        return (
            base.get("view_item", 0),
            base.get("add_to_cart", 0),
            base.get("begin_checkout", 0),
            base.get("purchase", 0),
        )

    view_t, cart_t, chk_t, buy_t = _extract_counts(df_this)
    view_l, cart_l, chk_l, buy_l = _extract_counts(df_last)

    def _rate(a, b):
        try:
            if b == 0:
                return 0.0
            return round(a / b * 100, 1)
        except Exception:
            return 0.0

    rows = []
    for name, vt, ct, vl, cl in [
        ("상품 상세 → 장바구니", cart_t, view_t, cart_l, view_l),
        ("장바구니 → 체크아웃", chk_t, cart_t, chk_l, cart_l),
        ("체크아웃 → 결제완료", buy_t, chk_t, buy_l, chk_l),
    ]:
        this_conv = _rate(vt, ct)
        last_conv = _rate(vl, cl)
        diff = round(this_conv - last_conv, 1)
        rows.append(
            {
                "구간": name,
                "이번주 전환율(%)": this_conv,
                "전주 전환율(%)": last_conv,
                "변화(ppt)": diff,
            }
        )

    funnel_compare_df = pd.DataFrame(rows)

    # 원시 이벤트 카운트
    raw_df = df_this.rename(columns={"수": "이번주 수"}).merge(
        df_last.rename(columns={"수": "전주 수"}),
        on="단계",
        how="outer",
    ).fillna(0)

    return raw_df, funnel_compare_df


def src_weekly_traffic(start_date: str, end_date: str) -> pd.DataFrame:
    df = ga_run_report(
        dimensions=["sessionDefaultChannelGroup"],
        metrics=["sessions", "transactions", "purchaseRevenue", "newUsers"],
        start_date=start_date,
        end_date=end_date,
    )
    if df.empty:
        return pd.DataFrame(columns=["채널", "UV", "구매수", "매출(만원)", "CVR(%)", "신규"])

    df = df.rename(
        columns={
            "sessionDefaultChannelGroup": "채널",
            "sessions": "UV",
            "transactions": "구매수",
            "purchaseRevenue": "매출(원)",
            "newUsers": "신규",
        }
    )
    df["UV"] = pd.to_numeric(df["UV"], errors="coerce").fillna(0).astype(int)
    df["구매수"] = pd.to_numeric(df["구매수"], errors="coerce").fillna(0).astype(int)
    df["매출(원)"] = pd.to_numeric(df["매출(원)"], errors="coerce").fillna(0.0)
    df["매출(만원)"] = (df["매출(원)"] / 10_000).round(1)
    df["CVR(%)"] = (df["구매수"] / df["UV"] * 100).replace([float("inf")], 0).round(2)
    df = df.sort_values("매출(원)", ascending=False)
    return df[["채널", "UV", "구매수", "매출(만원)", "CVR(%)", "신규"]]


def src_weekly_search(start_date: str, end_date: str, limit: int = 50) -> pd.DataFrame:
    df = ga_run_report(
        dimensions=["searchTerm"],
        metrics=["eventCount", "transactions"],
        start_date=start_date,
        end_date=end_date,
        limit=limit,
    )
    if df.empty:
        return pd.DataFrame(columns=["키워드", "검색수", "구매수", "CVR(%)"])

    df = df.rename(
        columns={
            "searchTerm": "키워드",
            "eventCount": "검색수",
            "transactions": "구매수",
        }
    )
    df["검색수"] = pd.to_numeric(df["검색수"], errors="coerce").fillna(0).astype(int)
    df["구매수"] = pd.to_numeric(df["구매수"], errors="coerce").fillna(0).astype(int)
    df["CVR(%)"] = (df["구매수"] / df["검색수"] * 100).replace([float("inf")], 0).round(2)
    df = df.sort_values("검색수", ascending=False)
    return df


def src_weekly_products(start_date: str, end_date: str, limit: int = 100) -> pd.DataFrame:
    """주간 상품 기준 구매/매출."""
    base = ga_run_report(
        dimensions=["itemName"],
        metrics=["itemsPurchased", "itemRevenue"],
        start_date=start_date,
        end_date=end_date,
        limit=limit,
    )
    if base.empty:
        return pd.DataFrame(columns=["상품명", "구매수", "매출(만원)"])

    base = base.rename(
        columns={
            "itemName": "상품명",
            "itemsPurchased": "구매수",
            "itemRevenue": "매출(원)",
        }
    )
    base["구매수"] = pd.to_numeric(base["구매수"], errors="coerce").fillna(0).astype(int)
    base["매출(원)"] = pd.to_numeric(base["매출(원)"], errors="coerce").fillna(0.0)
    base["매출(만원)"] = (base["매출(원)"] / 10_000).round(1)
    base = base.sort_values("매출(원)", ascending=False).head(limit)
    return base[["상품명", "구매수", "매출(만원)"]]


def build_channel_mix(df_this: pd.DataFrame, df_last: pd.DataFrame) -> pd.DataFrame:
    """이번주 vs 전주 채널 매출 비중 비교."""
    if df_this is None or df_this.empty:
        return pd.DataFrame(columns=["채널", "이번주 비중(%)", "전주 비중(%)", "변화(ppt)"])

    this = df_this.copy()
    last = df_last.copy() if df_last is not None else pd.DataFrame(columns=this.columns)

    this["매출(원)"] = this["매출(만원)"] * 10_000
    if not last.empty:
        last["매출(원)"] = last["매출(만원)"] * 10_000
    else:
        last["매출(원)"] = 0

    t_sum = this["매출(원)"].sum() or 1
    l_sum = last["매출(원)"].sum() or 1

    this["이번주 비중(%)"] = (this["매출(원)"] / t_sum * 100).round(1)
    last = last.set_index("채널")

    rows = []
    for _, row in this.iterrows():
        ch = row["채널"]
        this_share = safe_float(row["이번주 비중(%)"])
        if ch in last.index:
            last_share = safe_float(last.loc[ch, "매출(원)"] / l_sum * 100)
        else:
            last_share = 0.0
        diff = round(this_share - last_share, 1)
        rows.append(
            {
                "채널": ch,
                "이번주 비중(%)": this_share,
                "전주 비중(%)": round(last_share, 1),
                "변화(ppt)": diff,
            }
        )
    mix_df = pd.DataFrame(rows).sort_values("이번주 비중(%)", ascending=False)
    return mix_df


# =====================================================================
# 5) 인사이트 & 액션 텍스트 생성
# =====================================================================

def build_weekly_insight_paragraph(
    kpi: Dict[str, float],
    funnel_compare_df: pd.DataFrame,
    traffic_this: pd.DataFrame,
    search_this: pd.DataFrame,
) -> str:
    """
    01 · THIS WEEK INSIGHT
    - 숫자 요약이 아니라 의미 해석 중심
    - 원인 → 영향 → 시사점 구조 문단
    """
    rev_pct = kpi["revenue_lw_pct"]
    uv_pct = kpi["uv_lw_pct"]
    cvr_pct = kpi["cvr_lw_pct"]
    aov_pct = kpi["aov_lw_pct"]
    new_pct = kpi["new_lw_pct"]

    # KPI 흐름 요약
    if rev_pct > 0:
        p1 = (
            f"이번 주 매출은 전주 대비 {rev_pct:+.1f}% 증가했고, UV는 {uv_pct:+.1f}%, "
            f"CVR은 {cvr_pct:+.1f}p 수준의 변동을 보였습니다. "
        )
    else:
        p1 = (
            f"이번 주 매출은 전주 대비 {rev_pct:+.1f}% 감소했고, UV {uv_pct:+.1f}%, "
            f"CVR {cvr_pct:+.1f}p 조정이 함께 나타났습니다. "
        )
    p1 += (
        f"객단가(AOV)는 {aov_pct:+.1f}% 수준으로 변동되었고, 신규 유입 규모는 전주 대비 {new_pct:+.1f}% 변했습니다. "
    )

    # 퍼널 진단
    if funnel_compare_df is not None and not funnel_compare_df.empty:
        worst = funnel_compare_df.sort_values("변화(ppt)").iloc[0]
        if worst["변화(ppt)"] < 0:
            p2 = (
                f"퍼널에서는 '{worst['구간']}' 구간 전환율이 전주 대비 {worst['변화(ppt)']:+.1f}p 악화되어, "
                "장바구니 진입·체크아웃·결제 과정에서의 UX 또는 혜택 구조 점검이 필요해 보입니다. "
            )
        else:
            p2 = (
                f"퍼널에서는 '{worst['구간']}' 구간 전환율이 전주 대비 {worst['변화(ppt)']:+.1f}p 개선되며, "
                "전반적으로 이탈이 완만한 한 주였습니다. "
            )
    else:
        p2 = (
            "퍼널 데이터는 이번 주 기준으로 유의미한 전주 대비 비교가 어려워, "
            "우선 상단 KPI 중심으로 흐름을 해석했습니다. "
        )

    # 채널/검색/상품 믹스
    if traffic_this is not None and not traffic_this.empty:
        top_ch = traffic_this.iloc[0]
        p3 = (
            f"채널 믹스 관점에서는 '{top_ch['채널']}' 채널이 매출 비중과 CVR 측면에서 가장 큰 영향력을 갖고 있으며, "
            "오가닉/페이드 트래픽의 균형이 이번 주 실적에 직접적으로 연결되었습니다. "
        )
    else:
        p3 = (
            "채널 데이터가 충분하지 않아, 이번 주에는 전체 UV·CVR 흐름을 우선적으로 모니터링해야 합니다. "
        )

    if search_this is not None and not search_this.empty:
        low_cvr = search_this[search_this["CVR(%)"] < 1.0]
        if not low_cvr.empty:
            kw_list = ", ".join(low_cvr.head(3)["키워드"].tolist())
            p4 = (
                f"온사이트 검색에서는 검색량은 많지만 CVR이 낮은 키워드({kw_list} 등)가 확인되어, "
                "검색 결과 페이지 구성·가격 포지셔닝·프로모션 연계에 대한 개선 여지가 있습니다. "
            )
        else:
            p4 = (
                "온사이트 검색 상위 키워드들은 대체로 안정적인 CVR을 보이고 있어, "
                "상위 검색어 기반 기획전 및 추천 영역 확장에 더 많은 리소스를 배분할 수 있는 상태입니다. "
            )
    else:
        p4 = (
            "검색 데이터는 이번 주 기준으로 노이즈가 커, 상위 키워드 중심으로만 추세를 확인하는 수준으로 활용하는 것이 적절합니다. "
        )

    # 시사점
    if rev_pct < 0 and uv_pct < 0:
        p5 = (
            "종합하면 상단 유입과 매출이 함께 눌린 국면으로, 신규 유입 확대와 장바구니·체크아웃 구간의 전환율 개선이 "
            "다음 주 우선 실행 과제가 됩니다."
        )
    elif cvr_pct < 0 <= uv_pct:
        p5 = (
            "유입은 늘었지만 CVR이 떨어진 한 주였기 때문에, 유입 품질·랜딩 페이지·퍼널 UX에 대한 정교한 실험 설계가 "
            "필수적입니다."
        )
    else:
        p5 = (
            "이번 주는 전반적으로 안정적인 성과를 유지한 만큼, 퍼포먼스가 좋은 채널·키워드·상품을 기준으로 "
            "규모를 약간 확장하는 방향의 성장 실험이 가능해 보입니다."
        )

    return p1 + p2 + p3 + p4 + p5


def build_weekly_actions(
    kpi: Dict[str, float],
    funnel_compare_df: pd.DataFrame,
    traffic_this: pd.DataFrame,
    search_this: pd.DataFrame,
) -> List[str]:
    """
    THIS WEEK ACTION / GRAPH & ANALYSIS 공통으로 쓸 액션 아이템.
    - Paid media
    - Organic 콘텐츠
    - 장바구니/체크아웃 UX
    - 상품 믹스
    - 저CVR 키워드
    - 세그먼트 CRM
    - 리마케팅·프로모션
    """
    actions: List[str] = []

    # Paid media
    if kpi["revenue_lw_pct"] < 0 and kpi["uv_lw_pct"] < 0:
        actions.append(
            "Paid 채널 중 지난 4주 기준 ROAS 상위 캠페인의 일 예산을 10~15% 상향하고, "
            "성과 하위 캠페인은 입찰가·타겟을 재설정해 신규 유입을 회복합니다."
        )
    else:
        actions.append(
            "이번 주 성과가 좋은 캠페인·소재를 기준으로 유사 타겟 확장(룩어라이크, 관심사 확장)을 테스트해 "
            "획득 단가를 유지한 채 볼륨을 늘립니다."
        )

    # Organic 콘텐츠
    actions.append(
        "자사몰/인스타그램/네이버포스트 등에서 주간 베스트 상품·검색 상위 키워드를 묶어, "
        "UGC 기반 스타일링 포스트 1~2편을 제작해 오가닉 유입과 검색 전환을 동시에 끌어올립니다."
    )

    # 장바구니 · 체크아웃 UX
    if funnel_compare_df is not None and not funnel_compare_df.empty:
        worst = funnel_compare_df.sort_values("변화(ppt)").iloc[0]
        if worst["변화(ppt)"] < 0:
            actions.append(
                f"'{worst['구간']}' 구간 전환율이 악화된 원인을 확인하기 위해, "
                "해당 단계의 이탈 리포트(디바이스·채널·상품군 기준)를 분해해 최소 2개 이상의 UX/혜택 A/B 테스트를 설계합니다."
            )
        else:
            actions.append(
                f"'{worst['구간']}' 구간 전환율이 개선된 모멘텀을 유지하기 위해, "
                "동일 UX/혜택 구조를 타 주요 카테고리에도 복제 적용해 확장 효과를 검증합니다."
            )
    else:
        actions.append(
            "장바구니·체크아웃 구간의 이탈 데이터를 기기·브라우저·결제 수단 기준으로 분해해, "
            "특정 환경에서의 오류/로딩 문제 여부를 우선 점검합니다."
        )

    # 상품 믹스 조정
    actions.append(
        "매출 상위 SKU와 검색 상위 키워드의 교집합을 추출해 기획전 상단에 배치하고, "
        "재고 소진이 필요한 상품은 할인/쿠폰 배너를 장바구니·체크아웃 단계에 노출해 소진 속도를 높입니다."
    )

    # 저CVR 키워드
    if search_this is not None and not search_this.empty:
        low_cvr = search_this[search_this["CVR(%)"] < 1.0]
        if not low_cvr.empty:
            kw = ", ".join(low_cvr.head(3)["키워드"].tolist())
            actions.append(
                f"저CVR 검색어({kw})에 대해 결과 페이지의 상단 상품·필터·가격대를 재구성하고, "
                "관련 프로모션 배너를 추가해 전환율 개선 여부를 측정합니다."
            )

    # 세그먼트 CRM
    actions.append(
        "최근 90일 내 2회 이상 구매한 충성 고객과 최근 30일 유입·미구매 장바구니 이탈 고객을 분리해, "
        "각각 리워드 강화/재방문 유도형 CRM 캠페인을 별도로 실행합니다."
    )

    # 리마케팅 & 프로모션 실험
    actions.append(
        "주요 카테고리별로 리마케팅 캠페인을 분리하고, 쿠폰/무이자/무료배송 등 서로 다른 혜택 메시지를 AB 테스트해 "
        "세그먼트별 최적 인센티브를 찾습니다."
    )

    return actions


# =====================================================================
# 6) HTML 구성 유틸 (표/그래프)
# =====================================================================

def df_to_html_table(df: pd.DataFrame, max_rows: int = None) -> str:
    if df is None or df.empty:
        return "<p style='color:#999;font-size:11px;margin:4px 0 0 0;'>데이터 없음</p>"
    if max_rows is not None:
        df = df.head(max_rows)

    html = df.to_html(index=False, border=0, justify="left", escape=False)
    html = html.replace(
        '<table border="0" class="dataframe">',
        '<table style="width:100%; border-collapse:collapse; font-size:10px;">',
    )
    html = html.replace(
        '<tr style="text-align: right;">',
        '<tr style="background:#f4f6fb; text-align:left;">',
    )
    html = html.replace(
        "<th>",
        "<th style=\"padding:3px 6px; border-bottom:1px solid #e1e4f0; "
        "text-align:left; font-weight:600; color:#555;\">",
    )
    html = html.replace(
        "<td>",
        "<td style=\"padding:3px 6px; border-bottom:1px solid #f1f3fa; "
        "text-align:left; color:#333;\">",
    )
    return html


def build_kpi_cards_html(kpi: Dict[str, float]) -> str:
    """9개 KPI 카드 (THIS=이번주, LW=2주전, LY=전년동주)."""

    def card_block(title: str, main_html: str, this_txt: str, lw_txt: str, ly_txt: str,
                   lw_pct: float, ly_pct: float, unit_is_ppt: bool = False) -> str:
        lw_label = f"{lw_pct:+.1f}{'p' if unit_is_ppt else '%'}"
        ly_label = f"{ly_pct:+.1f}{'p' if unit_is_ppt else '%'}"
        return f"""
<div style="background:#ffffff; border-radius:16px; padding:14px 16px;
            border:1px solid #e1e7f5; height:100%;">
  <div style="font-size:11px; color:#777; margin-bottom:4px;">{title}</div>
  <div style="font-size:18px; font-weight:700; margin-bottom:4px;">
    {main_html}
  </div>
  <div style="font-size:10px; color:#999; margin-bottom:4px;">
    TODAY: {this_txt} · LW: {lw_txt} · LY: {ly_txt}
  </div>
  <div>
    <span style="display:inline-block; font-size:10px; padding:2px 7px; border-radius:999px;
                 background:#dbeafe; color:#1d4ed8; margin-right:4px;">
      LW {lw_label}
    </span>
    <span style="display:inline-block; font-size:10px; padding:2px 7px; border-radius:999px;
                 background:#fdeaea; color:#c53030;">
      LY {ly_label}
    </span>
  </div>
</div>
"""

    rev_card = card_block(
        "매출 (Revenue)",
        format_money_manwon(kpi["revenue_this"]),
        format_money_manwon(kpi["revenue_this"]),
        format_money_manwon(kpi["revenue_last"]),
        format_money_manwon(kpi["revenue_ly"]),
        kpi["revenue_lw_pct"],
        kpi["revenue_ly_pct"],
    )

    uv_card = card_block(
        "방문자수 (UV)",
        f"{kpi['uv_this']:,}명",
        f"{kpi['uv_this']:,}명",
        f"{kpi['uv_last']:,}명",
        f"{kpi['uv_ly']:,}명",
        kpi["uv_lw_pct"],
        kpi["uv_ly_pct"],
    )

    cvr_card = card_block(
        "전환율 (CVR)",
        f"{kpi['cvr_this']:.2f}%",
        f"{kpi['cvr_this']:.2f}%",
        f"{kpi['cvr_last']:.2f}%",
        f"{kpi['cvr_ly']:.2f}%",
        kpi["cvr_lw_pct"],
        kpi["cvr_ly_pct"],
        unit_is_ppt=True,
    )

    orders_card = card_block(
        "구매수 (Orders)",
        f"{kpi['orders_this']:,}건",
        f"{kpi['orders_this']:,}건",
        f"{kpi['orders_last']:,}건",
        f"{kpi['orders_ly']:,}건",
        kpi["orders_lw_pct"],
        kpi["orders_ly_pct"],
    )

    aov_card = card_block(
        "객단가 (AOV)",
        format_money(kpi["aov_this"]),
        format_money(kpi["aov_this"]),
        format_money(kpi["aov_last"]),
        format_money(kpi["aov_ly"]),
        kpi["aov_lw_pct"],
        kpi["aov_ly_pct"],
    )

    new_card = card_block(
        "신규 방문자 (New Users)",
        f"{kpi['new_this']:,}명",
        f"{kpi['new_this']:,}명",
        f"{kpi['new_last']:,}명",
        f"{kpi['new_ly']:,}명",
        kpi["new_lw_pct"],
        kpi["new_ly_pct"],
    )

    org_card = card_block(
        "오가닉 UV (Organic)",
        f"{kpi['organic_uv_this']:,}명",
        f"{kpi['organic_uv_this']:,}명",
        f"{kpi['organic_uv_last']:,}명",
        f"{kpi['organic_uv_ly']:,}명",
        kpi["organic_uv_lw_pct"],
        kpi["organic_uv_ly_pct"],
    )

    nonorg_card = card_block(
        "비오가닉 UV (Non-organic)",
        f"{kpi['nonorganic_uv_this']:,}명",
        f"{kpi['nonorganic_uv_this']:,}명",
        f"{kpi['nonorganic_uv_last']:,}명",
        f"{kpi['nonorganic_uv_ly']:,}명",
        kpi["nonorganic_uv_lw_pct"],
        kpi["nonorganic_uv_ly_pct"],
    )

    share_card = card_block(
        "오가닉 UV 비중 (Share)",
        f"{kpi['organic_share_this']:.1f}%",
        f"{kpi['organic_share_this']:.1f}%",
        f"{kpi['organic_share_last']:.1f}%",
        f"{kpi['organic_share_ly']:.1f}%",
        kpi["organic_share_lw_pct"],
        kpi["organic_share_ly_pct"],
    )

    html = f"""
<table width="100%" cellpadding="0" cellspacing="0"
       style="border-collapse:separate; border-spacing:8px 10px;">
  <tr>
    <td width="33.3%" valign="top">{rev_card}</td>
    <td width="33.3%" valign="top">{uv_card}</td>
    <td width="33.3%" valign="top">{cvr_card}</td>
  </tr>
  <tr>
    <td valign="top">{orders_card}</td>
    <td valign="top">{aov_card}</td>
    <td valign="top">{new_card}</td>
  </tr>
  <tr>
    <td valign="top">{org_card}</td>
    <td valign="top">{nonorg_card}</td>
    <td valign="top">{share_card}</td>
  </tr>
</table>
"""
    return html


def build_channel_mix_bars_html(mix_df: pd.DataFrame) -> str:
    if mix_df is None or mix_df.empty:
        return "<p style='color:#999;font-size:11px;margin:4px 0 0 0;'>데이터 없음</p>"

    max_share = max(mix_df["이번주 비중(%)"].max(), 1)
    rows_html = ""
    for _, row in mix_df.iterrows():
        width = max(5, int(row["이번주 비중(%)"] / max_share * 100))
        rows_html += f"""
<tr>
  <td style="font-size:10px; padding:3px 6px; color:#444; white-space:nowrap;">{row['채널']}</td>
  <td style="width:100%; padding:3px 6px;">
    <div style="background:#edf2ff; border-radius:999px; width:100%; height:10px; position:relative;">
      <div style="background:#4f46e5; border-radius:999px; height:10px; width:{width}%;"></div>
    </div>
  </td>
  <td style="font-size:10px; padding:3px 6px; color:#333; white-space:nowrap;">
    {row['이번주 비중(%)']:.1f}% / {row['변화(ppt)']:+.1f}p
  </td>
</tr>
"""
    return f"""
<table width="100%" cellpadding="0" cellspacing="0"
       style="border-collapse:collapse; margin-top:4px;">
  {rows_html}
</table>
"""


def build_search_heatmap_html(search_df: pd.DataFrame) -> str:
    if search_df is None or search_df.empty:
        return "<p style='color:#999;font-size:11px;margin:4px 0 0 0;'>데이터 없음</p>"

    df = search_df.copy().head(30)

    rows_html = ""
    for _, row in df.iterrows():
        cvr = safe_float(row["CVR(%)"])
        if cvr < 0.5:
            bg = "#fee2e2"
        elif cvr < 1.0:
            bg = "#ffedd5"
        elif cvr < 3.0:
            bg = "#ecfdf3"
        else:
            bg = "#dcfce7"
        rows_html += f"""
<tr>
  <td style="padding:3px 6px; font-size:10px; color:#333;">{row['키워드']}</td>
  <td style="padding:3px 6px; font-size:10px; color:#555; text-align:right;">{row['검색수']:,}</td>
  <td style="padding:3px 6px; font-size:10px; color:#555; text-align:right;">{row['구매수']:,}</td>
  <td style="padding:3px 6px; font-size:10px; text-align:right; background:{bg};">{cvr:.2f}%</td>
</tr>
"""
    return f"""
<table width="100%" cellpadding="0" cellspacing="0"
       style="border-collapse:collapse; font-size:10px; margin-top:4px;">
  <tr style="background:#f4f6fb;">
    <th style="padding:3px 6px; text-align:left; border-bottom:1px solid #e1e4f0;">키워드</th>
    <th style="padding:3px 6px; text-align:right; border-bottom:1px solid #e1e4f0;">검색수</th>
    <th style="padding:3px 6px; text-align:right; border-bottom:1px solid #e1e4f0;">구매수</th>
    <th style="padding:3px 6px; text-align:right; border-bottom:1px solid #e1e4f0;">CVR(%)</th>
  </tr>
  {rows_html}
</table>
"""


def build_graph_analysis_block(
    kpi: Dict[str, float],
    funnel_compare_df: pd.DataFrame,
    mix_df: pd.DataFrame,
    search_df: pd.DataFrame,
    actions: List[str],
) -> str:
    """04 · GRAPH & ANALYSIS 하단 '이번 주 변화 의미 분석' 텍스트 블록."""

    rev_line = f"- Revenue: 전주 대비 {kpi['revenue_lw_pct']:+.1f}% · 전년 동주 대비 {kpi['revenue_ly_pct']:+.1f}%"
    uv_line = f"- UV: 전주 대비 {kpi['uv_lw_pct']:+.1f}% · 전년 동주 대비 {kpi['uv_ly_pct']:+.1f}%"
    cvr_line = f"- CVR: 전주 대비 {kpi['cvr_lw_pct']:+.1f}p · 전년 동주 대비 {kpi['cvr_ly_pct']:+.1f}p"

    if funnel_compare_df is not None and not funnel_compare_df.empty:
        worst = funnel_compare_df.sort_values("변화(ppt)").iloc[0]
        funnel_line = f"- Funnel: '{worst['구간']}' 구간 전환율 {worst['변화(ppt)']:+.1f}p 변화"
    else:
        funnel_line = "- Funnel: 전주 대비 비교 가능한 데이터 부족"

    if mix_df is not None and not mix_df.empty:
        top = mix_df.iloc[0]
        mix_line = (
            f"- Channel Mix: '{top['채널']}' 채널 매출 비중 {top['이번주 비중(%)']:.1f}%, "
            f"전주 대비 {top['변화(ppt)']:+.1f}p"
        )
    else:
        mix_line = "- Channel Mix: 주간 채널 믹스 데이터 부족"

    if search_df is not None and not search_df.empty:
        low = search_df[search_df["CVR(%)"] < 1.0]
        if not low.empty:
            kw = ", ".join(low.head(3)["키워드"].tolist())
            search_line = f"- Search: 저CVR 키워드({kw}) 다수 존재 → 검색 결과·상품 구성이 전환에 제약 요인"
        else:
            search_line = "- Search: 상위 검색어 대부분 CVR 안정 구간, 상단 노출 확장 여지"
    else:
        search_line = "- Search: 유의미한 검색 데이터 부족"

    what = (
        "<p style='margin:4px 0 6px 0; font-size:11px; color:#111;'><b>1. What happened?</b><br>"
        f"{rev_line}<br>{uv_line}<br>{cvr_line}<br>{funnel_line}<br>{mix_line}<br>{search_line}</p>"
    )

    why = (
        "<p style='margin:4px 0 6px 0; font-size:11px; color:#111;'><b>2. Why?</b><br>"
        "채널 믹스 변화(오가닉 vs 페이드 비중), 특정 퍼널 단계의 이탈 확대/완화, "
        "검색 키워드·상품 조합의 전환 효율이 복합적으로 작용한 결과입니다. "
        "특히 매출과 UV의 방향이 다른 경우에는 유입 품질·랜딩 페이지 일관성·프로모션 노출 위치가 핵심 요인으로 작용했을 가능성이 큽니다."
        "</p>"
    )

    insight = (
        "<p style='margin:4px 0 6px 0; font-size:11px; color:#111;'><b>3. Insight</b><br>"
        "주간 단위로 KPI·퍼널·채널·검색을 동시에 바라보면, "
        "단일 채널/캠페인 성과만으로는 보이지 않던 구조적 패턴(예: 저CVR 키워드, 특정 디바이스·카테고리에서의 이탈 확대 등)을 "
        "빨리 발견할 수 있습니다. "
        "이 인사이트를 바탕으로 다음 주에는 '유입 품질'과 '퍼널 전환'을 분리해서 실험 설계를 진행하는 것이 중요합니다."
        "</p>"
    )

    action_items_html = "".join(
        f"<li style='margin-bottom:3px;'>{a}</li>" for a in actions[:4]
    )
    action = (
        "<p style='margin:4px 0 4px 0; font-size:11px; color:#111;'><b>4. Action</b></p>"
        f"<ul style='margin:0 0 0 16px; padding:0; font-size:11px; color:#333;'>{action_items_html}</ul>"
    )

    return f"""
<div style="margin-top:10px; padding:10px 12px; background:#f8fafc;
            border-radius:12px; border:1px solid #e2e8f0;">
  <div style="font-size:11px; font-weight:600; color:#0f172a; margin-bottom:4px;">
    이번 주 변화 의미 분석
  </div>
  {what}
  {why}
  {insight}
  {action}
</div>
"""


# =====================================================================
# 7) HTML 메일 전체 템플릿
# =====================================================================

def compose_html_weekly(
    kpi: Dict[str, float],
    funnel_raw: pd.DataFrame,
    funnel_compare_df: pd.DataFrame,
    traffic_this: pd.DataFrame,
    traffic_last: pd.DataFrame,
    products_this: pd.DataFrame,
    search_this: pd.DataFrame,
) -> str:
    # 01 Insight / Action
    insight_paragraph = build_weekly_insight_paragraph(
        kpi, funnel_compare_df, traffic_this, search_this
    )
    weekly_actions = build_weekly_actions(
        kpi, funnel_compare_df, traffic_this, search_this
    )

    insight_html = f"""
<table width="100%" cellpadding="0" cellspacing="0"
       style="background:#ffffff; border-radius:14px;
              border:1px solid #e1e7f5; box-shadow:0 4px 12px rgba(0,0,0,0.04);
              padding:10px 12px; border-collapse:separate; height:100%;">
  <tr><td>
    <div style="font-size:11px; font-weight:600; color:#004a99; margin-bottom:4px;">
      THIS WEEK INSIGHT
    </div>
    <div style="font-size:11px; color:#333; line-height:1.7;">
      {insight_paragraph}
    </div>
  </td></tr>
</table>
"""

    action_items_html = "".join(
        f"<li style='margin-bottom:4px;'>{a}</li>" for a in weekly_actions
    )
    action_html = f"""
<table width="100%" cellpadding="0" cellspacing="0"
       style="background:#ffffff; border-radius:14px;
              border:1px solid #e1e7f5; box-shadow:0 4px 12px rgba(0,0,0,0.04);
              padding:10px 12px; border-collapse:separate; height:100%;">
  <tr><td>
    <div style="font-size:11px; font-weight:600; color:#0f766e; margin-bottom:4px;">
      ACTION ITEMS
    </div>
    <ul style="margin:0; padding-left:16px; font-size:11px; color:#555; line-height:1.7;">
      {action_items_html}
    </ul>
  </td></tr>
</table>
"""

    insight_action_block = f"""
<table width="100%" cellpadding="0" cellspacing="0"
       style="border-collapse:separate; border-spacing:8px 10px; margin-top:14px;">
  <tr>
    <td width="50%" valign="top">{insight_html}</td>
    <td width="50%" valign="top">{action_html}</td>
  </tr>
</table>
"""

    # 02 KPI Cards
    kpi_cards_html = build_kpi_cards_html(kpi)

    # 03 Funnel / Traffic / Product / Search
    funnel_raw_box = f"""
<table width="100%" cellpadding="0" cellspacing="0"
       style="background:#ffffff; border-radius:12px;
              border:1px solid #e1e7f5; box-shadow:0 3px 10px rgba(0,0,0,0.03);
              padding:8px 10px; border-collapse:separate; min-height:180px;">
  <tr><td>
    <div style="font-size:11px; font-weight:600; color:#224; margin-bottom:2px;">
      Funnel Events (view → cart → checkout → purchase)
    </div>
    <div style="font-size:10px; color:#888; margin-bottom:6px; line-height:1.4;">
      주간 이벤트 카운트 기준 퍼널 흐름입니다.
    </div>
    {df_to_html_table(funnel_raw)}
  </td></tr>
</table>
"""

    funnel_compare_box = f"""
<table width="100%" cellpadding="0" cellspacing="0"
       style="background:#ffffff; border-radius:12px;
              border:1px solid #e1e7f5; box-shadow:0 3px 10px rgba(0,0,0,0.03);
              padding:8px 10px; border-collapse:separate; min-height:180px;">
  <tr><td>
    <div style="font-size:11px; font-weight:600; color:#224; margin-bottom:2px;">
      Funnel Conversion (이번주 vs 전주)
    </div>
    <div style="font-size:10px; color:#888; margin-bottom:6px; line-height:1.4;">
      구간별 전환율과 전주 대비 변화(ppt)를 비교합니다.
    </div>
    {df_to_html_table(funnel_compare_df)}
  </td></tr>
</table>
"""

    traffic_box = f"""
<table width="100%" cellpadding="0" cellspacing="0"
       style="background:#ffffff; border-radius:12px;
              border:1px solid #e1e7f5; box-shadow:0 3px 10px rgba(0,0,0,0.03);
              padding:8px 10px; border-collapse:separate; min-height:180px;">
  <tr><td>
    <div style="font-size:11px; font-weight:600; color:#224; margin-bottom:2px;">
      Traffic by Channel (이번주)
    </div>
    <div style="font-size:10px; color:#888; margin-bottom:6px; line-height:1.4;">
      채널별 UV · 구매수 · 매출 · CVR · 신규 방문자 요약입니다.
    </div>
    {df_to_html_table(traffic_this)}
  </td></tr>
</table>
"""

    products_box = f"""
<table width="100%" cellpadding="0" cellspacing="0"
       style="background:#ffffff; border-radius:12px;
              border:1px solid #e1e7f5; box-shadow:0 3px 10px rgba(0,0,0,0.03);
              padding:8px 10px; border-collapse:separate; min-height:180px;">
  <tr><td>
    <div style="font-size:11px; font-weight:600; color:#224; margin-bottom:2px;">
      Top Selling Products (이번주)
    </div>
    <div style="font-size:10px; color:#888; margin-bottom:6px; line-height:1.4;">
      매출 기준 상위 상품 리스트입니다. 신규/재구매 SKU 분리는 추후 CRM 연동 시 확장 가능합니다.
    </div>
    {df_to_html_table(products_this, max_rows=15)}
  </td></tr>
</table>
"""

    search_box = f"""
<table width="100%" cellpadding="0" cellspacing="0"
       style="background:#ffffff; border-radius:12px;
              border:1px solid #e1e7f5; box-shadow:0 3px 10px rgba(0,0,0,0.03);
              padding:8px 10px; border-collapse:separate; min-height:180px;">
  <tr><td>
    <div style="font-size:11px; font-weight:600; color:#224; margin-bottom:2px;">
      On-site Search Keywords (이번주)
    </div>
    <div style="font-size:10px; color:#888; margin-bottom:6px; line-height:1.4;">
      검색수 기준 상위 키워드와 CVR입니다.
    </div>
    {df_to_html_table(search_this, max_rows=20)}
  </td></tr>
</table>
"""

    # 04 GRAPH & ANALYSIS (그래프 영역은 텍스트+바/히트맵 형태)
    mix_df = build_channel_mix(traffic_this, traffic_last)
    mix_bars_html = build_channel_mix_bars_html(mix_df)
    search_heatmap_html = build_search_heatmap_html(search_this)
    graph_analysis_block = build_graph_analysis_block(
        kpi, funnel_compare_df, mix_df, search_this, weekly_actions
    )

    kpi_graph_html = f"""
<table width="100%" cellpadding="0" cellspacing="0"
       style="background:#ffffff; border-radius:12px;
              border:1px solid #e1e7f5; box-shadow:0 3px 10px rgba(0,0,0,0.03);
              padding:8px 10px; border-collapse:separate; min-height:120px;">
  <tr><td>
    <div style="font-size:11px; font-weight:600; color:#1e293b; margin-bottom:2px;">
      KPI 변화 (이번주 vs 전주)
    </div>
    <div style="font-size:10px; color:#64748b; margin-bottom:6px;">
      Revenue · UV · CVR · Orders · AOV · New Users 중심으로 주간 흐름을 비교합니다.
    </div>
    <ul style="margin:0 0 0 16px; padding:0; font-size:10px; color:#0f172a; line-height:1.6;">
      <li>Revenue: {kpi['revenue_lw_pct']:+.1f}% (vs LW)</li>
      <li>UV: {kpi['uv_lw_pct']:+.1f}% / CVR: {kpi['cvr_lw_pct']:+.1f}p</li>
      <li>Orders: {kpi['orders_lw_pct']:+.1f}% / AOV: {kpi['aov_lw_pct']:+.1f}%</li>
      <li>New Users: {kpi['new_lw_pct']:+.1f}%</li>
    </ul>
  </td></tr>
</table>
"""

    funnel_graph_html = f"""
<table width="100%" cellpadding="0" cellspacing="0"
       style="background:#ffffff; border-radius:12px;
              border:1px solid #e1e7f5; box-shadow:0 3px 10px rgba(0,0,0,0.03);
              padding:8px 10px; border-collapse:separate; min-height:120px;">
  <tr><td>
    <div style="font-size:11px; font-weight:600; color:#1e293b; margin-bottom:2px;">
      Funnel 비교 (이번주 vs 전주)
    </div>
    <div style="font-size:10px; color:#64748b; margin-bottom:6px;">
      각 퍼널 단계의 전환율 변화를 바탕으로 이탈 확대/완화 구간을 파악합니다.
    </div>
    {df_to_html_table(funnel_compare_df)}
  </td></tr>
</table>
"""

    mix_graph_html = f"""
<table width="100%" cellpadding="0" cellspacing="0"
       style="background:#ffffff; border-radius:12px;
              border:1px solid #e1e7f5; box-shadow:0 3px 10px rgba(0,0,0,0.03);
              padding:8px 10px; border-collapse:separate; min-height:120px;">
  <tr><td>
    <div style="font-size:11px; font-weight:600; color:#1e293b; margin-bottom:2px;">
      Channel Mix 변화
    </div>
    <div style="font-size:10px; color:#64748b; margin-bottom:6px;">
      채널별 매출 비중과 전주 대비 변화(ppt)를 막대 형태로 시각화했습니다.
    </div>
    {mix_bars_html}
  </td></tr>
</table>
"""

    search_heatmap_block = f"""
<table width="100%" cellpadding="0" cellspacing="0"
       style="background:#ffffff; border-radius:12px;
              border:1px solid #e1e7f5; box-shadow:0 3px 10px rgba(0,0,0,0.03);
              padding:8px 10px; border-collapse:separate; min-height:120px;">
  <tr><td>
    <div style="font-size:11px; font-weight:600; color:#1e293b; margin-bottom:2px;">
      Search CVR Heatmap
    </div>
    <div style="font-size:10px; color:#64748b; margin-bottom:6px;">
      키워드 × 전환율 매트릭스를 통해 저CVR 영역을 강조했습니다.
    </div>
    {search_heatmap_html}
  </td></tr>
</table>
"""

    html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="utf-8">
<title>Columbia Sportswear Korea — Weekly eCommerce Performance Digest</title>
</head>
<body style="margin:0; padding:0; background:#f5f7fb;
             font-family:-apple-system,BlinkMacSystemFont,'Segoe UI','Noto Sans KR',Arial,sans-serif;">

<table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="background:#f5f7fb;">
  <tr>
    <td align="center">
      <table role="presentation" width="900" cellspacing="0" cellpadding="0"
             style="padding:24px 12px 24px 12px; background:#f5f7fb;">
        <tr>
          <td>

            <!-- 헤더 -->
            <table role="presentation" width="100%" cellspacing="0" cellpadding="0"
                   style="background:#ffffff; border-radius:18px; border:1px solid #e6e9ef;
                          box-shadow:0 6px 18px rgba(0,0,0,0.06);">
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
                    {kpi['week_label']} 기준 (직전 7일)
                  </span>
                  <div style="font-size:11px; color:#777; margin-top:6px; margin-bottom:2px; line-height:1.6;">
                    매출 · UV · CVR · AOV · 신규 · 오가닉 비중을 중심으로 주간 GA4 데이터를 PPT용 구조 그대로 요약한 리포트입니다.
                  </div>
                </td>

                <td valign="top" align="right" style="padding:16px 20px 16px 0%;">
                  <table role="presentation" cellspacing="0" cellpadding="0" align="right"
                         style="margin-bottom:8px;">
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
                          FUNNEL · TRAFFIC
                        </span>
                      </td>
                      <td style="padding:0 3px;">
                        <span style="display:inline-block; font-size:10px; padding:4px 9px; border-radius:999px;
                                     background:#fafbfd; color:#445; border:1px solid #dfe6f3;">
                          GRAPH & ANALYSIS
                        </span>
                      </td>
                    </tr>
                  </table>
                </td>
              </tr>
            </table>

{insight_action_block}

<!-- 02 WEEKLY KPI SNAPSHOT -->
<div style="font-size:11px; letter-spacing:0.12em; color:#6d7a99;
            margin-top:18px; margin-bottom:10px;">
  02 · WEEKLY KPI SNAPSHOT
</div>
{kpi_cards_html}

<!-- 03 FUNNEL · TRAFFIC · PRODUCT · SEARCH -->
<div style="font-size:11px; letter-spacing:0.12em; color:#6d7a99;
            margin-top:20px; margin-bottom:8px;">
  03 · FUNNEL · TRAFFIC · PRODUCT · SEARCH
</div>
<table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:4px;">
  <tr>
    <td width="50%" valign="top" style="padding:4px 6px 8px 0%;">{funnel_raw_box}</td>
    <td width="50%" valign="top" style="padding:4px 0 8px 6px;">{funnel_compare_box}</td>
  </tr>
  <tr>
    <td width="50%" valign="top" style="padding:4px 6px 8px 0%;">{traffic_box}</td>
    <td width="50%" valign="top" style="padding:4px 0 8px 6px;">{products_box}</td>
  </tr>
  <tr>
    <td width="50%" valign="top" style="padding:4px 6px 0 0%;">{search_box}</td>
    <td width="50%" valign="top" style="padding:4px 0 0 6px;"></td>
  </tr>
</table>

<!-- 04 GRAPH & ANALYSIS -->
<div style="font-size:11px; letter-spacing:0.12em; color:#6d7a99;
            margin-top:20px; margin-bottom:8px;">
  04 · GRAPH & ANALYSIS
</div>
<table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:4px;">
  <tr>
    <td width="50%" valign="top" style="padding:4px 6px 8px 0%;">{kpi_graph_html}</td>
    <td width="50%" valign="top" style="padding:4px 0 8px 6px;">{funnel_graph_html}</td>
  </tr>
  <tr>
    <td width="50%" valign="top" style="padding:4px 6px 8px 0%;">{mix_graph_html}</td>
    <td width="50%" valign="top" style="padding:4px 0 8px 6px;">{search_heatmap_block}</td>
  </tr>
</table>

{graph_analysis_block}

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
# 8) 메인: 주간 다이제스트 생성 & 발송
# =====================================================================

def send_weekly_digest():
    try:
        ranges = get_week_ranges()

        # GA 데이터 수집
        kpi = build_weekly_kpi()
        funnel_raw, funnel_compare_df = build_weekly_funnel_comparison()
        traffic_this = src_weekly_traffic(ranges["this"]["start"], ranges["this"]["end"])
        traffic_last = src_weekly_traffic(ranges["last"]["start"], ranges["last"]["end"])
        products_this = src_weekly_products(ranges["this"]["start"], ranges["this"]["end"])
        search_this = src_weekly_search(ranges["this"]["start"], ranges["this"]["end"], limit=80)

        # HTML 생성
        html_body = compose_html_weekly(
            kpi=kpi,
            funnel_raw=funnel_raw,
            funnel_compare_df=funnel_compare_df,
            traffic_this=traffic_this,
            traffic_last=traffic_last,
            products_this=products_this,
            search_this=search_this,
        )

        # 옵션: 전체를 JPEG로 캡쳐해 인라인 이미지로 발송
        jpeg_path = html_to_jpeg(html_body) if ENABLE_INLINE_JPEG else ""
        subject = f"[Columbia] Weekly eCommerce Digest – {kpi['week_label']}"

        send_email_html(subject, html_body, WEEKLY_RECIPIENTS, jpeg_path)
        print("[INFO] Weekly digest sent.")
    except Exception as e:
        msg = f"[ERROR] Weekly digest 생성/발송 중 오류 발생: {e}"
        print(msg)
        send_critical_alert("[Columbia] Weekly Digest Error", msg)


if __name__ == "__main__":
    send_weekly_digest()

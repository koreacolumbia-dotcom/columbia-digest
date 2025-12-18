#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Columbia Sportswear Korea
Daily eCommerce Performance Digest (GA4 + HTML Mail)

[2025-12-18 patch]
- 02 카드 전일(2daysAgo) 대비 증감(Δ) 컬럼 추가
- 오가닉 서치 상세(Source/Medium) 카드 추가
- (추가요청) 1) 쿠폰/프로모션 요약 4) 검색 후 구매 0 TOP 5) 디바이스 스플릿 + 디바이스별 퍼널 추가

[Hotfix]
- pandas TypeError: Expected numeric dtype, got object -> _add_delta_cols() numeric casting 강화
- 02 카드 문구/컬럼명 오해 방지(수Δ -> 전일 대비(%))
- 02 카드 여백/높이(칸 넓음) 축소
- DC 크롤링/VOC 코드 전체 제거

[2025-12-18 layout patch]
- 01 KPI 카드 9개(3x3) 복구
- 02 섹션: products_hi 폭 축소 / search_top 폭 확대
- 03 오가닉: 엔진별 + Source/Medium 2개를 한 줄(50:50)
- 표 텍스트 세로 깨짐 방지(word-break/white-space/overflow-wrap)
"""

import os
import smtplib
import pandas as pd

from typing import List, Optional

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from datetime import datetime

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from google.oauth2 import service_account


# =====================================================================
# 0) 환경 변수 / 기본 설정
# =====================================================================

GA4_PROPERTY_ID = os.getenv("GA4_PROPERTY_ID", "358593394").strip()
GA_ITEM_VIEW_METRIC = os.getenv("GA_ITEM_VIEW_METRIC", "").strip()

SMTP_PROVIDER = os.getenv("SMTP_PROVIDER", "gmail").lower()
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "").strip()
SMTP_PASS = os.getenv("SMTP_PASS", "").strip()

DAILY_RECIPIENTS = [
    e.strip()
    for e in os.getenv("DAILY_RECIPIENTS", "korea_Ecom@columbia.com").split(",")
    if e.strip()
]

ALERT_RECIPIENT = os.getenv("ALERT_RECIPIENT", "").strip()

CVR_DROP_PPTS = float(os.getenv("CVR_DROP_PPTS", "0.5"))
REVENUE_DROP_PCT = float(os.getenv("REVENUE_DROP_PCT", "15"))
UV_DROP_PCT = float(os.getenv("UV_DROP_PCT", "20"))

PDP_ADD2CART_MIN_PCT = float(os.getenv("PDP_ADD2CART_MIN_PCT", "6"))
CART2CHK_MIN_PCT = float(os.getenv("CART2CHK_MIN_PCT", "45"))
CHK2BUY_MIN_PCT = float(os.getenv("CHK2BUY_MIN_PCT", "60"))

SEARCH_CVR_MIN = float(os.getenv("SEARCH_CVR_MIN", "1.0"))

PRODUCT_COLS = ["상품명", "상품조회수", "구매수", "매출(만원)", "CVR(%)"]

ENABLE_INLINE_JPEG = os.getenv("DIGEST_INLINE_JPEG", "0") == "1"
HTML_SCREENSHOT_WIDTH = int(os.getenv("DIGEST_IMG_WIDTH", "1200"))


# =====================================================================
# 1) 유틸 함수
# =====================================================================

def pct_change(curr, prev):
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


def format_date_label(ga_date_str):
    try:
        s = str(ga_date_str)
        if "." in s:
            s = str(int(float(s)))
        d = datetime.strptime(s, "%Y%m%d")
        return d.strftime("%Y-%m-%d")
    except Exception:
        return str(ga_date_str)


def _to_numeric_series(s: pd.Series) -> pd.Series:
    if s is None:
        return s
    try:
        s2 = s.astype(str).str.replace(",", "", regex=False)
        return pd.to_numeric(s2, errors="coerce")
    except Exception:
        return pd.to_numeric(s, errors="coerce")


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


def html_to_jpeg(html_body: str, out_path: str = "/tmp/columbia_daily_digest.jpg") -> str:
    if not ENABLE_INLINE_JPEG:
        return ""
    try:
        from pyppeteer import launch
        import asyncio
    except Exception:
        print("[WARN] pyppeteer 미설치 – HTML 그대로 발송.")
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
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
        loop.run_until_complete(_capture())
        print(f"[INFO] HTML→JPEG 변환 완료: {out_path}")
        return out_path
    except Exception as e:
        print("[WARN] HTML→JPEG 변환 실패:", e)
        return ""


def send_email_html(subject: str, html_body: str, recipients, jpeg_path: str = ""):
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

    msg = MIMEMultipart("related")
    msg["Subject"] = subject
    msg["From"] = SMTP_USER
    msg["To"] = ", ".join(recipients)

    alt = MIMEMultipart("alternative")
    msg.attach(alt)

    plain_text = "Columbia eCommerce Daily Digest 입니다. 메일이 제대로 보이지 않으면 HTML/이미지를 확인해주세요."
    alt.attach(MIMEText(plain_text, "plain", "utf-8"))

    if jpeg_path and os.path.exists(jpeg_path):
        html_body_effective = f"""<html><body style='margin:0; padding:0; background:#f4f6fb;'>
<div style='width:100%; text-align:center; padding:16px 0;'>
  <img src="cid:digest_image" alt="Columbia Daily eCommerce Digest" style="max-width:100%; height:auto; border:0; display:block; margin:0 auto;" />
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
# 3) GA4 Client & 공통 run_report
# =====================================================================

SERVICE_ACCOUNT_JSON = os.getenv("GA4_SERVICE_ACCOUNT_JSON", "").strip()

if SERVICE_ACCOUNT_JSON:
    SERVICE_ACCOUNT_FILE = "/tmp/ga4_service_account.json"
    with open(SERVICE_ACCOUNT_FILE, "w", encoding="utf-8") as f:
        f.write(SERVICE_ACCOUNT_JSON)
else:
    SERVICE_ACCOUNT_FILE = os.getenv("GA4_SERVICE_ACCOUNT_FILE", "").strip()


def ga_client():
    if not GA4_PROPERTY_ID:
        raise SystemExit("GA4_PROPERTY_ID가 비어 있습니다.")
    if not SERVICE_ACCOUNT_FILE or not os.path.exists(SERVICE_ACCOUNT_FILE):
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
        rows.append([*[d.value for d in r.dimension_values], *[m.value for m in r.metric_values]])
    df = pd.DataFrame(rows, columns=headers)
    return df


# =====================================================================
# 4) 데이터 소스 (GA4)
# =====================================================================

def src_kpi_one_day(start_date_str: str, end_date_str: str):
    df = ga_run_report(
        dimensions=["date"],
        metrics=["sessions", "transactions", "purchaseRevenue", "newUsers"],
        start_date=start_date_str,
        end_date=end_date_str,
    )
    if df.empty:
        return {"date": None, "sessions": 0, "transactions": 0, "purchaseRevenue": 0.0, "newUsers": 0}

    row = df.iloc[0]
    return {
        "date": row.get("date"),
        "sessions": safe_int(row.get("sessions")),
        "transactions": safe_int(row.get("transactions")),
        "purchaseRevenue": safe_float(row.get("purchaseRevenue")),
        "newUsers": safe_int(row.get("newUsers")),
    }


def src_funnel_day(day_keyword: str):
    df = ga_run_report(
        dimensions=["eventName"],
        metrics=["eventCount"],
        start_date=day_keyword,
        end_date=day_keyword,
    )
    want = ["view_item", "add_to_cart", "begin_checkout", "purchase"]
    df = df[df["eventName"].isin(want)].copy()
    df.rename(columns={"eventName": "단계", "eventCount": "수"}, inplace=True)

    if df.empty:
        return pd.DataFrame(columns=["단계", "수"]), pd.DataFrame(
            columns=["구간", "기준", "전환율(%)", "이탈율(%)", "벤치마크(전환 최소)"]
        )

    df["수"] = _to_numeric_series(df["수"]).fillna(0).astype(int)

    order = {k: i for i, k in enumerate(want)}
    df["ord"] = df["단계"].map(order)
    df = df.sort_values("ord").drop(columns=["ord"])

    def rate(a, b):
        if b == 0:
            return 0.0
        return round(a / b * 100, 1)

    base = df.set_index("단계")["수"]
    view_cnt = int(base.get("view_item", 0))
    cart_cnt = int(base.get("add_to_cart", 0))
    chk_cnt  = int(base.get("begin_checkout", 0))
    buy_cnt  = int(base.get("purchase", 0))

    data = [
        {"구간": "상품 상세 → 장바구니", "기준": "PDP → Cart",
         "전환율(%)": rate(cart_cnt, view_cnt),
         "이탈율(%)": rate(view_cnt - cart_cnt, view_cnt),
         "벤치마크(전환 최소)": PDP_ADD2CART_MIN_PCT},
        {"구간": "장바구니 → 체크아웃", "기준": "Cart → Checkout",
         "전환율(%)": rate(chk_cnt, cart_cnt),
         "이탈율(%)": rate(cart_cnt - chk_cnt, cart_cnt),
         "벤치마크(전환 최소)": CART2CHK_MIN_PCT},
        {"구간": "체크아웃 → 결제완료", "기준": "Checkout → Purchase",
         "전환율(%)": rate(buy_cnt, chk_cnt),
         "이탈율(%)": rate(chk_cnt - buy_cnt, chk_cnt),
         "벤치마크(전환 최소)": CHK2BUY_MIN_PCT},
    ]
    funnel_rate_df = pd.DataFrame(data)
    return df[["단계", "수"]], funnel_rate_df


def src_funnel_yesterday():
    return src_funnel_day("yesterday")


def src_traffic_day(day_keyword: str):
    df = ga_run_report(
        dimensions=["sessionDefaultChannelGroup"],
        metrics=["sessions", "transactions", "newUsers"],
        start_date=day_keyword,
        end_date=day_keyword,
    )
    if df.empty:
        return pd.DataFrame(columns=["소스", "UV", "구매수", "CVR(%)", "신규 방문자"])

    df.rename(columns={
        "sessionDefaultChannelGroup": "소스",
        "sessions": "UV",
        "transactions": "구매수",
        "newUsers": "신규 방문자",
    }, inplace=True)

    df["UV"] = _to_numeric_series(df["UV"]).fillna(0).astype(int)
    df["구매수"] = _to_numeric_series(df["구매수"]).fillna(0).astype(int)
    df["신규 방문자"] = _to_numeric_series(df["신규 방문자"]).fillna(0).astype(int)

    df["CVR(%)"] = (df["구매수"] / df["UV"].replace(0, pd.NA) * 100)
    df["CVR(%)"] = pd.to_numeric(df["CVR(%)"], errors="coerce").fillna(0.0).round(2)

    df = df.sort_values("UV", ascending=False)
    return df[["소스", "UV", "구매수", "CVR(%)", "신규 방문자"]]


def src_traffic_yesterday():
    return src_traffic_day("yesterday")


def src_search_day(day_keyword: str, limit=100):
    df = ga_run_report(
        dimensions=["searchTerm"],
        metrics=["eventCount", "transactions"],
        start_date=day_keyword,
        end_date=day_keyword,
        limit=limit,
    )
    if df.empty:
        return pd.DataFrame(columns=["키워드", "검색수", "구매수", "CVR(%)"])

    df.rename(columns={"searchTerm": "키워드", "eventCount": "검색수", "transactions": "구매수"}, inplace=True)

    df["검색수"] = _to_numeric_series(df["검색수"]).fillna(0).astype(int)
    df["구매수"] = _to_numeric_series(df["구매수"]).fillna(0).astype(int)

    df["CVR(%)"] = (df["구매수"] / df["검색수"].replace(0, pd.NA) * 100)
    df["CVR(%)"] = pd.to_numeric(df["CVR(%)"], errors="coerce").fillna(0.0).round(2)

    df = df.sort_values("검색수", ascending=False)
    return df[["키워드", "검색수", "구매수", "CVR(%)"]]


def src_search_yesterday(limit=100):
    return src_search_day("yesterday", limit=limit)


def src_hourly_revenue_traffic():
    df = ga_run_report(
        dimensions=["hour"],
        metrics=["sessions", "purchaseRevenue"],
        start_date="yesterday",
        end_date="yesterday",
    )
    if df.empty:
        return pd.DataFrame(columns=["시간", "시간_숫자", "세션수", "매출"])

    df = df.copy()
    df["시간_숫자"] = _to_numeric_series(df["hour"]).fillna(0).astype(int)
    df["시간"] = df["시간_숫자"].map(lambda h: f"{h:02d}")

    df.rename(columns={"sessions": "세션수", "purchaseRevenue": "매출"}, inplace=True)
    df["세션수"] = _to_numeric_series(df["세션수"]).fillna(0).astype(int)
    df["매출"] = _to_numeric_series(df["매출"]).fillna(0.0).astype(float)

    df = df.sort_values("시간_숫자")
    return df[["시간", "시간_숫자", "세션수", "매출"]]


def src_organic_search_engines_yesterday(limit: int = 10) -> pd.DataFrame:
    df = ga_run_report(
        dimensions=["sessionDefaultChannelGroup", "sessionSource"],
        metrics=["sessions", "transactions"],
        start_date="yesterday",
        end_date="yesterday",
        limit=0,
    )
    if df.empty:
        return pd.DataFrame(columns=["검색엔진", "UV", "구매수", "CVR(%)"])

    df = df[df["sessionDefaultChannelGroup"] == "Organic Search"].copy()
    if df.empty:
        return pd.DataFrame(columns=["검색엔진", "UV", "구매수", "CVR(%)"])

    df.rename(columns={"sessionSource": "검색엔진", "sessions": "UV", "transactions": "구매수"}, inplace=True)
    df["UV"] = _to_numeric_series(df["UV"]).fillna(0).astype(int)
    df["구매수"] = _to_numeric_series(df["구매수"]).fillna(0).astype(int)

    df = df.groupby("검색엔진", as_index=False).agg({"UV": "sum", "구매수": "sum"})
    df["CVR(%)"] = (df["구매수"] / df["UV"].replace(0, pd.NA) * 100)
    df["CVR(%)"] = pd.to_numeric(df["CVR(%)"], errors="coerce").fillna(0.0).round(1)

    df = df.sort_values("UV", ascending=False).head(limit)
    return df[["검색엔진", "UV", "구매수", "CVR(%)"]]


def src_organic_search_detail_source_medium_yesterday(limit: int = 15) -> pd.DataFrame:
    df = ga_run_report(
        dimensions=["sessionDefaultChannelGroup", "sessionSource", "sessionMedium"],
        metrics=["sessions", "transactions"],
        start_date="yesterday",
        end_date="yesterday",
        limit=0,
    )
    if df.empty:
        return pd.DataFrame(columns=["Source / Medium", "UV", "구매수", "CVR(%)"])

    df = df[df["sessionDefaultChannelGroup"] == "Organic Search"].copy()
    if df.empty:
        return pd.DataFrame(columns=["Source / Medium", "UV", "구매수", "CVR(%)"])

    df["sessions"] = _to_numeric_series(df["sessions"]).fillna(0).astype(int)
    df["transactions"] = _to_numeric_series(df["transactions"]).fillna(0).astype(int)
    df["Source / Medium"] = df["sessionSource"].astype(str) + " / " + df["sessionMedium"].astype(str)

    out = df.groupby("Source / Medium", as_index=False).agg({"sessions": "sum", "transactions": "sum"})
    out.rename(columns={"sessions": "UV", "transactions": "구매수"}, inplace=True)

    out["CVR(%)"] = (out["구매수"] / out["UV"].replace(0, pd.NA) * 100)
    out["CVR(%)"] = pd.to_numeric(out["CVR(%)"], errors="coerce").fillna(0.0).round(1)

    out = out.sort_values("UV", ascending=False).head(limit)
    return out[["Source / Medium", "UV", "구매수", "CVR(%)"]]


def src_coupon_performance_yesterday(limit: int = 12) -> pd.DataFrame:
    try:
        df = ga_run_report(
            dimensions=["coupon"],
            metrics=["transactions", "purchaseRevenue"],
            start_date="yesterday",
            end_date="yesterday",
            limit=0,
        )
    except Exception:
        return pd.DataFrame(columns=["쿠폰", "구매수", "매출(만원)", "매출비중(%)"])

    if df.empty:
        return pd.DataFrame(columns=["쿠폰", "구매수", "매출(만원)", "매출비중(%)"])

    df.rename(columns={"coupon": "쿠폰", "transactions": "구매수", "purchaseRevenue": "매출(원)"}, inplace=True)
    df["쿠폰"] = df["쿠폰"].astype(str)
    df = df[~df["쿠폰"].str.contains(r"^\(not set\)$", regex=True, na=False)]
    df = df[df["쿠폰"].str.strip() != ""]

    if df.empty:
        return pd.DataFrame(columns=["쿠폰", "구매수", "매출(만원)", "매출비중(%)"])

    df["구매수"] = _to_numeric_series(df["구매수"]).fillna(0).astype(int)
    df["매출(원)"] = _to_numeric_series(df["매출(원)"]).fillna(0.0).astype(float)

    total_rev = float(df["매출(원)"].sum())
    df["매출(만원)"] = (df["매출(원)"] / 10_000).round(1)
    df["매출비중(%)"] = ((df["매출(원)"] / total_rev) * 100).round(1) if total_rev > 0 else 0.0

    df = df.sort_values(["구매수", "매출(원)"], ascending=[False, False]).head(limit)
    return df[["쿠폰", "구매수", "매출(만원)", "매출비중(%)"]]


def src_search_zero_purchase_yesterday(min_searches: int = 20, limit: int = 12) -> pd.DataFrame:
    df = src_search_yesterday(limit=500)
    if df.empty:
        return pd.DataFrame(columns=["키워드", "검색수", "구매수", "CVR(%)"])

    d = df.copy()
    d["검색수"] = _to_numeric_series(d["검색수"]).fillna(0).astype(int)
    d["구매수"] = _to_numeric_series(d["구매수"]).fillna(0).astype(int)

    d = d[(d["검색수"] >= min_searches) & (d["구매수"] == 0)]
    if d.empty:
        return pd.DataFrame(columns=["키워드", "검색수", "구매수", "CVR(%)"])

    d = d.sort_values("검색수", ascending=False).head(limit)
    return d[["키워드", "검색수", "구매수", "CVR(%)"]]


def src_device_split_yesterday() -> pd.DataFrame:
    try:
        df = ga_run_report(
            dimensions=["deviceCategory"],
            metrics=["sessions", "transactions", "purchaseRevenue"],
            start_date="yesterday",
            end_date="yesterday",
            limit=0,
        )
    except Exception:
        return pd.DataFrame(columns=["디바이스", "UV", "구매수", "매출(만원)", "CVR(%)", "AOV(원)"])

    if df.empty:
        return pd.DataFrame(columns=["디바이스", "UV", "구매수", "매출(만원)", "CVR(%)", "AOV(원)"])

    df.rename(columns={
        "deviceCategory": "디바이스",
        "sessions": "UV",
        "transactions": "구매수",
        "purchaseRevenue": "매출(원)",
    }, inplace=True)

    df["UV"] = _to_numeric_series(df["UV"]).fillna(0).astype(int)
    df["구매수"] = _to_numeric_series(df["구매수"]).fillna(0).astype(int)
    df["매출(원)"] = _to_numeric_series(df["매출(원)"]).fillna(0.0).astype(float)

    df["매출(만원)"] = (df["매출(원)"] / 10_000).round(1)
    df["CVR(%)"] = (df["구매수"] / df["UV"].replace(0, pd.NA) * 100)
    df["CVR(%)"] = pd.to_numeric(df["CVR(%)"], errors="coerce").fillna(0.0).round(2)

    df["AOV(원)"] = (df["매출(원)"] / df["구매수"].replace(0, pd.NA))
    df["AOV(원)"] = pd.to_numeric(df["AOV(원)"], errors="coerce").fillna(0.0).round(0).astype(int)

    df = df.sort_values("UV", ascending=False)
    return df[["디바이스", "UV", "구매수", "매출(만원)", "CVR(%)", "AOV(원)"]]


def src_funnel_by_device_yesterday() -> pd.DataFrame:
    want = ["view_item", "add_to_cart", "begin_checkout", "purchase"]
    try:
        df = ga_run_report(
            dimensions=["deviceCategory", "eventName"],
            metrics=["eventCount"],
            start_date="yesterday",
            end_date="yesterday",
            limit=0,
        )
    except Exception:
        return pd.DataFrame(columns=["디바이스", "PDP→Cart(%)", "Cart→Checkout(%)", "Checkout→Purchase(%)"])

    if df.empty:
        return pd.DataFrame(columns=["디바이스", "PDP→Cart(%)", "Cart→Checkout(%)", "Checkout→Purchase(%)"])

    df = df[df["eventName"].isin(want)].copy()
    if df.empty:
        return pd.DataFrame(columns=["디바이스", "PDP→Cart(%)", "Cart→Checkout(%)", "Checkout→Purchase(%)"])

    df["eventCount"] = _to_numeric_series(df["eventCount"]).fillna(0).astype(int)

    pivot = df.pivot_table(
        index="deviceCategory", columns="eventName", values="eventCount",
        aggfunc="sum", fill_value=0
    ).reset_index()

    pivot.columns.name = None
    pivot = pivot.reset_index()
    pivot.rename(columns={"deviceCategory": "디바이스"}, inplace=True)

    def rate(a, b):
        if b <= 0:
            return 0.0
        return round(a / b * 100, 1)

    pivot["PDP→Cart(%)"] = pivot.apply(lambda r: rate(r.get("add_to_cart", 0), r.get("view_item", 0)), axis=1)
    pivot["Cart→Checkout(%)"] = pivot.apply(lambda r: rate(r.get("begin_checkout", 0), r.get("add_to_cart", 0)), axis=1)
    pivot["Checkout→Purchase(%)"] = pivot.apply(lambda r: rate(r.get("purchase", 0), r.get("begin_checkout", 0)), axis=1)

    out = pivot[["디바이스", "PDP→Cart(%)", "Cart→Checkout(%)", "Checkout→Purchase(%)"]].copy()
    out = out.sort_values("디바이스")
    return out


def src_top_products_ga(limit: int = 200) -> pd.DataFrame:
    base = ga_run_report(
        dimensions=["itemName"],
        metrics=["itemsPurchased", "itemRevenue"],
        start_date="yesterday",
        end_date="yesterday",
        limit=limit,
    )
    if base.empty:
        return pd.DataFrame(columns=PRODUCT_COLS)

    base.rename(columns={"itemName": "상품명", "itemsPurchased": "구매수", "itemRevenue": "매출(원)"}, inplace=True)
    base["구매수"] = _to_numeric_series(base["구매수"]).fillna(0).astype(int)
    base["매출(원)"] = _to_numeric_series(base["매출(원)"]).fillna(0.0).astype(float)

    views = pd.DataFrame(columns=["상품명", "상품조회수"])
    candidates = []
    if GA_ITEM_VIEW_METRIC:
        candidates.append(GA_ITEM_VIEW_METRIC)
    for m in ["itemsViewed", "itemViews", "view_item_event_count", "eventCount"]:
        if m not in candidates:
            candidates.append(m)

    for metric_name in candidates:
        try:
            raw = ga_run_report(
                dimensions=["itemName"],
                metrics=[metric_name],
                start_date="yesterday",
                end_date="yesterday",
                limit=limit,
            )
            if raw is not None and not raw.empty and metric_name in raw.columns:
                tmp = raw[["itemName", metric_name]].copy()
                tmp.rename(columns={"itemName": "상품명", metric_name: "상품조회수"}, inplace=True)
                tmp["상품조회수"] = _to_numeric_series(tmp["상품조회수"]).fillna(0).astype(int)
                views = tmp
                print(f"[INFO] 상품조회수 메트릭 '{metric_name}' 사용")
                break
        except Exception as e:
            print(f"[WARN] 상품조회수 메트릭 '{metric_name}' 조회 실패:", e)

    df = base.merge(views, on="상품명", how="left") if not views.empty else base.assign(상품조회수=0)
    df["상품조회수"] = _to_numeric_series(df["상품조회수"]).fillna(0).astype(int)

    df["매출(만원)"] = (df["매출(원)"] / 10_000).round(1)

    def _calc_cvr(row):
        v = row.get("상품조회수", 0)
        b = row.get("구매수", 0)
        if v <= 0:
            return 0.00
        return round((b / v) * 100, 2)

    df["CVR(%)"] = df.apply(_calc_cvr, axis=1)
    df = df.sort_values(["상품조회수", "매출(원)"], ascending=[False, False]).head(limit)
    return df[PRODUCT_COLS]


def src_top_pages_ga(limit: int = 10) -> pd.DataFrame:
    df = ga_run_report(
        dimensions=["pagePathPlusQueryString"],
        metrics=["screenPageViews"],
        start_date="yesterday",
        end_date="yesterday",
        limit=limit,
    )
    if df.empty:
        return pd.DataFrame(columns=["페이지", "페이지뷰"])

    df.rename(columns={"pagePathPlusQueryString": "페이지", "screenPageViews": "페이지뷰"}, inplace=True)
    df["페이지뷰"] = _to_numeric_series(df["페이지뷰"]).fillna(0).astype(int)
    df = df.sort_values("페이지뷰", ascending=False).head(limit)
    return df[["페이지", "페이지뷰"]]


# =====================================================================
# 4.5) 전일 대비 Δ merge 유틸
# =====================================================================

def _add_delta_cols(curr: pd.DataFrame, prev: pd.DataFrame, key_cols: list, metric_cols: list, mode: str = "pct"):
    if curr is None or curr.empty:
        return curr

    out = curr.copy()

    if prev is None or prev.empty:
        for m in metric_cols:
            out[f"{m} Δ"] = ""
        return out

    c = curr.copy()
    p = prev.copy()

    keep_prev = key_cols + [m for m in metric_cols if m in p.columns]
    p = p[keep_prev].copy()
    p.rename(columns={m: f"{m}__prev" for m in metric_cols if m in p.columns}, inplace=True)

    out = c.merge(p, on=key_cols, how="left")

    for m in metric_cols:
        if m in out.columns:
            out[m] = _to_numeric_series(out[m])
        prev_col = f"{m}__prev"
        if prev_col in out.columns:
            out[prev_col] = _to_numeric_series(out[prev_col])

    for m in metric_cols:
        prev_col = f"{m}__prev"
        if prev_col not in out.columns or m not in out.columns:
            out[f"{m} Δ"] = ""
            continue

        if mode == "pp":
            delta = (out[m] - out[prev_col])
            delta = pd.to_numeric(delta, errors="coerce").round(2)
            out[f"{m} Δ"] = delta.map(lambda x: "" if pd.isna(x) else f"{x:+.2f}p")
        else:
            denom = out[prev_col].replace(0, pd.NA)
            delta = (out[m] - out[prev_col]) / denom * 100
            delta = pd.to_numeric(delta, errors="coerce").round(1)
            out[f"{m} Δ"] = delta.map(lambda x: "" if pd.isna(x) else f"{x:+.1f}%")

        out.drop(columns=[prev_col], inplace=True)

    return out


# =====================================================================
# 5) KPI & 시그널
# =====================================================================

def _channel_uv_for_day(day_keyword: str):
    df = ga_run_report(
        dimensions=["sessionDefaultChannelGroup"],
        metrics=["sessions"],
        start_date=day_keyword,
        end_date=day_keyword,
    )
    if df is None or df.empty:
        return {"total_uv": 0, "organic_uv": 0, "nonorganic_uv": 0, "organic_share": 0.0}

    df = df.copy()
    df["sessions"] = _to_numeric_series(df["sessions"]).fillna(0).astype(int)

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


def build_core_kpi():
    kpi_today = src_kpi_one_day("yesterday", "yesterday")
    kpi_ld = src_kpi_one_day("2daysAgo", "2daysAgo")
    kpi_prev = src_kpi_one_day("8daysAgo", "8daysAgo")
    kpi_yoy = src_kpi_one_day("366daysAgo", "366daysAgo")

    rev_today, rev_ld, rev_prev, rev_yoy = (kpi_today["purchaseRevenue"], kpi_ld["purchaseRevenue"], kpi_prev["purchaseRevenue"], kpi_yoy["purchaseRevenue"])
    uv_today, uv_ld, uv_prev, uv_yoy = (kpi_today["sessions"], kpi_ld["sessions"], kpi_prev["sessions"], kpi_yoy["sessions"])
    ord_today, ord_ld, ord_prev, ord_yoy = (kpi_today["transactions"], kpi_ld["transactions"], kpi_prev["transactions"], kpi_yoy["transactions"])
    new_today, new_ld, new_prev, new_yoy = (kpi_today["newUsers"], kpi_ld["newUsers"], kpi_prev["newUsers"], kpi_yoy["newUsers"])

    cvr_today = (ord_today / uv_today * 100) if uv_today else 0.0
    cvr_ld = (ord_ld / uv_ld * 100) if uv_ld else 0.0
    cvr_prev = (ord_prev / uv_prev * 100) if uv_prev else 0.0
    cvr_yoy = (ord_yoy / uv_yoy * 100) if uv_yoy else 0.0

    aov_today = (rev_today / ord_today) if ord_today else 0.0
    aov_ld = (rev_ld / ord_ld) if ord_ld else 0.0
    aov_prev = (rev_prev / ord_prev) if ord_prev else 0.0
    aov_yoy = (rev_yoy / ord_yoy) if ord_yoy else 0.0

    ch_today = _channel_uv_for_day("yesterday")
    ch_ld = _channel_uv_for_day("2daysAgo")
    ch_prev = _channel_uv_for_day("8daysAgo")
    ch_yoy = _channel_uv_for_day("366daysAgo")

    return {
        "date_label": format_date_label(kpi_today["date"]) if kpi_today["date"] else "어제",

        "revenue_today": rev_today, "revenue_ld": rev_ld, "revenue_prev": rev_prev, "revenue_yoy": rev_yoy,
        "revenue_ld_pct": pct_change(rev_today, rev_ld), "revenue_lw_pct": pct_change(rev_today, rev_prev), "revenue_ly_pct": pct_change(rev_today, rev_yoy),

        "uv_today": uv_today, "uv_ld": uv_ld, "uv_prev": uv_prev, "uv_yoy": uv_yoy,
        "uv_ld_pct": pct_change(uv_today, uv_ld), "uv_lw_pct": pct_change(uv_today, uv_prev), "uv_ly_pct": pct_change(uv_today, uv_yoy),

        "orders_today": ord_today, "orders_ld": ord_ld, "orders_prev": ord_prev, "orders_yoy": ord_yoy,
        "orders_ld_pct": pct_change(ord_today, ord_ld), "orders_lw_pct": pct_change(ord_today, ord_prev), "orders_ly_pct": pct_change(ord_today, ord_yoy),

        "cvr_today": round(cvr_today, 2), "cvr_ld": round(cvr_ld, 2), "cvr_prev": round(cvr_prev, 2), "cvr_yoy": round(cvr_yoy, 2),
        "cvr_ld_pct": pct_change(cvr_today, cvr_ld), "cvr_lw_pct": pct_change(cvr_today, cvr_prev), "cvr_ly_pct": pct_change(cvr_today, cvr_yoy),

        "aov_today": aov_today, "aov_ld": aov_ld, "aov_prev": aov_prev, "aov_yoy": aov_yoy,
        "aov_ld_pct": pct_change(aov_today, aov_ld), "aov_lw_pct": pct_change(aov_today, aov_prev), "aov_ly_pct": pct_change(aov_today, aov_yoy),

        "new_today": new_today, "new_ld": new_ld, "new_prev": new_prev, "new_yoy": new_yoy,
        "new_ld_pct": pct_change(new_today, new_ld), "new_lw_pct": pct_change(new_today, new_prev), "new_ly_pct": pct_change(new_today, new_yoy),

        "organic_uv_today": ch_today["organic_uv"], "organic_uv_ld": ch_ld["organic_uv"], "organic_uv_prev": ch_prev["organic_uv"], "organic_uv_yoy": ch_yoy["organic_uv"],
        "organic_uv_ld_pct": pct_change(ch_today["organic_uv"], ch_ld["organic_uv"]),
        "organic_uv_lw_pct": pct_change(ch_today["organic_uv"], ch_prev["organic_uv"]),
        "organic_uv_ly_pct": pct_change(ch_today["organic_uv"], ch_yoy["organic_uv"]),

        "nonorganic_uv_today": ch_today["nonorganic_uv"], "nonorganic_uv_ld": ch_ld["nonorganic_uv"], "nonorganic_uv_prev": ch_prev["nonorganic_uv"], "nonorganic_uv_yoy": ch_yoy["nonorganic_uv"],
        "nonorganic_uv_ld_pct": pct_change(ch_today["nonorganic_uv"], ch_ld["nonorganic_uv"]),
        "nonorganic_uv_lw_pct": pct_change(ch_today["nonorganic_uv"], ch_prev["nonorganic_uv"]),
        "nonorganic_uv_ly_pct": pct_change(ch_today["nonorganic_uv"], ch_yoy["nonorganic_uv"]),

        "organic_share_today": ch_today["organic_share"], "organic_share_ld": ch_ld["organic_share"], "organic_share_prev": ch_prev["organic_share"], "organic_share_yoy": ch_yoy["organic_share"],
        "organic_share_ld_pct": pct_change(ch_today["organic_share"], ch_ld["organic_share"]),
        "organic_share_lw_pct": pct_change(ch_today["organic_share"], ch_prev["organic_share"]),
        "organic_share_ly_pct": pct_change(ch_today["organic_share"], ch_yoy["organic_share"]),
    }


def build_signals(kpi, funnel_rate_df, traffic_df, search_df):
    signals = []

    if kpi["revenue_lw_pct"] > 0 and kpi["cvr_lw_pct"] > 0:
        signals.append(f"매출이 전주 동일 요일 대비 {kpi['revenue_lw_pct']:.1f}% ↑, CVR은 {kpi['cvr_lw_pct']:.1f}%p 개선되었습니다.")
    elif kpi["revenue_lw_pct"] < 0 and kpi["uv_lw_pct"] < 0:
        signals.append(f"매출({kpi['revenue_lw_pct']:.1f}%)과 UV({kpi['uv_lw_pct']:.1f}%)가 함께 감소해 상단 퍼널 유입 점검이 필요합니다.")
    else:
        signals.append(f"매출 {kpi['revenue_lw_pct']:.1f}%, UV {kpi['uv_lw_pct']:.1f}%, CVR {kpi['cvr_lw_pct']:.1f}%p 변동을 보였습니다.")

    if funnel_rate_df is not None and not funnel_rate_df.empty:
        high_drop = funnel_rate_df[funnel_rate_df["전환율(%)"] < funnel_rate_df["벤치마크(전환 최소)"]]
        if not high_drop.empty:
            names = ", ".join(high_drop["구간"].tolist())
            signals.append(f"퍼널 기준 이탈이 큰 구간은 {names}로, 해당 단계 UI/혜택/카피 점검이 우선입니다.")
        else:
            signals.append("퍼널 전환율은 설정한 벤치마크 이상으로 전반적으로 안정적입니다.")

    if traffic_df is not None and not traffic_df.empty:
        top = traffic_df.iloc[0]
        signals.append(f"유입은 {top['소스']} 채널(UV {int(top['UV']):,}명, CVR {float(top['CVR(%)']):.2f}%) 비중이 가장 큽니다.")

    if search_df is not None and not search_df.empty:
        bad = search_df[search_df["CVR(%)"] < SEARCH_CVR_MIN]
        if not bad.empty:
            top_bad = bad.head(2)["키워드"].tolist()
            signals.append(f"저전환 검색어(CVR {SEARCH_CVR_MIN}% 미만)는 {', '.join(top_bad)} 등이 있어 결과 보완이 필요합니다.")

    fallback = [
        "· 오늘은 전반적인 트렌드를 중심으로 지표를 확인해 주세요.",
        "· 주요 채널·퍼널 구간·상품 성과를 함께 보면서 액션 포인트를 잡을 수 있습니다.",
    ]
    while len(signals) < 4:
        signals.append(fallback[len(signals) % len(fallback)])
    return signals[:4]


def build_actions(kpi, funnel_rate_df, traffic_df, search_df):
    actions = []

    if kpi["revenue_lw_pct"] < 0 and kpi["uv_lw_pct"] < 0:
        actions.append("매출·UV가 동반 하락 중이므로 상단 퍼널 신규 유입 캠페인(소재·입찰·예산)을 우선 점검합니다.")
    elif kpi["cvr_lw_pct"] < 0:
        actions.append("CVR이 전주 대비 하락해 모바일 장바구니·체크아웃 구간의 전환율과 UX를 집중적으로 확인합니다.")
    else:
        actions.append("성과가 좋은 채널/소재의 예산을 소폭 상향해 상승 구간을 더 밀어주는 실험을 진행합니다.")

    if funnel_rate_df is not None and not funnel_rate_df.empty:
        high_drop = funnel_rate_df[funnel_rate_df["전환율(%)"] < funnel_rate_df["벤치마크(전환 최소)"]]
        if not high_drop.empty:
            actions.append("이탈이 큰 퍼널 구간의 배송비·쿠폰·CTA 카피를 이번 주 안에 최소 1개 이상 테스트합니다.")
        else:
            actions.append("퍼널이 안정적인 편이므로 신규 유입 확대 및 VIP 재구매 쪽으로 테스트 리소스를 배분합니다.")
    else:
        actions.append("퍼널 데이터가 부족해 우선 전체 전환율 흐름을 모니터링하면서, 채널/상품 단위의 이상만 체크합니다.")

    if traffic_df is not None and not traffic_df.empty:
        top = traffic_df.iloc[0]
        actions.append(f"{top['소스']} 채널의 성과 좋은 소재를 기준으로 유사 카피·이미지를 다른 채널에도 확장 테스트합니다.")

    if search_df is not None and not search_df.empty:
        bad = search_df[search_df["CVR(%)"] < SEARCH_CVR_MIN]
        if not bad.empty:
            actions.append("저전환 검색어의 노출 상품/필터를 재구성하거나, 상세 설명·가격 정책을 조정하는 안을 검토합니다.")
        else:
            actions.append("상위 검색어 기준으로 기획전/컬렉션 페이지를 추가 구성해 전환을 더 끌어올릴 수 있는지 테스트합니다.")

    while len(actions) < 4:
        actions.append("오늘 눈에 띄는 채널/상품 1~2개를 선정해 소규모 예산으로 실험을 바로 실행합니다.")
    return actions[:4]


# =====================================================================
# 6) EXTRA 섹션 HTML 헬퍼
# =====================================================================

def _table_style_replace(html: str) -> str:
    """
    메일 클라이언트에서 글자 세로깨짐/한글 단어 단위 유지.
    """
    html = html.replace('<table border="0" class="dataframe">', '<table style="width:100%; border-collapse:collapse; font-size:10px; table-layout:fixed;">')
    html = html.replace('<tr style="text-align: right;">', '<tr style="background:#f4f6fb; text-align:left;">')
    html = html.replace(
        "<th>",
        "<th style=\"padding:3px 6px; border-bottom:1px solid #e1e4f0; text-align:left; font-weight:600; color:#555;"
        "word-break:keep-all; white-space:normal; overflow-wrap:anywhere;\">"
    )
    html = html.replace(
        "<td>",
        "<td style=\"padding:3px 6px; border-bottom:1px solid #f1f3fa; text-align:left; color:#333;"
        "word-break:keep-all; white-space:normal; overflow-wrap:anywhere;\">"
    )
    return html


def df_to_html_box_extra(title: str, subtitle: str, df: pd.DataFrame, max_rows: Optional[int] = None) -> str:
    if df is None or df.empty:
        table_html = "<p style='color:#999;font-size:11px;margin:4px 0 0 0;'>데이터 없음</p>"
    else:
        d = df.copy()
        if max_rows is not None:
            d = d.head(max_rows)
        inner = d.to_html(index=False, border=0, justify="left", escape=False)
        table_html = _table_style_replace(inner)

    return f"""<table width="100%" cellpadding="0" cellspacing="0"
       style="background:#ffffff; border-radius:12px;
              border:1px solid #e1e7f5; box-shadow:0 3px 10px rgba(0,0,0,0.03);
              padding:8px 10px; border-collapse:separate;">
  <tr><td>
    <div style="font-size:11px; font-weight:600; color:#004a99; margin-bottom:3px;">
      {title}
    </div>
    <div style="font-size:10px; color:#777; margin-bottom:6px; line-height:1.35;">
      {subtitle}
    </div>
    {table_html}
  </td></tr>
</table>"""


def build_extra_sections_html(
    organic_engines_df: pd.DataFrame | None,
    organic_detail_df: pd.DataFrame | None,
    coupon_df: pd.DataFrame | None,
    search_zero_buy_df: pd.DataFrame | None,
    device_split_df: pd.DataFrame | None,
    device_funnel_df: pd.DataFrame | None,
) -> str:
    blocks: List[str] = []

    # ✅ 03: 두 카드(엔진별/소스미디엄) 한 줄(50:50)
    organic_cards = []
    if organic_engines_df is not None and not organic_engines_df.empty:
        organic_cards.append(df_to_html_box_extra(
            "오가닉 검색 유입 (검색엔진별)",
            "어제 Organic Search 유입을 검색엔진(소스)별로 나눈 데이터입니다.",
            organic_engines_df,
            max_rows=10,
        ))
    else:
        organic_cards.append("")

    if organic_detail_df is not None and not organic_detail_df.empty:
        organic_cards.append(df_to_html_box_extra(
            "오가닉 서치 상세 (Source / Medium)",
            "Organic Search를 Source/Medium 조합으로 더 자세히 쪼갠 데이터입니다.",
            organic_detail_df,
            max_rows=15,
        ))
    else:
        organic_cards.append("")

    if any(c.strip() for c in organic_cards):
        blocks.append(f"""<div style="font-size:11px; letter-spacing:0.12em; color:#6d7a99; margin-top:22px; margin-bottom:8px;">
  03 · ORGANIC SEARCH DETAIL
</div>
<table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:4px;">
  <tr>
    <td width="50%" valign="top" style="padding:4px 6px 8px 0;">{organic_cards[0]}</td>
    <td width="50%" valign="top" style="padding:4px 0 8px 6px;">{organic_cards[1]}</td>
  </tr>
</table>""")

    # 04 OPS
    ops_cards: List[str] = []

    if coupon_df is not None and not coupon_df.empty:
        ops_cards.append(df_to_html_box_extra(
            "쿠폰/프로모션 사용 요약",
            "어제 기준 쿠폰별 구매/매출 기여 (not set 제외).",
            coupon_df,
            max_rows=12,
        ))

    if search_zero_buy_df is not None and not search_zero_buy_df.empty:
        ops_cards.append(df_to_html_box_extra(
            "검색했지만 구매 0 키워드",
            "검색수는 높은데 구매가 0인 키워드 — 결과/필터/상품구성 점검 우선순위.",
            search_zero_buy_df,
            max_rows=12,
        ))

    if device_split_df is not None and not device_split_df.empty:
        ops_cards.append(df_to_html_box_extra(
            "디바이스 성과 스플릿",
            "deviceCategory별 UV/구매/매출/CVR/AOV 요약.",
            device_split_df,
            max_rows=10,
        ))

    if device_funnel_df is not None and not device_funnel_df.empty:
        ops_cards.append(df_to_html_box_extra(
            "디바이스별 퍼널 전환율",
            "eventCount 기준 PDP→Cart / Cart→Checkout / Checkout→Purchase.",
            device_funnel_df,
            max_rows=10,
        ))

    if ops_cards:
        grid_rows = []
        for i in range(0, len(ops_cards), 2):
            left = ops_cards[i]
            right = ops_cards[i + 1] if i + 1 < len(ops_cards) else ""
            grid_rows.append(f"""
  <tr>
    <td width="50%" valign="top" style="padding:4px 6px 8px 0;">{left}</td>
    <td width="50%" valign="top" style="padding:4px 0 8px 6px;">{right}</td>
  </tr>
""")
        blocks.append(f"""<div style="font-size:11px; letter-spacing:0.12em; color:#6d7a99; margin-top:22px; margin-bottom:8px;">
  04 · OPS CHECK (COUPON · SEARCH · DEVICE)
</div>
<table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:4px;">
{''.join(grid_rows)}
</table>""")

    return "\n\n".join(blocks) if blocks else ""


# =====================================================================
# 7) HTML 템플릿
# =====================================================================

def compose_html_daily(
    kpi,
    funnel_counts_df,
    funnel_rate_df,
    traffic_df,
    hourly_df,
    search_df,
    products_top_df,
    products_lowconv_df,
    products_hiconv_df,
    pages_df,
):
    def df_to_html_box(title, subtitle, df, max_rows=None):
        if df is None or df.empty:
            table_html = "<p style='color:#999;font-size:11px;margin:4px 0 0 0;'>데이터 없음</p>"
        else:
            d = df.copy()
            if max_rows is not None:
                d = d.head(max_rows)
            inner = d.to_html(index=False, border=0, justify="left", escape=False)
            table_html = _table_style_replace(inner)

        return f"""
<table width="100%" cellpadding="0" cellspacing="0"
       style="background:#ffffff; border-radius:12px;
              border:1px solid #e1e7f5; box-shadow:0 3px 10px rgba(0,0,0,0.03);
              padding:6px 8px; border-collapse:separate;">
  <tr><td>
    <div style="font-size:11px; font-weight:600; color:#224; margin-bottom:2px;">
      {title}
    </div>
    <div style="font-size:10px; color:#888; margin-bottom:6px; line-height:1.4;">
      {subtitle}
    </div>
    {table_html}
  </td></tr>
</table>
"""

    # ---- 시간대별 카드 (원본 유지) ----
    def build_hourly_card(df):
        if df is None or df.empty:
            body_html = "<p style='color:#999;font-size:11px;margin:4px 0 0 0;'>데이터 없음</p>"
            return f"""
<table width="100%" cellpadding="0" cellspacing="0"
       style="background:#ffffff; border-radius:12px;
              border:1px solid #e1e7f5; box-shadow:0 3px 10px rgba(0,0,0,0.03);
              padding:10px 12px; border-collapse:separate; margin-top:10px;">
  <tr><td>
    <div style="font-size:11px; font-weight:600; color:#224; margin-bottom:2px;">
      시간대별 트래픽 & 매출 (막대)
    </div>
    <div style="font-size:10px; color:#888; margin-bottom:6px; line-height:1.4;">
      어제 0~23시 기준 — 위에는 트래픽(세션), 아래에는 매출을 시간대별 막대그래프로 비교합니다.
    </div>
    {body_html}
  </td></tr>
</table>
"""

        df = df.copy()
        df["세션수"] = _to_numeric_series(df["세션수"]).fillna(0).astype(int)
        df["매출"] = _to_numeric_series(df["매출"]).fillna(0.0).astype(float)
        df = df.sort_values("시간_숫자")

        hours = df["시간_숫자"].tolist()
        sessions = df["세션수"].tolist()
        revenue = df["매출"].tolist()

        max_sess = max(sessions) if sessions and max(sessions) > 0 else 1
        max_rev = max(revenue) if revenue and max(revenue) > 0 else 1
        max_bar_height = 80

        labels_row = "".join(
            f"<td style='font-size:9px; color:#666; padding-top:2px; text-align:center;'>{int(h):02d}</td>"
            for h in hours
        )

        sess_bar_row = ""
        for s in sessions:
            ratio = s / max_sess if max_sess > 0 else 0
            h = max(3, int(ratio * max_bar_height))
            sess_bar_row += f"""
<td style="vertical-align:bottom; text-align:center;">
  <div style="margin:0 auto; width:10px; height:{h}px;
              border-radius:999px 999px 0 0; background:#2563eb;"></div>
</td>
"""

        traffic_chart_html = f"""
<div style="font-size:10px; color:#555; margin-bottom:4px;">
  · 트래픽 (세션수, 막대)
</div>
<table cellpadding="0" cellspacing="0" style="width:100%; border-collapse:collapse;">
  <tr style="height:{max_bar_height+15}px; vertical-align:bottom;">
    {sess_bar_row}
  </tr>
  <tr>
    {labels_row}
  </tr>
</table>
"""

        rev_bar_row = ""
        for r in revenue:
            ratio = r / max_rev if max_rev > 0 else 0
            h = max(3, int(ratio * max_bar_height))
            rev_bar_row += f"""
<td style="vertical-align:bottom; text-align:center;">
  <div style="margin:0 auto; width:10px; height:{h}px;
              border-radius:999px 999px 0 0; background:#fb923c;"></div>
</td>
"""

        revenue_chart_html = f"""
<div style="font-size:10px; color:#555; margin-top:12px; margin-bottom:4px;">
  · 매출 (원, 막대)
</div>
<table cellpadding="0" cellspacing="0" style="width:100%; border-collapse:collapse;">
  <tr style="height:{max_bar_height+15}px; vertical-align:bottom;">
    {rev_bar_row}
  </tr>
  <tr>
    {labels_row}
  </tr>
</table>
"""

        return f"""
<table width="100%" cellpadding="0" cellspacing="0"
       style="background:#ffffff; border-radius:12px;
              border:1px solid #e1e7f5; box-shadow:0 3px 10px rgba(0,0,0,0.03);
              padding:10px 12px; border-collapse:separate; margin-top:10px;">
  <tr><td>
    <div style="font-size:11px; font-weight:600; color:#224; margin-bottom:2px;">
      시간대별 트래픽 & 매출 (막대)
    </div>
    <div style="font-size:10px; color:#888; margin-bottom:6px; line-height:1.4;">
      어제 0~23시 기준 — 위에는 트래픽(세션), 아래에는 매출을 시간대별 막대그래프로 비교합니다.
    </div>
    {traffic_chart_html}
    {revenue_chart_html}
  </td></tr>
</table>
"""

    signals_list = build_signals(kpi, funnel_rate_df, traffic_df, search_df)
    actions_list = build_actions(kpi, funnel_rate_df, traffic_df, search_df)

    insight_items_html = "".join(f"<li style='margin-bottom:3px;'>{s}</li>" for s in signals_list)
    action_items_html = "".join(f"<li style='margin-bottom:3px;'>{s}</li>" for s in actions_list)

    insight_card_html = f"""
<table width="100%" cellpadding="0" cellspacing="0"
       style="background:#ffffff; border-radius:14px;
              border:1px solid #e1e7f5; box-shadow:0 4px 12px rgba(0,0,0,0.04);
              padding:10px 12px; border-collapse:separate;">
  <tr><td>
    <div style="font-size:11px; font-weight:600; color:#004a99; margin-bottom:4px;">
      오늘의 인사이트
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
      오늘 취할 액션
    </div>
    <ul style="margin:0; padding-left:16px; font-size:11px; color:#555; line-height:1.6;">
      {action_items_html}
    </ul>
  </td></tr>
</table>
"""

    insight_action_html = f"""
<table width="100%" cellpadding="0" cellspacing="0"
       style="border-collapse:separate; border-spacing:8px 10px; margin-top:14px;">
  <tr>
    <td width="50%" valign="top">{insight_card_html}</td>
    <td width="50%" valign="top">{action_card_html}</td>
  </tr>
</table>
"""

    funnel_counts_box = df_to_html_box(
        "퍼널 전환 (view → cart → checkout → purchase)",
        "단계별 이벤트 수 기준 전환 흐름입니다. (전일 대비는 ‘이벤트 수’ 증감률)",
        funnel_counts_df,
        max_rows=None,
    )

    funnel_rate_box = df_to_html_box(
        "퍼널 이탈/전환율 & 벤치마크 비교",
        "위험 기준: 전환율이 ‘전환 최소(벤치마크)’ 미만인 경우 (Δ는 전일 대비 %p)",
        funnel_rate_df.assign(
            위험=lambda d: d.apply(
                lambda r: "위험" if r["전환율(%)"] < r["벤치마크(전환 최소)"] else "",
                axis=1,
            )
        ),
        max_rows=None,
    )

    traffic_box = df_to_html_box(
        "채널별 유입 & 오가닉",
        "채널별 UV · 구매수 · 신규 방문자 · CVR입니다. (전일 대비 Δ 포함)",
        traffic_df,
        max_rows=None,
    )

    pages_box = df_to_html_box(
        "많이 본 페이지 TOP 10",
        "페이지뷰 기준 상위 페이지입니다.",
        pages_df,
        max_rows=10,
    )

    products_top_box = df_to_html_box(
        "지금 치고 올라오는 상품",
        "조회수·매출 기준 상위 상품입니다.",
        products_top_df[PRODUCT_COLS],
        max_rows=7,
    )

    products_low_box = df_to_html_box(
        "조회는 많은데 구매 전환이 낮은 상품",
        "조회 TOP 30 중 CVR 하위 상품입니다.",
        products_lowconv_df[PRODUCT_COLS] if not products_lowconv_df.empty else products_lowconv_df,
        max_rows=5,
    )

    products_hi_box = df_to_html_box(
        "조회는 적지만 구매 전환이 좋은 상품",
        "조회 하위 구간 중 CVR 상위 상품입니다.",
        products_hiconv_df[PRODUCT_COLS] if not products_hiconv_df.empty else products_hiconv_df,
        max_rows=5,
    )

    search_top_box = df_to_html_box(
        "온사이트 검색 상위 키워드",
        "검색수 기준 상위 키워드와 CVR입니다. (전일 대비 Δ 포함)",
        search_df,
        max_rows=10,
    )

    hourly_box = build_hourly_card(hourly_df)

    # ✅ 02 섹션 폭 조정: (products_hi 좁게 / search_top 넓게)
    section2_grid_html = f"""
<div style="font-size:11px; letter-spacing:0.12em; color:#6d7a99; margin-top:20px; margin-bottom:8px;">
  02 · FUNNEL · TRAFFIC · PRODUCT · SEARCH
</div>
<table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:4px;">
  <tr>
    <td width="50%" valign="top" style="padding:4px 6px 6px 0;">{funnel_counts_box}</td>
    <td width="50%" valign="top" style="padding:4px 0 6px 6px;">{funnel_rate_box}</td>
  </tr>
  <tr>
    <td width="50%" valign="top" style="padding:4px 6px 6px 0;">{traffic_box}</td>
    <td width="50%" valign="top" style="padding:4px 0 6px 6px;">{pages_box}</td>
  </tr>
  <tr>
    <td width="50%" valign="top" style="padding:4px 6px 0 0;">{products_top_box}</td>
    <td width="50%" valign="top" style="padding:4px 0 0 6px;">{products_low_box}</td>
  </tr>
  <tr>
    <td width="35%" valign="top" style="padding:4px 6px 0 0;">{products_hi_box}</td>
    <td width="65%" valign="top" style="padding:4px 0 0 6px;">{search_top_box}</td>
  </tr>
</table>
<div>
  {hourly_box}
</div>
"""

    # ✅ 01 KPI 카드 9개 복구 (3x3)
    def kpi_card(title, value, ld, lw, ly, ld_pct, lw_pct, ly_pct, suffix=""):
        return f"""
<div style="background:#ffffff; border-radius:16px; padding:14px 16px; border:1px solid #e1e7f5;">
  <div style="font-size:11px; color:#777; margin-bottom:4px;">{title}</div>
  <div style="font-size:18px; font-weight:700; margin-bottom:4px; color:#111;">{value}{suffix}</div>
  <div style="font-size:10px; color:#999; margin-bottom:6px; word-break:keep-all; white-space:normal; overflow-wrap:anywhere;">
    LD: {ld} · LW: {lw} · LY: {ly}
  </div>
  <div>
    <span style="display:inline-block; font-size:10px; padding:2px 7px; border-radius:999px; background:#e7f5ec; color:#1b7f4d; margin-right:4px;">LD {ld_pct}</span>
    <span style="display:inline-block; font-size:10px; padding:2px 7px; border-radius:999px; background:#dbeafe; color:#1d4ed8; margin-right:4px;">LW {lw_pct}</span>
    <span style="display:inline-block; font-size:10px; padding:2px 7px; border-radius:999px; background:#fdeaea; color:#c53030;">LY {ly_pct}</span>
  </div>
</div>
"""

    kpi_9_html = f"""
<div style="font-size:11px; letter-spacing:0.12em; color:#6d7a99; margin-top:18px; margin-bottom:10px;">
  01 · EXECUTIVE KPI SNAPSHOT
</div>

<table width="100%" cellpadding="0" cellspacing="0" style="border-collapse:separate; border-spacing:8px 10px;">
  <tr>
    <td width="33.3%" valign="top">
      {kpi_card("매출 (Revenue)", format_money_manwon(kpi['revenue_today']),
                format_money_manwon(kpi['revenue_ld']), format_money_manwon(kpi['revenue_prev']), format_money_manwon(kpi['revenue_yoy']),
                f"{kpi['revenue_ld_pct']:+.1f}%", f"{kpi['revenue_lw_pct']:+.1f}%", f"{kpi['revenue_ly_pct']:+.1f}%")}
    </td>
    <td width="33.3%" valign="top">
      {kpi_card("방문자수 (UV)", f"{kpi['uv_today']:,}명",
                f"{kpi['uv_ld']:,}명", f"{kpi['uv_prev']:,}명", f"{kpi['uv_yoy']:,}명",
                f"{kpi['uv_ld_pct']:+.1f}%", f"{kpi['uv_lw_pct']:+.1f}%", f"{kpi['uv_ly_pct']:+.1f}%")}
    </td>
    <td width="33.3%" valign="top">
      {kpi_card("구매수 (Orders)", f"{kpi['orders_today']:,}건",
                f"{kpi['orders_ld']:,}건", f"{kpi['orders_prev']:,}건", f"{kpi['orders_yoy']:,}건",
                f"{kpi['orders_ld_pct']:+.1f}%", f"{kpi['orders_lw_pct']:+.1f}%", f"{kpi['orders_ly_pct']:+.1f}%")}
    </td>
  </tr>

  <tr>
    <td width="33.3%" valign="top">
      {kpi_card("전환율 (CVR)", f"{kpi['cvr_today']:.2f}%",
                f"{kpi['cvr_ld']:.2f}%", f"{kpi['cvr_prev']:.2f}%", f"{kpi['cvr_yoy']:.2f}%",
                f"{kpi['cvr_ld_pct']:+.1f}%p", f"{kpi['cvr_lw_pct']:+.1f}%p", f"{kpi['cvr_ly_pct']:+.1f}%p")}
    </td>
    <td width="33.3%" valign="top">
      {kpi_card("객단가 (AOV)", format_money(kpi['aov_today']),
                format_money(kpi['aov_ld']), format_money(kpi['aov_prev']), format_money(kpi['aov_yoy']),
                f"{kpi['aov_ld_pct']:+.1f}%", f"{kpi['aov_lw_pct']:+.1f}%", f"{kpi['aov_ly_pct']:+.1f}%")}
    </td>
    <td width="33.3%" valign="top">
      {kpi_card("신규 사용자 (New Users)", f"{kpi['new_today']:,}명",
                f"{kpi['new_ld']:,}명", f"{kpi['new_prev']:,}명", f"{kpi['new_yoy']:,}명",
                f"{kpi['new_ld_pct']:+.1f}%", f"{kpi['new_lw_pct']:+.1f}%", f"{kpi['new_ly_pct']:+.1f}%")}
    </td>
  </tr>

  <tr>
    <td width="33.3%" valign="top">
      {kpi_card("오가닉 UV", f"{kpi['organic_uv_today']:,}명",
                f"{kpi['organic_uv_ld']:,}명", f"{kpi['organic_uv_prev']:,}명", f"{kpi['organic_uv_yoy']:,}명",
                f"{kpi['organic_uv_ld_pct']:+.1f}%", f"{kpi['organic_uv_lw_pct']:+.1f}%", f"{kpi['organic_uv_ly_pct']:+.1f}%")}
    </td>
    <td width="33.3%" valign="top">
      {kpi_card("비오가닉 UV", f"{kpi['nonorganic_uv_today']:,}명",
                f"{kpi['nonorganic_uv_ld']:,}명", f"{kpi['nonorganic_uv_prev']:,}명", f"{kpi['nonorganic_uv_yoy']:,}명",
                f"{kpi['nonorganic_uv_ld_pct']:+.1f}%", f"{kpi['nonorganic_uv_lw_pct']:+.1f}%", f"{kpi['nonorganic_uv_ly_pct']:+.1f}%")}
    </td>
    <td width="33.3%" valign="top">
      {kpi_card("오가닉 비중", f"{kpi['organic_share_today']:.1f}%",
                f"{kpi['organic_share_ld']:.1f}%", f"{kpi['organic_share_prev']:.1f}%", f"{kpi['organic_share_yoy']:.1f}%",
                f"{kpi['organic_share_ld_pct']:+.1f}%", f"{kpi['organic_share_lw_pct']:+.1f}%", f"{kpi['organic_share_ly_pct']:+.1f}%")}
    </td>
  </tr>
</table>
"""

    html = f"""<!DOCTYPE html>
<html lang="ko">
<head>
<meta charset="utf-8">
<title>Columbia Sportswear Korea — Daily eCommerce Performance Digest</title>
</head>
<body style="margin:0; padding:0; background:#f5f7fb; font-family:-apple-system,BlinkMacSystemFont,'Segoe UI','Noto Sans KR',Arial,sans-serif;">

<table role="presentation" width="100%" cellspacing="0" cellpadding="0" style="background:#f5f7fb;">
  <tr>
    <td align="center">
      <table role="presentation" width="900" cellspacing="0" cellpadding="0" style="padding:24px 12px 24px 12px; background:#f5f7fb;">
        <tr>
          <td>

            <table role="presentation" width="100%" cellspacing="0" cellpadding="0"
                   style="background:#ffffff; border-radius:18px; border:1px solid #e6e9ef; box-shadow:0 6px 18px rgba(0,0,0,0.06);">
              <tr>
                <td valign="top" style="padding:18px 20px 16px 20px;">
                  <div style="font-size:18px; font-weight:700; color:#0055a5; margin-bottom:2px;">
                    COLUMBIA SPORTSWEAR KOREA
                  </div>
                  <div style="font-size:13px; color:#555; margin-bottom:8px;">
                    Daily eCommerce Performance Digest
                  </div>
                  <span style="display:inline-block; font-size:11px; padding:4px 10px; border-radius:999px;
                               background:#eaf3ff; color:#0055a5; margin-bottom:6px;">
                    {kpi['date_label']} 기준 (어제 데이터)
                  </span>
                  <div style="font-size:11px; color:#777; margin-top:6px; margin-bottom:2px; line-height:1.6;">
                    매출·UV·CVR 흐름과 퍼널 · 온사이트 검색 · 상품 성과를 한 번에 보는 데일리 요약입니다.
                  </div>
                </td>

                <td valign="top" align="right" style="padding:16px 20px 16px 0%;">
                  <table role="presentation" cellspacing="0" cellpadding="0" align="right" style="margin-bottom:8px;">
                    <tr>
                      <td style="padding:0 3px;">
                        <span style="display:inline-block; font-size:10px; padding:4px 9px; border-radius:999px;
                                     background:#0055a5; color:#ffffff; border:1px solid #0055a5;">
                          DAILY
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
                          FUNNEL
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

{kpi_9_html}

{section2_grid_html}

<div style="margin-top:18px; font-size:10px; color:#99a; text-align:right;">
  Columbia Sportswear Korea · Daily eCommerce Digest · GA4 · Python
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
# 8) 메인
# =====================================================================

def send_daily_digest():
    kpi = build_core_kpi()

    funnel_counts_df, funnel_rate_df = src_funnel_yesterday()
    traffic_df = src_traffic_yesterday()
    search_df = src_search_yesterday(limit=100)
    hourly_df = src_hourly_revenue_traffic()

    funnel_counts_prev_df, funnel_rate_prev_df = src_funnel_day("2daysAgo")
    traffic_prev_df = src_traffic_day("2daysAgo")
    search_prev_df = src_search_day("2daysAgo", limit=100)

    funnel_counts_df = _add_delta_cols(curr=funnel_counts_df, prev=funnel_counts_prev_df, key_cols=["단계"], metric_cols=["수"], mode="pct")
    if funnel_counts_df is not None and not funnel_counts_df.empty:
        funnel_counts_df = funnel_counts_df.rename(columns={"수 Δ": "전일 대비(%)"})

    funnel_rate_df = _add_delta_cols(curr=funnel_rate_df, prev=funnel_rate_prev_df, key_cols=["기준"], metric_cols=["전환율(%)", "이탈율(%)"], mode="pp")
    traffic_df = _add_delta_cols(curr=traffic_df, prev=traffic_prev_df, key_cols=["소스"], metric_cols=["UV", "구매수", "신규 방문자", "CVR(%)"], mode="pct")
    search_df = _add_delta_cols(curr=search_df, prev=search_prev_df, key_cols=["키워드"], metric_cols=["검색수", "구매수", "CVR(%)"], mode="pct")

    products_all = src_top_products_ga(limit=200)
    pages_df = src_top_pages_ga(limit=10)

    organic_engines_df = src_organic_search_engines_yesterday(limit=10)
    organic_detail_df = src_organic_search_detail_source_medium_yesterday(limit=15)

    coupon_df = src_coupon_performance_yesterday(limit=12)
    search_zero_buy_df = src_search_zero_purchase_yesterday(min_searches=20, limit=12)
    device_split_df = src_device_split_yesterday()
    device_funnel_df = src_funnel_by_device_yesterday()

    products_top_df = products_all.sort_values("상품조회수", ascending=False) if not products_all.empty else products_all

    products_lowconv_df = pd.DataFrame(columns=PRODUCT_COLS)
    products_hiconv_df = pd.DataFrame(columns=PRODUCT_COLS)

    if not products_all.empty:
        tmp_top = products_all.sort_values("상품조회수", ascending=False).head(30)
        products_lowconv_df = tmp_top.sort_values("CVR(%)", ascending=True).head(10)

        tmp_low = products_all.sort_values("상품조회수", ascending=True).head(50)
        products_hiconv_df = tmp_low.sort_values("CVR(%)", ascending=False).head(10)

    html = compose_html_daily(
        kpi=kpi,
        funnel_counts_df=funnel_counts_df,
        funnel_rate_df=funnel_rate_df,
        traffic_df=traffic_df,
        hourly_df=hourly_df,
        search_df=search_df,
        products_top_df=products_top_df,
        products_lowconv_df=products_lowconv_df,
        products_hiconv_df=products_hiconv_df,
        pages_df=pages_df,
    )

    critical_reasons = []
    if kpi["cvr_lw_pct"] <= -CVR_DROP_PPTS:
        critical_reasons.append(f"CVR LW 대비 {CVR_DROP_PPTS}p 이상 하락")
    if kpi["revenue_lw_pct"] <= -REVENUE_DROP_PCT:
        critical_reasons.append(f"매출 LW 대비 {REVENUE_DROP_PCT}% 이상 하락")
    if kpi["uv_lw_pct"] <= -UV_DROP_PCT:
        critical_reasons.append(f"UV LW 대비 {UV_DROP_PCT}% 이상 하락")

    if critical_reasons:
        body = " / ".join(critical_reasons)
        body += f"\n\n어제 기준 CVR {kpi['cvr_today']:.2f}%, 매출 {format_money_manwon(kpi['revenue_today'])}, UV {kpi['uv_today']:,}명."
        send_critical_alert("⚠️ [Critical] Columbia Daily 지표 이상 감지", body)

    extra_html = build_extra_sections_html(
        organic_engines_df=organic_engines_df,
        organic_detail_df=organic_detail_df,
        coupon_df=coupon_df,
        search_zero_buy_df=search_zero_buy_df,
        device_split_df=device_split_df,
        device_funnel_df=device_funnel_df,
    )

    if extra_html:
        footer_marker = '<div style="margin-top:18px; font-size:10px; color:#99a; text-align:right;">'
        if footer_marker in html:
            html = html.replace(footer_marker, extra_html + "\n\n" + footer_marker, 1)
        else:
            html = html.replace("</body>", extra_html + "\n</body>", 1)

    subject = "[Daily] Columbia eCommerce Performance Digest"

    jpeg_path = html_to_jpeg(html)
    send_email_html(subject, html, DAILY_RECIPIENTS, jpeg_path=jpeg_path)


if __name__ == "__main__":
    send_daily_digest()

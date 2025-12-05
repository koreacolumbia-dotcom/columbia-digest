#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Columbia Sportswear Korea
Weekly eCommerce Performance Digest (GA4 + HTML Mail)

- Weekly KPI / Funnel / Traffic / Product / Search / Graph & Analysis
- HTML 이메일용 리포트
"""

import os
import smtplib
import base64
import io
from datetime import datetime, timedelta
from typing import Dict, Tuple, List

import pandas as pd
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import DateRange, Dimension, Metric, RunReportRequest
from google.oauth2 import service_account


# =====================================================================
# 0) 환경 변수 / 기본 설정
# =====================================================================

GA4_PROPERTY_ID = os.getenv("GA4_PROPERTY_ID", "").strip()

SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASS = os.getenv("SMTP_PASS", "")

WEEKLY_RECIPIENTS = [
    e.strip() for e in os.getenv("WEEKLY_RECIPIENTS", "").split(",") if e.strip()
]

ALERT_RECIPIENT = os.getenv("ALERT_RECIPIENT", "").strip()

SERVICE_ACCOUNT_JSON = os.getenv("GA4_SERVICE_ACCOUNT_JSON", "")

if SERVICE_ACCOUNT_JSON:
    SERVICE_ACCOUNT_FILE = "/tmp/ga4_service_account.json"
    with open(SERVICE_ACCOUNT_FILE, "w", encoding="utf-8") as f:
        f.write(SERVICE_ACCOUNT_JSON)
else:
    SERVICE_ACCOUNT_FILE = os.getenv("GA4_SERVICE_ACCOUNT_FILE", "")


# =====================================================================
# 1) 유틸
# =====================================================================

def pct_change(curr, prev) -> float:
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
    return f"{round(safe_float(won)):,}원"


def format_money_manwon(won) -> str:
    return f"{round(safe_float(won) / 10_000):,}만원"


def send_email_html(subject: str, html_body: str, recipients):
    if isinstance(recipients, str):
        recipients = [recipients]
    if not recipients:
        print("[WARN] WEEKLY_RECIPIENTS 비어 있음")
        print(html_body[:2000])
        return
    if not (SMTP_USER and SMTP_PASS):
        print("[WARN] SMTP 정보 없음 – HTML만 출력")
        print(html_body[:2000])
        return

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = SMTP_USER
    msg["To"] = ", ".join(recipients)

    plain = "Columbia Weekly eCommerce Digest 입니다. HTML 메일이 보이지 않으면 브라우저에서 확인해주세요."
    msg.attach(MIMEText(plain, "plain", "utf-8"))
    msg.attach(MIMEText(html_body, "html", "utf-8"))

    with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASS)
        server.sendmail(SMTP_USER, recipients, msg.as_string())


def send_critical_alert(subject: str, body_text: str):
    recipient = ALERT_RECIPIENT or SMTP_USER
    if not recipient:
        print("[WARN] ALERT Recipient 없음:", subject)
        return
    html = f"<pre>{body_text}</pre>"
    send_email_html(subject, html, [recipient])


# =====================================================================
# 2) GA4 Client
# =====================================================================

def ga_client() -> BetaAnalyticsDataClient:
    if not GA4_PROPERTY_ID:
        raise SystemExit("GA4_PROPERTY_ID 비어 있음")
    if not SERVICE_ACCOUNT_FILE or not os.path.exists(SERVICE_ACCOUNT_FILE):
        raise SystemExit(f"Service Account 파일 없음: {SERVICE_ACCOUNT_FILE}")
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/analytics.readonly"],
    )
    return BetaAnalyticsDataClient(credentials=creds)


def ga_run_report(dimensions, metrics, start_date, end_date, limit=None) -> pd.DataFrame:
    client = ga_client()
    req = RunReportRequest(
        property=f"properties/{GA4_PROPERTY_ID}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[Dimension(name=d) for d in dimensions],
        metrics=[Metric(name=m) for m in metrics],
        limit=limit or 0,
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
# 3) 날짜 범위
# =====================================================================

def get_week_ranges():
    today = datetime.utcnow().date()
    this_end = today - timedelta(days=1)
    this_start = today - timedelta(days=7)

    last_end = this_start - timedelta(days=1)
    last_start = last_end - timedelta(days=6)

    ly_start = this_start - timedelta(days=365)
    ly_end = this_end - timedelta(days=365)

    return {
        "this": {"start": this_start.isoformat(), "end": this_end.isoformat()},
        "last": {"start": last_start.isoformat(), "end": last_end.isoformat()},
        "ly": {"start": ly_start.isoformat(), "end": ly_end.isoformat()},
        "label": f"{this_start.isoformat()} ~ {this_end.isoformat()}",
    }


# =====================================================================
# 4) 데이터 소스
# =====================================================================

def src_weekly_kpi(start_date: str, end_date: str) -> Dict[str, float]:
    df = ga_run_report(
        dimensions=[],
        metrics=["sessions", "transactions", "purchaseRevenue", "newUsers"],
        start_date=start_date,
        end_date=end_date,
    )
    if df.empty:
        return {"sessions": 0, "transactions": 0, "purchaseRevenue": 0.0, "newUsers": 0}
    row = df.iloc[0]
    return {
        "sessions": safe_int(row["sessions"]),
        "transactions": safe_int(row["transactions"]),
        "purchaseRevenue": safe_float(row["purchaseRevenue"]),
        "newUsers": safe_int(row["newUsers"]),
    }


def _weekly_channel_uv(start_date: str, end_date: str):
    df = ga_run_report(
        dimensions=["sessionDefaultChannelGroup"],
        metrics=["sessions"],
        start_date=start_date,
        end_date=end_date,
    )
    if df.empty:
        return {"total_uv": 0, "organic_uv": 0, "nonorganic_uv": 0, "organic_share": 0.0}
    df["sessions"] = pd.to_numeric(df["sessions"], errors="coerce").fillna(0).astype(int)
    total_uv = int(df["sessions"].sum())
    organic_uv = int(df.loc[df["sessionDefaultChannelGroup"] == "Organic Search", "sessions"].sum())
    nonorganic_uv = total_uv - organic_uv
    share = (organic_uv / total_uv * 100) if total_uv else 0.0
    return {
        "total_uv": total_uv,
        "organic_uv": organic_uv,
        "nonorganic_uv": nonorganic_uv,
        "organic_share": round(share, 1),
    }


def build_weekly_kpi() -> Dict[str, float]:
    r = get_week_ranges()
    t = src_weekly_kpi(r["this"]["start"], r["this"]["end"])
    l = src_weekly_kpi(r["last"]["start"], r["last"]["end"])
    ly = src_weekly_kpi(r["ly"]["start"], r["ly"]["end"])

    ct = _weekly_channel_uv(r["this"]["start"], r["this"]["end"])
    cl = _weekly_channel_uv(r["last"]["start"], r["last"]["end"])
    cly = _weekly_channel_uv(r["ly"]["start"], r["ly"]["end"])

    rev_t, rev_l, rev_ly = t["purchaseRevenue"], l["purchaseRevenue"], ly["purchaseRevenue"]
    uv_t, uv_l, uv_ly = t["sessions"], l["sessions"], ly["sessions"]
    ord_t, ord_l, ord_ly = t["transactions"], l["transactions"], ly["transactions"]
    new_t, new_l, new_ly = t["newUsers"], l["newUsers"], ly["newUsers"]

    cvr_t = (ord_t / uv_t * 100) if uv_t else 0.0
    cvr_l = (ord_l / uv_l * 100) if uv_l else 0.0
    cvr_ly = (ord_ly / uv_ly * 100) if uv_ly else 0.0

    aov_t = (rev_t / ord_t) if ord_t else 0.0
    aov_l = (rev_l / ord_l) if ord_l else 0.0
    aov_ly = (rev_ly / ord_ly) if ord_ly else 0.0

    return {
        "week_label": r["label"],
        "revenue_this": rev_t,
        "revenue_last": rev_l,
        "revenue_ly": rev_ly,
        "revenue_lw_pct": pct_change(rev_t, rev_l),
        "revenue_ly_pct": pct_change(rev_t, rev_ly),
        "uv_this": uv_t,
        "uv_last": uv_l,
        "uv_ly": uv_ly,
        "uv_lw_pct": pct_change(uv_t, uv_l),
        "uv_ly_pct": pct_change(uv_t, uv_ly),
        "orders_this": ord_t,
        "orders_last": ord_l,
        "orders_ly": ord_ly,
        "orders_lw_pct": pct_change(ord_t, ord_l),
        "orders_ly_pct": pct_change(ord_t, ord_ly),
        "cvr_this": round(cvr_t, 2),
        "cvr_last": round(cvr_l, 2),
        "cvr_ly": round(cvr_ly, 2),
        "cvr_lw_pct": pct_change(cvr_t, cvr_l),
        "cvr_ly_pct": pct_change(cvr_t, cvr_ly),
        "aov_this": aov_t,
        "aov_last": aov_l,
        "aov_ly": aov_ly,
        "aov_lw_pct": pct_change(aov_t, aov_l),
        "aov_ly_pct": pct_change(aov_t, aov_ly),
        "new_this": new_t,
        "new_last": new_l,
        "new_ly": new_ly,
        "new_lw_pct": pct_change(new_t, new_l),
        "new_ly_pct": pct_change(new_t, new_ly),
        "organic_uv_this": ct["organic_uv"],
        "organic_uv_last": cl["organic_uv"],
        "organic_uv_ly": cly["organic_uv"],
        "organic_uv_lw_pct": pct_change(ct["organic_uv"], cl["organic_uv"]),
        "organic_uv_ly_pct": pct_change(ct["organic_uv"], cly["organic_uv"]),
        "nonorganic_uv_this": ct["nonorganic_uv"],
        "nonorganic_uv_last": cl["nonorganic_uv"],
        "nonorganic_uv_ly": cly["nonorganic_uv"],
        "nonorganic_uv_lw_pct": pct_change(ct["nonorganic_uv"], cl["nonorganic_uv"]),
        "nonorganic_uv_ly_pct": pct_change(ct["nonorganic_uv"], cly["nonorganic_uv"]),
        "organic_share_this": ct["organic_share"],
        "organic_share_last": cl["organic_share"],
        "organic_share_ly": cly["organic_share"],
        "organic_share_lw_pct": pct_change(ct["organic_share"], cl["organic_share"]),
        "organic_share_ly_pct": pct_change(ct["organic_share"], cly["organic_share"]),
    }


def src_weekly_funnel(start_date: str, end_date: str) -> pd.DataFrame:
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


def build_weekly_funnel_comparison():
    r = get_week_ranges()
    df_t = src_weekly_funnel(r["this"]["start"], r["this"]["end"])
    df_l = src_weekly_funnel(r["last"]["start"], r["last"]["end"])

    # -----------------------------
    # 1) 퍼널 전환율 비교 (지금 쓰고 있는 compare DF)
    # -----------------------------
    def _get(df):
        base = df.set_index("단계")["수"]
        return (
            base.get("view_item", 0),
            base.get("add_to_cart", 0),
            base.get("begin_checkout", 0),
            base.get("purchase", 0),
        )

    vt, ct, cht, bt = _get(df_t)
    vl, cl, chl, bl = _get(df_l)

    def _rate(a, b):
        try:
            return round(a / b * 100, 1) if b else 0.0
        except Exception:
            return 0.0

    rows = []
    for name, a_t, b_t, a_l, b_l in [
        ("상품 상세 → 장바구니", ct, vt, cl, vl),
        ("장바구니 → 체크아웃", cht, ct, chl, cl),
        ("체크아웃 → 결제완료", bt, cht, bl, chl),
    ]:
        t_rate = _rate(a_t, b_t)
        l_rate = _rate(a_l, b_l)
        rows.append(
            {
                "구간": name,
                "이번주 전환율(%)": t_rate,
                "전주 전환율(%)": l_rate,
                "변화(ppt)": round(t_rate - l_rate, 1),
            }
        )

    compare = pd.DataFrame(rows)

    # -----------------------------
    # 2) RAW 이벤트 카운트 + 전주 대비 증감률 + 순서 정렬
    # -----------------------------
    # 이번주 / 전주 카운트 머지
    raw = df_t.rename(columns={"수": "이번주 수"}).merge(
        df_l.rename(columns={"수": "전주 수"}), on="단계", how="outer"
    ).fillna(0)

    raw["이번주 수"] = raw["이번주 수"].astype(int)
    raw["전주 수"] = raw["전주 수"].astype(int)

    # 증감률(%) 컬럼 추가
    raw["증감률(%)"] = raw.apply(
        lambda r: pct_change(r["이번주 수"], r["전주 수"]),
        axis=1,
    )

    # 단계 순서: view_item → add_to_cart → begin_checkout → purchase
    order = ["view_item", "add_to_cart", "begin_checkout", "purchase"]
    raw["단계"] = pd.Categorical(raw["단계"], categories=order, ordered=True)
    raw = raw.sort_values("단계").reset_index(drop=True)

    return raw, compare

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


def src_weekly_products(start_date: str, end_date: str, limit: int = 100) -> pd.DataFrame:
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
        columns={"itemName": "상품명", "itemsPurchased": "구매수", "itemRevenue": "매출(원)"}
    )
    base["구매수"] = pd.to_numeric(base["구매수"], errors="coerce").fillna(0).astype(int)
    base["매출(원)"] = pd.to_numeric(base["매출(원)"], errors="coerce").fillna(0.0)
    base["매출(만원)"] = (base["매출(원)"] / 10_000).round(1)
    base = base.sort_values("매출(원)", ascending=False).head(limit)
    return base[["상품명", "구매수", "매출(만원)"]]


def src_weekly_search(start_date: str, end_date: str, limit: int = 80) -> pd.DataFrame:
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
        columns={"searchTerm": "키워드", "eventCount": "검색수", "transactions": "구매수"}
    )
    df["검색수"] = pd.to_numeric(df["검색수"], errors="coerce").fillna(0).astype(int)
    df["구매수"] = pd.to_numeric(df["구매수"], errors="coerce").fillna(0).astype(int)
    df["CVR(%)"] = (df["구매수"] / df["검색수"] * 100).replace([float("inf")], 0).round(2)
    df = df.sort_values("검색수", ascending=False)
    return df

def build_search_wow_table(search_this: pd.DataFrame,
                           search_last: pd.DataFrame) -> pd.DataFrame:
    """전주 대비 이번주 검색수가 증가한 키워드 정리."""

    if (search_this is None or search_this.empty or
        search_last is None or search_last.empty):
        return pd.DataFrame(columns=[
            "키워드", "검색수(THIS)", "검색수(LW)", "검색수 증감", "검색수 증감률(%)", "CVR(%)"
        ])

    t = search_this.copy()
    l = search_last.copy()

    t = t[["키워드", "검색수", "구매수", "CVR(%)"]]
    l = l[["키워드", "검색수"]].rename(columns={"검색수": "검색수_LW"})

    df = t.merge(l, on="키워드", how="left")
    df["검색수_LW"] = df["검색수_LW"].fillna(0).astype(int)

    df["검색수 증감"] = df["검색수"] - df["검색수_LW"]

    def _pct(row):
        base = row["검색수_LW"]
        if base == 0:
            return 0.0
        return round((row["검색수"] - base) / base * 100, 1)

    df["검색수 증감률(%)"] = df.apply(_pct, axis=1)

    # 최소 검색수·증감 필터(너무 잡음인 애들 제거)
    df = df[(df["검색수"] >= 10) & (df["검색수 증감"] > 0)]

    df = df.sort_values(["검색수 증감", "검색수 증감률(%)"], ascending=False).head(20)

    df = df.rename(columns={
        "검색수": "검색수(THIS)",
        "검색수_LW": "검색수(LW)"
    })

    return df[["키워드", "검색수(THIS)", "검색수(LW)", "검색수 증감", "검색수 증감률(%)", "CVR(%)"]]



def build_channel_mix(df_this: pd.DataFrame, df_last: pd.DataFrame) -> pd.DataFrame:
    if df_this is None or df_this.empty:
        return pd.DataFrame(columns=["채널", "이번주 비중(%)", "전주 비중(%)", "변화(ppt)"])
    this = df_this.copy()
    last = df_last.copy() if df_last is not None else pd.DataFrame(columns=this.columns)
    this["매출(원)"] = this["매출(만원)"] * 10_000
    if not last.empty:
        last["매출(원)"] = last["매출(만원)"] * 10_000
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
        rows.append(
            {
                "채널": ch,
                "이번주 비중(%)": this_share,
                "전주 비중(%)": round(last_share, 1),
                "변화(ppt)": round(this_share - last_share, 1),
            }
        )
    return pd.DataFrame(rows).sort_values("이번주 비중(%)", ascending=False)

def calc_wow_delta(this_df: pd.DataFrame,
                   last_df: pd.DataFrame,
                   key_col: str,
                   metric_cols: List[str]) -> pd.DataFrame:
    """이번주 vs 전주 % 증감 계산용 공통 함수."""
    if this_df is None or this_df.empty:
        return pd.DataFrame(columns=[key_col] + metric_cols)

    this = this_df.copy()
    last = last_df.copy() if last_df is not None else pd.DataFrame(columns=[key_col] + metric_cols)

    last = last[[key_col] + [c for c in metric_cols if c in last.columns]].copy()
    last = last.rename(columns={c: f"{c}_LW" for c in metric_cols if c in last.columns})

    merged = this.merge(last, on=key_col, how="left")

    for col in metric_cols:
        lw_col = f"{col}_LW"
        if lw_col not in merged.columns:
            merged[lw_col] = 0
        merged[lw_col] = pd.to_numeric(merged[lw_col], errors="coerce").fillna(0)
        merged[col] = pd.to_numeric(merged[col], errors="coerce").fillna(0)
        merged[f"{col}_chg_pct"] = merged.apply(
            lambda r: pct_change(r[col], r[lw_col]),
            axis=1,
        )

    return merged


def build_traffic_wow(traffic_this: pd.DataFrame,
                      traffic_last: pd.DataFrame) -> pd.DataFrame:
    """채널별 이번주 지표 + 전주 대비 % 증감."""
    metric_cols = ["UV", "구매수", "매출(만원)", "CVR(%)"]
    merged = calc_wow_delta(traffic_this, traffic_last, "채널", metric_cols)

    rename = {
        "UV_chg_pct": "UV Δ%(vs LW)",
        "구매수_chg_pct": "구매수 Δ%(vs LW)",
        "매출(만원)_chg_pct": "매출 Δ%(vs LW)",
        "CVR(%)_chg_pct": "CVR Δp(vs LW)",
    }
    merged = merged.rename(columns=rename)

    out_cols = [
        "채널",
        "UV", "UV Δ%(vs LW)",
        "구매수", "구매수 Δ%(vs LW)",
        "매출(만원)", "매출 Δ%(vs LW)",
        "CVR(%)", "CVR Δp(vs LW)",
        "신규",
    ]
    return merged[out_cols].sort_values("매출(만원)", ascending=False)


def build_products_wow(products_this: pd.DataFrame,
                       products_last: pd.DataFrame) -> pd.DataFrame:
    """상품별 이번주 지표 + 전주 대비 % 증감."""
    metric_cols = ["구매수", "매출(만원)"]
    merged = calc_wow_delta(products_this, products_last, "상품명", metric_cols)

    rename = {
        "구매수_chg_pct": "구매수 Δ%(vs LW)",
        "매출(만원)_chg_pct": "매출 Δ%(vs LW)",
    }
    merged = merged.rename(columns=rename)

    out_cols = [
        "상품명",
        "구매수", "구매수 Δ%(vs LW)",
        "매출(만원)", "매출 Δ%(vs LW)",
    ]
    return merged[out_cols].sort_values("매출(만원)", ascending=False)


# =====================================================================
# 5) 인사이트 / 액션
# =====================================================================

def build_weekly_insight_paragraph(
    kpi: Dict[str, float],
    funnel_compare_df: pd.DataFrame,
    traffic_this: pd.DataFrame,
    search_this: pd.DataFrame,
) -> str:
    rev_pct = kpi["revenue_lw_pct"]
    uv_pct = kpi["uv_lw_pct"]
    cvr_pct = kpi["cvr_lw_pct"]
    aov_pct = kpi["aov_lw_pct"]
    new_pct = kpi["new_lw_pct"]

    if rev_pct >= 0:
        p1 = (
            f"이번 주 매출은 전주 대비 {rev_pct:+.1f}% 증가했고, UV는 {uv_pct:+.1f}%,"
            f" CVR은 {cvr_pct:+.1f}p 수준의 변동을 보였습니다. "
            f"객단가(AOV)는 {aov_pct:+.1f}% 변동, 신규 유입은 {new_pct:+.1f}% 수준입니다."
        )
    else:
        p1 = (
            f"이번 주 매출은 전주 대비 {rev_pct:+.1f}% 감소했고, UV {uv_pct:+.1f}% ·"
            f" CVR {cvr_pct:+.1f}p 조정이 함께 나타났습니다. "
            f"객단가(AOV)는 {aov_pct:+.1f}% 변동, 신규 유입은 {new_pct:+.1f}% 수준입니다."
        )

    if funnel_compare_df is not None and not funnel_compare_df.empty:
        worst = funnel_compare_df.sort_values("변화(ppt)").iloc[0]
        if worst["변화(ppt)"] < 0:
            p2 = (
                f"퍼널에서는 '{worst['구간']}' 구간 전환율이 전주 대비 {worst['변화(ppt)']:+.1f}p 악화되어,"
                " 장바구니·체크아웃·결제 과정에서의 UX 또는 혜택 구조 점검이 필요합니다."
            )
        else:
            p2 = (
                f"퍼널에서는 '{worst['구간']}' 구간 전환율이 전주 대비 {worst['변화(ppt)']:+.1f}p 개선되며,"
                " 전반적으로 이탈이 완만한 한 주였습니다."
            )
    else:
        p2 = (
            "퍼널 데이터는 이번 주 기준으로 전주와의 직접 비교가 어려워,"
            " 상단 KPI 중심으로 우선 흐름을 모니터링해야 합니다."
        )

    if traffic_this is not None and not traffic_this.empty:
        top_ch = traffic_this.iloc[0]
        p3 = (
            f"채널 믹스 관점에서는 '{top_ch['채널']}' 채널이 매출 비중과 CVR 측면에서 가장 큰 영향력을 가지고 있으며,"
            " 오가닉/페이드 트래픽의 균형이 이번 주 성과에 직접적으로 연결되었습니다."
        )
    else:
        p3 = (
            "채널 데이터가 충분하지 않아, 이번 주에는 전체 UV·CVR 수준과 주요 캠페인 성과 위주로"
            " 단순 모니터링하는 것이 적절합니다."
        )

    if search_this is not None and not search_this.empty:
        low_cvr = search_this[search_this["CVR(%)"] < 1.0]
        if not low_cvr.empty:
            kw_list = ", ".join(low_cvr.head(3)["키워드"].tolist())
            p4 = (
                f"온사이트 검색에서는 검색량은 많지만 CVR이 낮은 키워드({kw_list} 등)가 확인되어,"
                " 검색 결과 페이지 구성·가격대·프로모션 연계 개선 여지가 있습니다."
            )
        else:
            p4 = (
                "온사이트 검색 상위 키워드들은 대체로 안정적인 CVR을 보이고 있어,"
                " 상위 검색어 기반 기획전 및 추천 영역 확장을 통해 볼륨을 키울 수 있는 상태입니다."
            )
    else:
        p4 = (
            "검색 데이터는 이번 주 기준으로 노이즈가 커, 상위 키워드 중심으로만 추세를 확인하는 수준으로"
            " 활용하는 것이 적절합니다."
        )

    if rev_pct < 0 and uv_pct < 0:
        p5 = (
            "종합하면 상단 유입과 매출이 함께 눌린 국면으로,"
            " 신규 유입 확대와 장바구니·체크아웃 구간 전환율 개선이 다음 주 최우선 과제입니다."
        )
    elif cvr_pct < 0 <= uv_pct:
        p5 = (
            "유입은 늘었지만 CVR이 떨어진 한 주였기 때문에,"
            " 유입 품질·랜딩 페이지·퍼널 UX에 대한 정교한 실험 설계가 필요합니다."
        )
    else:
        p5 = (
            "전반적으로 안정적인 성과를 유지한 한 주이며,"
            " 퍼포먼스가 좋은 채널·키워드·상품을 기준으로 규모를 소폭 확장하는 성장 실험이 가능한 상황입니다."
        )

    return f"""
<ul style="margin:4px 0 0 0; padding-left:16px; line-height:1.8;">
  <li><b>KPI 흐름:</b> {p1}</li>
  <li><b>퍼널 변화:</b> {p2}</li>
  <li><b>채널 믹스:</b> {p3}</li>
  <li><b>검색 행동:</b> {p4}</li>
  <li><b>종합 시사점:</b> {p5}</li>
</ul>
"""


def build_weekly_actions(
    kpi: Dict[str, float],
    funnel_compare_df: pd.DataFrame,
    traffic_this: pd.DataFrame,
    search_this: pd.DataFrame,
) -> List[str]:
    actions: List[str] = []

    if kpi["revenue_lw_pct"] < 0 and kpi["uv_lw_pct"] < 0:
        actions.append(
            "지난 4주 ROAS 상위 캠페인의 예산을 10~15% 상향하고, 성과 하위 캠페인은 입찰·타겟을 조정해 신규 유입을 회복합니다."
        )
    else:
        actions.append(
            "성과 상위 캠페인/소재를 기준으로 유사 타겟 확장(룩어라이크, 관심사 확장)을 적용해 획득 단가를 유지한 채 볼륨을 키웁니다."
        )

    actions.append(
        "자사몰/인스타그램/네이버포스트에서 주간 베스트 상품·검색 상위 키워드를 묶은 UGC 기반 스타일링 포스트 1~2편을 제작합니다."
    )

    if funnel_compare_df is not None and not funnel_compare_df.empty:
        worst = funnel_compare_df.sort_values("변화(ppt)").iloc[0]
        if worst["변화(ppt)"] < 0:
            actions.append(
                f"'{worst['구간']}' 구간 전환율 악화 원인을 파악하기 위해 디바이스·채널·상품군 기준 이탈 리포트를 분해하고,"
                " 최소 2개 이상의 UX/혜택 A/B 테스트를 설계합니다."
            )
        else:
            actions.append(
                f"'{worst['구간']}' 구간 전환율 개선 모멘텀을 다른 핵심 카테고리에 복제 적용해 확장 효과를 검증합니다."
            )
    else:
        actions.append(
            "장바구니·체크아웃 구간 이탈을 기기·결제수단 기준으로 분해해 특정 환경에서의 오류/로딩 이슈 여부를 우선 점검합니다."
        )

    actions.append(
        "매출 상위 SKU와 검색 상위 키워드 교집합을 추출해 기획전 상단에 배치하고, 재고 소진이 필요한 상품은 장바구니/체크아웃에"
        " 쿠폰/혜택 배너를 노출해 소진 속도를 높입니다."
    )

    if search_this is not None and not search_this.empty:
        low_cvr = search_this[search_this["CVR(%)"] < 1.0]
        if not low_cvr.empty:
            kw = ", ".join(low_cvr.head(3)["키워드"].tolist())
            actions.append(
                f"저CVR 검색어({kw})에 대해 결과 페이지 상단 상품·필터·가격대를 재구성하고, 관련 프로모션 배너를 추가해 CVR 개선 여부를 측정합니다."
            )

    actions.append(
        "최근 90일 내 2회 이상 구매한 고객과 최근 30일 유입·미구매 장바구니 이탈 고객을 분리해, 리워드 강화형/재방문 유도형 CRM 캠페인을 각각 실행합니다."
    )
    actions.append(
        "주요 카테고리별 리마케팅 캠페인을 분리하고 쿠폰/무이자/무료배송 등 서로 다른 혜택 메시지를 A/B 테스트해 세그먼트별 최적 인센티브를 찾습니다."
    )

    return actions


# =====================================================================
# 6) HTML 유틸
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
        "<th style=\"padding:3px 6px; border-bottom:1px solid #e1e4f0; text-align:left; font-weight:600; color:#555;\">",
    )
    html = html.replace(
        "<td>",
        "<td style=\"padding:3px 6px; border-bottom:1px solid #f1f3fa; text-align:left; color:#333;\">",
    )
    return html


def build_kpi_cards_html(kpi: Dict[str, float]) -> str:
    def card_block(title, main_html, this_txt, lw_txt, ly_txt, lw_pct, ly_pct, unit_is_ppt=False):
        lw_label = f"{lw_pct:+.1f}{'p' if unit_is_ppt else '%'}"
        ly_label = f"{ly_pct:+.1f}{'p' if unit_is_ppt else '%'}"
        return f"""
<div style="background:#ffffff; border-radius:16px; padding:14px 16px;
            border:1px solid #e1e7f5; height:100%;">
  <div style="font-size:11px; color:#777; margin-bottom:4px;">{title}</div>
  <div style="font-size:18px; font-weight:700; margin-bottom:4px;">{main_html}</div>
  <div style="font-size:10px; color:#999; margin-bottom:4px;">
    LW: {lw_txt} · LY: {ly_txt}
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
    rows = ""
    for _, row in mix_df.iterrows():
        width = max(5, int(row["이번주 비중(%)"] / max_share * 100))
        rows += f"""
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
  {rows}
</table>
"""


# =====================================================================
# 7) Matplotlib 그래프 → data URI
# =====================================================================

def _fig_to_data_uri(fig) -> str:
    import matplotlib.pyplot as plt  # noqa
    buf = io.BytesIO()
    fig.tight_layout()
    fig.savefig(buf, format="png", dpi=120)
    plt.close(fig)
    b64 = base64.b64encode(buf.getvalue()).decode("ascii")
    return f"data:image/png;base64,{b64}"


def make_kpi_change_chart(kpi: Dict[str, float]) -> str:
    import matplotlib.pyplot as plt
    metrics = ["Revenue", "UV", "CVR", "Orders", "AOV", "New"]
    vals = [
        kpi["revenue_lw_pct"],
        kpi["uv_lw_pct"],
        kpi["cvr_lw_pct"],
        kpi["orders_lw_pct"],
        kpi["aov_lw_pct"],
        kpi["new_lw_pct"],
    ]
    fig, ax = plt.subplots(figsize=(4.5, 3))
    ax.bar(metrics, vals)
    ax.axhline(0, linewidth=0.8)
    ax.set_ylabel("% vs LW")
    ax.set_title("KPI WoW Change")
    return _fig_to_data_uri(fig)


def make_funnel_chart(funnel_compare_df: pd.DataFrame) -> str:
    import matplotlib.pyplot as plt

    if funnel_compare_df is None or funnel_compare_df.empty:
        fig, ax = plt.subplots(figsize=(4.5, 3))
        ax.text(0.5, 0.5, "No data", ha="center", va="center")
        ax.axis("off")
        return _fig_to_data_uri(fig)

    # 원본 한글 구간명
    labels_kr = funnel_compare_df["구간"].tolist()

    # 그래프에서 쓸 영어 라벨 매핑
    label_map = {
        "상품 상세 → 장바구니": "Detail→Cart",
        "장바구니 → 체크아웃": "Cart→Checkout",
        "체크아웃 → 결제완료": "Checkout→Purchase",
    }
    labels = [label_map.get(x, f"Step{i+1}") for i, x in enumerate(labels_kr)]

    this_rates = funnel_compare_df["이번주 전환율(%)"].tolist()
    last_rates = funnel_compare_df["전주 전환율(%)"].tolist()
    x = range(len(labels))

    fig, ax = plt.subplots(figsize=(4.5, 3))
    ax.bar([i - 0.15 for i in x], last_rates, width=0.3, label="LW")
    ax.bar([i + 0.15 for i in x], this_rates, width=0.3, label="THIS")
    ax.set_xticks(list(x))
    ax.set_xticklabels(labels, rotation=15, ha="right")
    ax.set_ylabel("Conversion %")
    ax.set_title("Funnel Conversion WoW")
    ax.legend(fontsize=8)

    return _fig_to_data_uri(fig)


def make_channel_wow_chart(traffic_this: pd.DataFrame,
                           traffic_last: pd.DataFrame) -> str:
    """매출 증감(%)이 큰 채널 TOP6를 그리는 그래프."""
    import matplotlib.pyplot as plt

    if (traffic_this is None or traffic_this.empty or
        traffic_last is None or traffic_last.empty):
        fig, ax = plt.subplots(figsize=(4.5, 3))
        ax.text(0.5, 0.5, "No channel data", ha="center", va="center")
        ax.axis("off")
        return _fig_to_data_uri(fig)

    metric_cols = ["매출(만원)"]
    merged = calc_wow_delta(traffic_this, traffic_last, "채널", metric_cols)

    # 매출 증감 절대값 기준 상위 6개 채널
    merged["매출_chg_abs"] = merged["매출(만원)_chg_pct"].abs()
    top = merged.sort_values("매출_chg_abs", ascending=False).head(6)

    labels = top["채널"].tolist()
    vals = top["매출(만원)_chg_pct"].tolist()

    fig, ax = plt.subplots(figsize=(4.5, 3))
    ax.bar(labels, vals)
    ax.axhline(0, linewidth=0.8)
    ax.set_ylabel("Revenue % vs LW")
    ax.set_title("Top Channels by Revenue WoW Change")
    ax.set_xticklabels(labels, rotation=20, ha="right")

    return _fig_to_data_uri(fig)

# =====================================================================
# 8) GRAPH & ANALYSIS 요약 카드
# =====================================================================

def build_graph_summary_cards(
    kpi: Dict[str, float],
    funnel_compare_df: pd.DataFrame,
    mix_df: pd.DataFrame,
    search_df: pd.DataFrame,
) -> str:
    if funnel_compare_df is not None and not funnel_compare_df.empty:
        worst = funnel_compare_df.sort_values("변화(ppt)").iloc[0]
        funnel_line = f"'{worst['구간']}' 전환율 {worst['변화(ppt)']:+.1f}p 변화"
    else:
        funnel_line = "퍼널 비교 데이터 부족"

    if mix_df is not None and not mix_df.empty:
        top = mix_df.iloc[0]
        mix_line = f"'{top['채널']}' 비중 {top['이번주 비중(%)']:.1f}% · {top['변화(ppt)']:+.1f}p"
    else:
        mix_line = "채널 믹스 데이터 부족"

    if search_df is not None and not search_df.empty:
        low = search_df[search_df["CVR(%)"] < 1.0]
        if not low.empty:
            kw = ", ".join(low.head(3)["키워드"].tolist())
            search_line = f"저CVR 검색어: {kw}"
        else:
            search_line = "상위 검색어 CVR 안정 구간"
    else:
        search_line = "검색 데이터 부족"

    card_style = (
        "background:#ffffff; border-radius:12px; border:1px solid #e2e8f0;"
        " padding:8px 10px; font-size:10px; color:#111; height:100%;"
    )

    what = f"""
<div style="{card_style}">
  <div style="font-size:11px; font-weight:600; color:#0f172a; margin-bottom:4px;">1. What happened?</div>
  <p style="margin:0 0 4px 0; line-height:1.6;">
    Revenue {kpi['revenue_lw_pct']:+.1f}%, UV {kpi['uv_lw_pct']:+.1f}%, CVR {kpi['cvr_lw_pct']:+.1f}p 수준의 주간 변동이 있었습니다.
  </p>
  <p style="margin:0 0 4px 0; line-height:1.6;">
    Orders {kpi['orders_lw_pct']:+.1f}% · AOV {kpi['aov_lw_pct']:+.1f}% · 신규 {kpi['new_lw_pct']:+.1f}%로,
    매출 변화가 유입·전환·객단가 조합으로 설명됩니다.
  </p>
  <p style="margin:0; line-height:1.6;">
    퍼널/채널/검색을 함께 보면 단일 채널이 아닌 구조적 변화 여부를 확인할 수 있습니다.
  </p>
</div>
"""

    why = f"""
<div style="{card_style}">
  <div style="font-size:11px; font-weight:600; color:#0f172a; margin-bottom:4px;">2. Why?</div>
  <p style="margin:0 0 4px 0; line-height:1.6;">
    {funnel_line} 와 함께 채널 믹스 변화({mix_line})가 합쳐지며 KPI에 영향을 주었습니다.
  </p>
  <p style="margin:0 0 4px 0; line-height:1.6;">
    또한 {search_line} 등 검색 품질 편차가 특정 카테고리·상품의 전환 효율을 갈라놓았을 가능성이 있습니다.
  </p>
  <p style="margin:0; line-height:1.6;">
    유입 품질, 랜딩 페이지 일관성, 장바구니·체크아웃 UX가 복합적으로 작용한 결과로 해석할 수 있습니다.
  </p>
</div>
"""

    insight = """
<div style="background:#ffffff; border-radius:12px; border:1px solid #e2e8f0;
            padding:8px 10px; font-size:10px; color:#111; height:100%;">
  <div style="font-size:11px; font-weight:600; color:#0f172a; margin-bottom:4px;">3. Insight</div>
  <p style="margin:0 0 4px 0; line-height:1.6;">
    주간 단위 KPI만 보는 것보다 퍼널·채널·검색·상품을 동시에 보는 것이 구조적 이슈를 더 빨리 발견하게 해 줍니다.
  </p>
  <p style="margin:0 0 4px 0; line-height:1.6;">
    특히 유입은 늘었지만 CVR이 떨어지는 국면에서는 랜딩/검색/장바구니 UX가, 반대로 유입이 줄었지만 CVR이 유지될 경우
    미디어/브랜드 도달 측면이 핵심 과제가 됩니다.
  </p>
  <p style="margin:0; line-height:1.6;">
    이런 관점으로 보면 “어디를 더 써야 할지”보다 “어디를 먼저 막아야 할지”가 더 명확해집니다.
  </p>
</div>
"""

    action = """
<div style="background:#ffffff; border-radius:12px; border:1px solid #e2e8f0;
            padding:8px 10px; font-size:10px; color:#111; height:100%;">
  <div style="font-size:11px; font-weight:600; color:#0f172a; margin-bottom:4px;">4. Action</div>
  <p style="margin:0 0 4px 0; line-height:1.6;">
    단기적으로는 전환율이 많이 떨어진 퍼널 구간과 저CVR 검색어를 우선순위로 A/B 테스트를 설계합니다.
  </p>
  <p style="margin:0 0 4px 0; line-height:1.6;">
    중기적으로는 매출 비중이 높은 채널·상품 조합을 기준으로 예산 확대 및 전용 랜딩/기획전을 추가해 성장 구간을 키워야 합니다.
  </p>
  <p style="margin:0; line-height:1.6;">
    이 액션들을 주간 단위로 반복하면서 성과가 검증된 항목만 상시 구조로 편입하는 것이 효율적인 운영 방법입니다.
  </p>
</div>
"""

    return f"""
<table width="100%" cellpadding="0" cellspacing="0"
       style="border-collapse:separate; border-spacing:8px 10px; margin-top:8px;">
  <tr>
    <td width="50%" valign="top">{what}</td>
    <td width="50%" valign="top">{why}</td>
  </tr>
  <tr>
    <td width="50%" valign="top">{insight}</td>
    <td width="50%" valign="top">{action}</td>
  </tr>
</table>
"""


# =====================================================================
# 9) HTML 메인
# =====================================================================

def compose_html_weekly(
    kpi: Dict[str, float],
    funnel_raw: pd.DataFrame,
    funnel_compare_df: pd.DataFrame,
    traffic_this: pd.DataFrame,
    traffic_last: pd.DataFrame,
    products_this: pd.DataFrame,
    products_last: pd.DataFrame,   # ✅ 추가
    search_this: pd.DataFrame,
    search_last: pd.DataFrame,
) -> str:

    traffic_wow = build_traffic_wow(traffic_this, traffic_last)
    products_wow = build_products_wow(products_this, products_last)

    insight_paragraph = build_weekly_insight_paragraph(
        kpi, funnel_compare_df, traffic_this, search_this
    )
    weekly_actions = build_weekly_actions(
        kpi, funnel_compare_df, traffic_this, search_this
    )
    action_items_html = "".join(
        f"<li style='margin-bottom:4px;'>{a}</li>" for a in weekly_actions
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

    kpi_cards_html = build_kpi_cards_html(kpi)

    funnel_raw_box = f"""
<table width="100%" cellpadding="0" cellspacing="0"
       style="background:#ffffff; border-radius:12px;
              border:1px solid #e1e7f5; box-shadow:0 3px 10px rgba(0,0,0,0.03);
              padding:8px 10px; border-collapse:separate; min-height:220px;">
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
              padding:8px 10px; border-collapse:separate; min-height:220px;">
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
              padding:8px 10px; border-collapse:separate; min-height:220px;">
  <tr><td>
    <div style="font-size:11px; font-weight:600; color:#224; margin-bottom:2px;">
      Traffic by Channel (이번주 vs 전주)
    </div>
    <div style="font-size:10px; color:#888; margin-bottom:6px; line-height:1.4;">
      채널별 UV · 구매수 · 매출 · CVR과 전주 대비 증감률(%)을 함께 보여줍니다.
    </div>
    {df_to_html_table(traffic_wow)}
  </td></tr>
</table>
"""

    products_box = f"""
<table width="100%" cellpadding="0" cellspacing="0"
       style="background:#ffffff; border-radius:12px;
              border:1px solid #e1e7f5; box-shadow:0 3px 10px rgba(0,0,0,0.03);
              padding:8px 10px; border-collapse:separate; min-height:220px;">
  <tr><td>
    <div style="font-size:11px; font-weight:600; color:#224; margin-bottom:2px;">
      Top Selling Products (이번주 vs 전주)
    </div>
    <div style="font-size:10px; color:#888; margin-bottom:6px; line-height:1.4;">
      매출 기준 상위 상품과 전주 대비 구매수·매출 증감률입니다.
    </div>
    {df_to_html_table(products_wow.head(15))}
  </td></tr>
</table>
"""


    # 03번용: 전주 대비 검색수 증가 키워드
    search_wow_df = build_search_wow_table(search_this, search_last)
    search_wow_box = f"""
<table width="100%" cellpadding="0" cellspacing="0"
       style="background:#ffffff; border-radius:12px;
              border:1px solid #e1e7f5; box-shadow:0 3px 10px rgba(0,0,0,0.03);
              padding:8px 10px; border-collapse:separate; min-height:220px; margin-top:6px;">
  <tr><td>
    <div style="font-size:11px; font-weight:600; color:#224; margin-bottom:2px;">
      Search Keywords WoW (전주 대비 검색량 증가)
    </div>
    <div style="font-size:10px; color:#888; margin-bottom:6px; line-height:1.4;">
      이번 주 검색수가 전주 대비 유의미하게 증가한 키워드입니다. 신규 관심 카테고리 및 프로모션 기회 포인트로 활용할 수 있습니다.
    </div>
    {df_to_html_table(search_wow_df)}
  </td></tr>
</table>
"""

    mix_df = build_channel_mix(traffic_this, traffic_last)
    mix_bars_html = build_channel_mix_bars_html(mix_df)

    # Graph images
    kpi_img = make_kpi_change_chart(kpi)
    funnel_img = make_funnel_chart(funnel_compare_df)
    channel_wow_img = make_channel_wow_chart(traffic_this, traffic_last)

    if search_stats["has_data"]:
        search_insight_line = (
            f"검색 사용 세션 CVR은 {search_stats['search_cvr']*100:.2f}%, "
            f"비검색 세션 CVR은 {search_stats['non_cvr']*100:.2f}%로, "
            f"차이는 {search_stats['cvr_diff_ppt']:+.2f}p 수준입니다."
        )
    else:
        search_insight_line = (
            "검색 이벤트 데이터가 적어 검색 사용 세션과 비검색 세션의 CVR 비교는 "
            "참고용으로만 보는 것이 적절합니다."
        )


    kpi_graph_html = f"""
<table width="100%" cellpadding="0" cellspacing="0"
       style="background:#ffffff; border-radius:12px;
              border:1px solid #e1e7f5; box-shadow:0 3px 10px rgba(0,0,0,0.03);
              padding:8px 10px; border-collapse:separate; min-height:260px;">
  <tr><td>
    <div style="font-size:11px; font-weight:600; color:#1e293b; margin-bottom:4px;">
      KPI 변화 (이번주 vs 전주)
    </div>
    <img src="{kpi_img}" style="width:100%; max-width:100%; height:auto; border-radius:8px; margin-bottom:6px;" />
    <p style="margin:0 0 4px 0; font-size:10px; color:#111; line-height:1.6;">
      Revenue, UV, CVR, Orders, AOV, New Users의 전주 대비 증감률을 한 번에 보여주는 그래프입니다.
    </p>
    <p style="margin:0; font-size:10px; color:#111; line-height:1.6;">
      <b>Action:</b> 증감 폭이 큰 지표 순으로 캠페인·랜딩·프로모션 우선순위를 조정하고, 감소 지표는 A/B 테스트로 원인을 검증합니다.
    </p>
  </td></tr>
</table>
"""

    funnel_graph_html = f"""
<table width="100%" cellpadding="0" cellspacing="0"
       style="background:#ffffff; border-radius:12px;
              border:1px solid #e1e7f5; box-shadow:0 3px 10px rgba(0,0,0,0.03);
              padding:8px 10px; border-collapse:separate; min-height:260px;">
  <tr><td>
    <div style="font-size:11px; font-weight:600; color:#1e293b; margin-bottom:4px;">
      Funnel 비교 (이번주 vs 전주)
    </div>
    <img src="{funnel_img}" style="width:100%; max-width:100%; height:auto; border-radius:8px; margin-bottom:6px;" />
    <p style="margin:0 0 4px 0; font-size:10px; color:#111; line-height:1.6;">
      각 퍼널 단계별 이번주와 전주 전환율을 나란히 비교해, 이탈이 확대·완화된 구간을 확인할 수 있습니다.
    </p>
    <p style="margin:0; font-size:10px; color:#111; line-height:1.6;">
      <b>Action:</b> 전환율 하락 폭이 가장 큰 구간을 1순위로 선정하여 UX/혜택 구조 A/B 테스트를 설계합니다.
    </p>
  </td></tr>
</table>
"""

    mix_graph_html = f"""
<table width="100%" cellpadding="0" cellspacing="0"
       style="background:#ffffff; border-radius:12px;
              border:1px solid #e1e7f5; box-shadow:0 3px 10px rgba(0,0,0,0.03);
              padding:8px 10px; border-collapse:separate; min-height:260px;">
  <tr><td>
    <div style="font-size:11px; font-weight:600; color:#1e293b; margin-bottom:4px;">
      Channel Mix 변화
    </div>
    <div style="font-size:10px; color:#64748b; margin-bottom:6px;">
      채널별 매출 비중과 전주 대비 변화(ppt)를 막대 형태로 표현한 영역입니다.
    </div>
    {mix_bars_html}
    <p style="margin:6px 0 4px 0; font-size:10px; color:#111; line-height:1.6;">
      상위 채널의 비중 변화가 전체 매출에 미치는 영향이 크기 때문에, 상위 채널 위주로 증감 방향을 먼저 확인합니다.
    </p>
    <p style="margin:0; font-size:10px; color:#111; line-height:1.6;">
      <b>Action:</b> 비중이 늘어난 채널은 예산·소재 확장을, 줄어든 채널은 타겟·소재·랜딩 조정 실험을 우선 배치합니다.
    </p>
  </td></tr>
</table>
"""

    channel_wow_graph_html = f"""
<table width="100%" cellpadding="0" cellspacing="0"
       style="background:#ffffff; border-radius:12px;
              border:1px solid #e1e7f5; box-shadow:0 3px 10px rgba(0,0,0,0.03);
              padding:8px 10px; border-collapse:separate; min-height:260px;">
  <tr><td>
    <div style="font-size:11px; font-weight:600; color:#1e293b; margin-bottom:4px;">
      Top Channels by Revenue WoW Change
    </div>
    <img src="{channel_wow_img}" style="width:100%; max-width:100%; height:auto; border-radius:8px; margin-bottom:6px;" />
    <p style="margin:0 0 4px 0; font-size:10px; color:#111; line-height:1.6;">
      전주 대비 매출 증감률(%)이 가장 큰 채널을 상위 6개까지 보여줍니다. 증감 방향과 폭을 한 번에 확인할 수 있어
      어떤 채널이 이번 주 성과를 끌어올렸는지 / 끌어내렸는지 빠르게 파악할 수 있습니다.
    </p>
    <p style="margin:0; font-size:10px; color:#111; line-height:1.6;">
      <b>Action:</b> 매출 증감 폭이 큰 채널부터 예산·입찰·타겟·소재·랜딩 조정을 우선 적용하고, 특히 급감 채널은
      이상 트래픽·소재 피로도·가격/재고 이슈 여부를 먼저 점검합니다.
    </p>
  </td></tr>
</table>
"""



    graph_summary_cards = build_graph_summary_cards(
        kpi, funnel_compare_df, mix_df, search_this
    )

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
                          GRAPH &amp; ANALYSIS
                        </span>
                      </td>
                    </tr>
                  </table>
                </td>
              </tr>
            </table>

{insight_action_block}

<div style="font-size:11px; letter-spacing:0.12em; color:#6d7a99;
            margin-top:18px; margin-bottom:10px;">
  02 · WEEKLY KPI SNAPSHOT
</div>
{kpi_cards_html}

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
    <td colspan="2" valign="top" style="padding:4px 0 0 0%;">{search_wow_box}</td>
  </tr>
</table>

<div style="font-size:11px; letter-spacing:0.12em; color:#6d7a99;
            margin-top:20px; margin-bottom:8px;">
  04 · GRAPH &amp; ANALYSIS
</div>
<table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:4px;">
  <tr>
    <td width="50%" valign="top" style="padding:4px 6px 8px 0%;">{kpi_graph_html}</td>
    <td width="50%" valign="top" style="padding:4px 0 8px 6px;">{funnel_graph_html}</td>
  </tr>
  <tr>
    <td width="50%" valign="top" style="padding:4px 6px 8px 0%;">{mix_graph_html}</td>
    <td width="50%" valign="top" style="padding:4px 0 8px 6px;">{channel_wow_graph_html}</td>
  </tr>
</table>

<div style="font-size:11px; letter-spacing:0.12em; color:#6d7a99;
            margin-top:20px; margin-bottom:8px;">
  05 · INSIGHT
</div>

{graph_summary_cards}

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
# 10) 메인 실행
# =====================================================================

def send_weekly_digest():
    try:
        ranges = get_week_ranges()
        kpi = build_weekly_kpi()
        funnel_raw, funnel_compare_df = build_weekly_funnel_comparison()
        traffic_this = src_weekly_traffic(ranges["this"]["start"], ranges["this"]["end"])
        traffic_last = src_weekly_traffic(ranges["last"]["start"], ranges["last"]["end"])
        products_this = src_weekly_products(ranges["this"]["start"], ranges["this"]["end"])
        products_last = src_weekly_products(ranges["last"]["start"], ranges["last"]["end"])
        search_this = src_weekly_search(ranges["this"]["start"], ranges["this"]["end"])
        search_last = src_weekly_search(ranges["last"]["start"], ranges["last"]["end"])

        html_body = compose_html_weekly(
            kpi=kpi,
            funnel_raw=funnel_raw,
            funnel_compare_df=funnel_compare_df,
            traffic_this=traffic_this,
            traffic_last=traffic_last,
            products_this=products_this,
            products_last=products_last,   # ✅ 추가
            search_this=search_this,
            search_last=search_last,
        )


        subject = f"[Columbia] Weekly eCommerce Digest – {kpi['week_label']}"
        send_email_html(subject, html_body, WEEKLY_RECIPIENTS)
        print("[INFO] Weekly digest sent.")
    except Exception as e:
        msg = f"[ERROR] Weekly digest 생성/발송 중 오류: {e}"
        print(msg)
        send_critical_alert("[Columbia] Weekly Digest Error", msg)



if __name__ == "__main__":
    send_weekly_digest()

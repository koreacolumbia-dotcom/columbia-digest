#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Columbia KR - MD Digest (DAILY)
- 어제(KST) 기준 Daily 모니터링(운영/즉시 액션용)
- Gmail / Outlook SMTP
- MD 가독성(한글 컬럼명/정렬/소수점) 최적화

DATA (mart):
- alerts_daily
- segment_by_channel_daily
- abandon_recovery_summary_daily
- md_high_intent_items_daily
- md_low_cvr_high_view_items_weekly

ENV:
- BQ_PROJECT, BQ_DATASET, (권장) GCP_SA_JSON
- SMTP_PROVIDER=gmail|outlook, SMTP_USER, SMTP_PASS
- MD_DAILY_RECIPIENTS="a@x.com,b@x.com"
- INCLUDE_ATTACHMENTS=0|1  (default 0)
"""

import os
import json
import smtplib
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import List, Tuple, Optional

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication


# -----------------------
# Config
# -----------------------
BQ_PROJECT = os.getenv("BQ_PROJECT", "columbia-ga4").strip()
BQ_DATASET = os.getenv("BQ_DATASET", "mart").strip()

SMTP_PROVIDER = os.getenv("SMTP_PROVIDER", "gmail").lower().strip()  # gmail/outlook
SMTP_USER = os.getenv("SMTP_USER", "").strip()
SMTP_PASS = os.getenv("SMTP_PASS", "").strip()

MD_DAILY_RECIPIENTS = [
    e.strip() for e in os.getenv("MD_DAILY_RECIPIENTS", "").split(",") if e.strip()
]

INCLUDE_ATTACHMENTS = os.getenv("INCLUDE_ATTACHMENTS", "0").strip() == "1"


# -----------------------
# BigQuery
# -----------------------
def _build_bq_client():
    from google.cloud import bigquery
    sa_json = os.getenv("GCP_SA_JSON", "").strip()

    if sa_json:
        from google.oauth2 import service_account
        info = json.loads(sa_json)
        creds = service_account.Credentials.from_service_account_info(info)
        return bigquery.Client(project=BQ_PROJECT, credentials=creds)

    return bigquery.Client(project=BQ_PROJECT)


def bq_query_df(sql: str) -> pd.DataFrame:
    return _build_bq_client().query(sql).result().to_dataframe()


def _get_table_columns(table_fqn: str) -> List[str]:
    """Return column list using INFORMATION_SCHEMA."""
    project, dataset, table = table_fqn.split(".")
    df = bq_query_df(f"""
      SELECT column_name
      FROM `{project}.{dataset}.INFORMATION_SCHEMA.COLUMNS`
      WHERE table_name = '{table}'
      ORDER BY ordinal_position
    """)
    if df is None or df.empty:
        return []
    return df["column_name"].tolist()


def _pick_date_column(cols: List[str]) -> Optional[str]:
    """Pick a likely date/snapshot column name."""
    candidates = [
        "date", "dt", "event_date", "snapshot_dt", "date_kst",
        "partition_date", "run_date", "report_date"
    ]
    colset = set(cols)
    for c in candidates:
        if c in colset:
            return c
    return None


def read_daily_table(table_fqn: str, target_date: str, limit_when_no_datecol: int = 5000) -> pd.DataFrame:
    """
    Read rows for target_date if date-like column exists.
    If empty, fallback to latest date.
    If no date column, LIMIT fallback.
    """
    cols = _get_table_columns(table_fqn)
    date_col = _pick_date_column(cols)

    if date_col:
        df = bq_query_df(f"""
          SELECT *
          FROM `{table_fqn}`
          WHERE {date_col} = '{target_date}'
        """)
        if df is None or df.empty:
            df = bq_query_df(f"""
              SELECT *
              FROM `{table_fqn}`
              WHERE {date_col} = (SELECT MAX({date_col}) FROM `{table_fqn}`)
            """)
        return df

    return bq_query_df(f"SELECT * FROM `{table_fqn}` LIMIT {int(limit_when_no_datecol)}")


# -----------------------
# SMTP
# -----------------------
def _smtp_host_port():
    if SMTP_PROVIDER == "gmail":
        return ("smtp.gmail.com", 587)
    if SMTP_PROVIDER == "outlook":
        return ("smtp.office365.com", 587)

    host = os.getenv("SMTP_HOST", "").strip()
    port = int(os.getenv("SMTP_PORT", "587"))
    if not host:
        raise RuntimeError("SMTP_PROVIDER must be gmail/outlook or set SMTP_HOST/SMTP_PORT")
    return (host, port)


def send_mail(subject: str, html: str, recipients: List[str], attachments: List[Tuple[str, bytes]]):
    if not recipients:
        print("[WARN] MD_DAILY_RECIPIENTS empty - skip send")
        return
    if not (SMTP_USER and SMTP_PASS):
        raise RuntimeError("SMTP_USER/SMTP_PASS missing (check GitHub Secrets)")

    host, port = _smtp_host_port()

    msg = MIMEMultipart("mixed")
    msg["From"] = SMTP_USER
    msg["To"] = ", ".join(recipients)
    msg["Subject"] = subject

    alt = MIMEMultipart("alternative")
    msg.attach(alt)
    alt.attach(MIMEText("MD Daily Digest", "plain", "utf-8"))
    alt.attach(MIMEText(html, "html", "utf-8"))

    for name, data in attachments:
        part = MIMEApplication(data, Name=name)
        part["Content-Disposition"] = f'attachment; filename="{name}"'
        msg.attach(part)

    with smtplib.SMTP(host, port) as server:
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(SMTP_USER, SMTP_PASS)
        server.sendmail(SMTP_USER, recipients, msg.as_string())

    print(f"[OK] Daily mail sent via {SMTP_PROVIDER}: from={SMTP_USER}, to={len(recipients)}, attachments={len(attachments)}")


# -----------------------
# Formatting helpers (MD UX)
# -----------------------
def _coerce_numeric(df: pd.DataFrame, cols: List[str]) -> pd.DataFrame:
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_numeric(df[c], errors="coerce")
    return df


def _rename_columns(df: pd.DataFrame, mapping: dict) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    return df.rename(columns={k: v for k, v in mapping.items() if k in df.columns})


def _round_cols(df: pd.DataFrame, round0: List[str] = None, round1: List[str] = None) -> pd.DataFrame:
    if df is None or df.empty:
        return df
    round0 = round0 or []
    round1 = round1 or []

    df = _coerce_numeric(df, round0 + round1)

    for c in round0:
        if c in df.columns:
            df[c] = df[c].round(0).astype("Int64")  # 정수처럼 표시
    for c in round1:
        if c in df.columns:
            df[c] = df[c].round(1)

    return df


def _format_pct_if_needed(df: pd.DataFrame, col: str) -> pd.DataFrame:
    """
    pdp_to_atc_user_cvr 값이 0.0136 같은 비율(0~1)로 들어오면 100 곱해서 %로 보이게.
    이미 1.3 / 13.6 처럼 들어오면 그대로.
    """
    if df is None or df.empty or col not in df.columns:
        return df
    s = pd.to_numeric(df[col], errors="coerce")
    if s.dropna().empty:
        return df

    # 휴리스틱: 대부분이 0~1 사이면 비율로 판단
    ratio_share = ((s >= 0) & (s <= 1)).mean()
    if ratio_share >= 0.8:
        df[col] = (s * 100.0)
    else:
        df[col] = s
    return df


def _make_table_html(df: pd.DataFrame, center_cols: List[str] = None, max_rows: int = 20) -> str:
    """
    Outlook-friendly: inline style + fixed layout
    center_cols: column names to center-align
    """
    if df is None or df.empty:
        return "<div style='color:#999;font-size:12px;'>데이터 없음</div>"

    center_cols = center_cols or []
    d = df.head(max_rows).copy()

    # Build HTML manually for safer alignment
    cols = list(d.columns)

    table_style = (
        "width:100%; border-collapse:collapse; font-size:12px; table-layout:fixed;"
    )
    th_base = (
        "text-align:left; padding:6px 8px; background:#f3f6fb; border-bottom:1px solid #e6eaf2;"
        "white-space:normal; overflow-wrap:anywhere;"
    )
    td_base = (
        "text-align:left; padding:6px 8px; border-bottom:1px solid #f0f2f7;"
        "white-space:normal; overflow-wrap:anywhere;"
    )

    # alignment map by column
    align_map = {c: ("center" if c in center_cols else "left") for c in cols}

    html = [f"<table style='{table_style}'>"]
    # header
    html.append("<thead><tr>")
    for c in cols:
        html.append(
            f"<th style='{th_base} text-align:{align_map[c]};'>{str(c)}</th>"
        )
    html.append("</tr></thead>")

    # body
    html.append("<tbody>")
    for _, row in d.iterrows():
        html.append("<tr>")
        for c in cols:
            v = row[c]
            if pd.isna(v):
                v = ""
            html.append(
                f"<td style='{td_base} text-align:{align_map[c]};'>{v}</td>"
            )
        html.append("</tr>")
    html.append("</tbody></table>")

    return "".join(html)


def card(title: str, desc: str, body_html: str) -> str:
    return f"""
    <div style="background:#fff;border:1px solid #e6eaf2;border-radius:12px;padding:14px;margin-bottom:12px;">
      <div style="font-weight:900;font-size:13px;">{title}</div>
      <div style="font-size:12px;color:#667085;margin-top:4px;line-height:1.4;">{desc}</div>
      <div style="margin-top:10px;">{body_html}</div>
    </div>
    """


def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    if df is None:
        df = pd.DataFrame()
    return df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")


# -----------------------
# MAIN
# -----------------------
def run_md_daily():
    kst = ZoneInfo("Asia/Seoul")
    today = datetime.now(kst).date()
    yesterday = (today - timedelta(days=1)).isoformat()

    # FQNs
    alerts_fqn  = f"{BQ_PROJECT}.{BQ_DATASET}.alerts_daily"
    channel_fqn = f"{BQ_PROJECT}.{BQ_DATASET}.segment_by_channel_daily"
    abandon_fqn = f"{BQ_PROJECT}.{BQ_DATASET}.abandon_recovery_summary_daily"

    hot_items_fqn = f"{BQ_PROJECT}.{BQ_DATASET}.md_high_intent_items_daily"
    fix_items_fqn = f"{BQ_PROJECT}.{BQ_DATASET}.md_low_cvr_high_view_items_weekly"

    # --- base monitoring ---
    df_alerts  = read_daily_table(alerts_fqn, yesterday)
    df_channel = read_daily_table(channel_fqn, yesterday)
    df_abandon = read_daily_table(abandon_fqn, yesterday)

    # --- A: 구매 직전 유저가 멈춘 상품 TOP ---
    # (테이블이 이미 한글 컬럼이면 그대로 쓰고, 아니면 python에서 rename)
    df_hot = read_daily_table(hot_items_fqn, yesterday)

    # rename/format for df_hot (둘 다 대응)
    hot_rename = {
        "snapshot_dt": "기준일",
        "last_item_category": "카테고리",
        "last_item_name": "상품명",
        "users": "유저수",
        "atc_cnt_3d_sum": "최근3일_ATC합",
        "avg_view_7d": "최근7일_평균조회",
        "view_item_cnt_7d": "최근7일_평균조회",  # 혹시 다른 이름일 때
    }
    df_hot = _rename_columns(df_hot, hot_rename)
    df_hot = _round_cols(df_hot, round0=["최근7일_평균조회"])

    # --- B: 전환 개선 후보 TOP (weekly source) ---
    # 최신주차 기준 데이터는 유지하되, 표에서 “주차시작일/종료일”은 숨기고 “기준일(어제)”만 보여줌
    df_fix = bq_query_df(f"""
      WITH latest AS (
        SELECT MAX(week_start_dt) AS wk
        FROM `{fix_items_fqn}`
      )
      SELECT *
      FROM `{fix_items_fqn}`
      WHERE week_start_dt = (SELECT wk FROM latest)
      ORDER BY pdp_view_users DESC, pdp_to_atc_user_cvr ASC
      LIMIT 50
    """)

    fix_rename = {
        "week_start_dt": "주차시작일",
        "week_end_dt": "주차종료일",
        "item_id": "상품ID",
        "item_name": "상품명",
        "item_category": "카테고리",
        "pdp_views": "PDP조회수",
        "atc_events": "ATC이벤트수",
        "pdp_view_users": "PDP유저수",
        "atc_users": "ATC유저수",
        "pdp_to_atc_user_cvr": "PDP→ATC_유저CVR(%)",
        "pdp_to_atc_event_rate": "PDP→ATC_이벤트전환율(%)",
    }
    df_fix = _rename_columns(df_fix, fix_rename)

    # CVR 소수점 1자리 + (0~1 비율이면 100곱해서 %로)
    df_fix = _format_pct_if_needed(df_fix, "PDP→ATC_유저CVR(%)")
    df_fix = _format_pct_if_needed(df_fix, "PDP→ATC_이벤트전환율(%)")
    df_fix = _round_cols(df_fix, round1=["PDP→ATC_유저CVR(%)", "PDP→ATC_이벤트전환율(%)"])

    # 표에서 주차 컬럼이 이상해 보이는 문제 대응: 표시용으로 "기준일" 추가 + 주차컬럼 드랍
    if not df_fix.empty:
        df_fix.insert(0, "기준일", yesterday)
        for drop_c in ["주차시작일", "주차종료일"]:
            if drop_c in df_fix.columns:
                df_fix.drop(columns=[drop_c], inplace=True)

    # --- abandon: 어제 기준 + 소수점 1자리(매출 등) ---
    abandon_rename = {
        "snapshot_dt": "기준일",
        "segment": "세그먼트",
        "device_category": "디바이스",
        "abandon_users": "이탈유저수",
        "recovered_users": "복구유저수",
        "recovered_revenue": "복구매출",
    }
    df_abandon = _rename_columns(df_abandon, abandon_rename)
    df_abandon = _round_cols(df_abandon, round1=["복구매출"])

    # --- channel: 가능한 부분만 한글/정렬 ---
    # (스키마를 정확히 모르니 흔한 컬럼만 매핑)
    channel_rename = {
        "snapshot_dt": "기준일",
        "date": "기준일",
        "channel_group": "채널",
        "channel": "채널",
        "source_medium": "소스/매체",
        "sessions": "세션",
        "users": "유저수",
        "transactions": "구매수",
        "revenue": "매출",
        "cvr": "CVR(%)",
    }
    df_channel = _rename_columns(df_channel, channel_rename)
    df_channel = _round_cols(df_channel, round1=["CVR(%)", "매출"])

    # --- alerts: 가능한 부분만 정리 ---
    alerts_rename = {"snapshot_dt": "기준일", "date": "기준일"}
    df_alerts = _rename_columns(df_alerts, alerts_rename)

    # Center align columns (MD readability)
    center_cols_hot = [c for c in ["기준일", "카테고리"] if c in df_hot.columns]
    center_cols_fix = [c for c in ["기준일", "카테고리"] if c in df_fix.columns]
    center_cols_channel = [c for c in ["기준일", "채널", "소스/매체"] if c in df_channel.columns]
    center_cols_abandon = [c for c in ["기준일", "세그먼트", "디바이스"] if c in df_abandon.columns]

    # Build blocks (MD 읽는 순서)
    blocks = [
        card(
            "🧲 구매 직전 유저가 멈춘 상품 TOP",
            "최근 3일 장바구니 담음 + 최근 7일 구매 없음(‘아까운 유저’). 오늘 상단/기획전/혜택/정렬로 회수 타겟.",
            _make_table_html(df_hot, center_cols=center_cols_hot, max_rows=20),
        ),
        card(
            "🔧 전환 개선 후보 TOP",
            "노출(유저)은 많은데 PDP→ATC 전환이 낮은 상품. PDP/옵션/혜택/리뷰/배송 문구/재고표시 점검 우선순위. (최신 주차 기준)",
            _make_table_html(df_fix, center_cols=center_cols_fix, max_rows=20),
        ),
        card(
            "🚨 이상 징후 (Alerts)",
            "어제 기준 급변 지표(없으면 정상).",
            _make_table_html(df_alerts, center_cols=["기준일"] if "기준일" in df_alerts.columns else [], max_rows=12),
        ),
        card(
            "📊 채널별 Daily 성과",
            "어제 기준 유입/성과 흐름(유입 감소 vs 상품/전환 문제 분리).",
            _make_table_html(df_channel, center_cols=center_cols_channel, max_rows=15),
        ),
        card(
            "🛒 Abandon Recovery 요약",
            "어제 기준 이탈/복구 요약(결제/혜택/배송/재고/옵션/UX 이슈 신호).",
            _make_table_html(df_abandon, center_cols=center_cols_abandon, max_rows=15),
        ),
    ]

    html = f"""<!doctype html>
<html lang="ko">
<head><meta charset="utf-8"></head>
<body style="margin:0;background:#f5f7fb;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI','Noto Sans KR',Arial,sans-serif;">
  <div style="max-width:980px;margin:0 auto;padding:18px 12px;">
    <div style="background:#ffffff;border:1px solid #e6eaf2;border-radius:14px;padding:16px 16px;">
      <div style="font-size:18px;font-weight:900;color:#0055a5;">MD Daily Digest</div>
      <div style="font-size:13px;color:#475467;margin-top:4px;">기준일: {yesterday} (KST) · 즉시 액션용 모니터링</div>
      <div style="font-size:12px;color:#667085;margin-top:10px;line-height:1.6;">
        - 상단 2개 블록이 MD 액션 핵심(회수/개선 상품)<br/>
        - Alerts/채널/Abandon은 “원인 분리(유입 vs 전환 vs 이탈)”용
      </div>
    </div>

    <div style="margin-top:14px;">
      {''.join(blocks)}
    </div>

    <div style="font-size:11px;color:#98a2b3;text-align:right;margin-top:10px;">
      Generated by BigQuery (mart) · mailed via Python SMTP
    </div>
  </div>
</body>
</html>"""

    attachments: List[Tuple[str, bytes]] = []
    if INCLUDE_ATTACHMENTS:
        attachments = [
            (f"md_high_intent_items_daily_{yesterday}.csv", df_to_csv_bytes(df_hot)),
            (f"md_low_cvr_high_view_items_weekly_latest_asof_{yesterday}.csv", df_to_csv_bytes(df_fix)),
            (f"alerts_daily_{yesterday}.csv", df_to_csv_bytes(df_alerts)),
            (f"segment_by_channel_daily_{yesterday}.csv", df_to_csv_bytes(df_channel)),
            (f"abandon_recovery_summary_daily_{yesterday}.csv", df_to_csv_bytes(df_abandon)),
        ]

    send_mail(
        subject=f"[MD Daily] 핵심상품/Alerts/채널/Abandon 요약 ({yesterday})",
        html=html,
        recipients=MD_DAILY_RECIPIENTS,
        attachments=attachments,
    )


if __name__ == "__main__":
    run_md_daily()

#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Columbia KR - MD Digest (DAILY)
- 어제(KST) 기준 Daily 모니터링(운영/즉시 액션용)
- Gmail / Outlook SMTP

DATA (mart):
[모니터링]
- alerts_daily
- segment_by_channel_daily
- abandon_recovery_summary_daily
- daily_behavior_segments  (원본, 필요 시 참고)

[MD 핵심 상품 요약 - Scheduled Query로 미리 생성 권장]
- md_high_intent_items_daily              (A) 구매 직전 유저가 멈춘 상품 TOP (어제 스냅샷)
- md_low_cvr_high_view_items_weekly       (B) 노출 많고 전환 약한 상품 TOP (최신 주차 스냅샷)

필수 ENV:
- BQ_PROJECT, BQ_DATASET, (권장) GCP_SA_JSON
- SMTP_PROVIDER=gmail|outlook, SMTP_USER, SMTP_PASS
- MD_DAILY_RECIPIENTS="a@x.com,b@x.com"
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
# HTML helpers
# -----------------------
def df_html(df: pd.DataFrame, n: int = 12) -> str:
    if df is None or df.empty:
        return "<div style='color:#999;font-size:12px;'>데이터 없음</div>"
    return df.head(n).to_html(index=False, border=0)


def card(title: str, desc: str, body: str) -> str:
    return f"""
    <div style="background:#fff;border:1px solid #e6eaf2;border-radius:12px;padding:14px;margin-bottom:12px;">
      <div style="font-weight:900;">{title}</div>
      <div style="font-size:12px;color:#667085;margin-top:4px;line-height:1.4;">{desc}</div>
      <div style="margin-top:10px;">{body}</div>
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

    # Base monitoring tables
    alerts_fqn  = f"{BQ_PROJECT}.{BQ_DATASET}.alerts_daily"
    channel_fqn = f"{BQ_PROJECT}.{BQ_DATASET}.segment_by_channel_daily"
    abandon_fqn = f"{BQ_PROJECT}.{BQ_DATASET}.abandon_recovery_summary_daily"

    # MD 핵심 상품 요약 테이블(예약쿼리로 생성 권장)
    # - 없으면(테이블 미생성) 아래 fallback 쿼리로 계산해서 사용
    hot_items_fqn = f"{BQ_PROJECT}.{BQ_DATASET}.md_high_intent_items_daily"
    fix_items_fqn = f"{BQ_PROJECT}.{BQ_DATASET}.md_low_cvr_high_view_items_weekly"

    df_alerts  = read_daily_table(alerts_fqn, yesterday)
    df_channel = read_daily_table(channel_fqn, yesterday)
    df_abandon = read_daily_table(abandon_fqn, yesterday)

    # ---- A) 구매 직전 유저가 멈춘 상품 TOP ----
    try:
        df_hot = read_daily_table(hot_items_fqn, yesterday)
    except Exception as e:
        print(f"[WARN] fallback hot_items (table missing or query fail): {e}")
        df_hot = bq_query_df(f"""
          SELECT
            snapshot_dt,
            last_item_category,
            last_item_name,
            COUNT(DISTINCT user_pseudo_id) AS users,
            SUM(atc_cnt_3d) AS atc_cnt_3d_sum,
            AVG(view_item_cnt_7d) AS avg_view_7d
          FROM `{BQ_PROJECT}.{BQ_DATASET}.daily_behavior_segments`
          WHERE snapshot_dt = DATE_SUB(CURRENT_DATE('Asia/Seoul'), INTERVAL 1 DAY)
            AND atc_cnt_3d >= 1
            AND purchase_cnt_7d = 0
            AND last_item_name IS NOT NULL
          GROUP BY 1,2,3
          ORDER BY users DESC
          LIMIT 50
        """)

    # ---- B) 노출 많고 전환 약한 상품 TOP (최신 주차) ----
    try:
        df_fix = bq_query_df(f"""
          SELECT *
          FROM `{fix_items_fqn}`
          ORDER BY pdp_view_users DESC, pdp_to_atc_user_cvr ASC
          LIMIT 50
        """)
    except Exception as e:
        print(f"[WARN] fallback fix_items (table missing or query fail): {e}")
        df_fix = bq_query_df(f"""
          WITH latest AS (
            SELECT MAX(week_start_dt) AS wk
            FROM `{BQ_PROJECT}.{BQ_DATASET}.pdp_to_atc_item_weekly`
          )
          SELECT
            week_start_dt,
            item_category,
            item_name,
            pdp_view_users,
            atc_users,
            pdp_to_atc_user_cvr
          FROM `{BQ_PROJECT}.{BQ_DATASET}.pdp_to_atc_item_weekly`
          WHERE week_start_dt = (SELECT wk FROM latest)
            AND pdp_view_users >= 200
          ORDER BY pdp_view_users DESC, pdp_to_atc_user_cvr ASC
          LIMIT 50
        """)

    # Build email blocks (MD 읽는 순서 기준)
    blocks = [
        card(
            "🧲 구매 직전 유저가 멈춘 상품 TOP",
            "최근 3일 장바구니 담음 + 최근 7일 구매 없음(‘아까운 유저’). 오늘 상단/기획전/혜택/정렬로 회수 타겟.",
            df_html(df_hot, 20)
        ),
        card(
            "🔧 전환 개선 후보 TOP (최신 주차)",
            "노출(유저)은 많은데 PDP→ATC 전환이 낮은 상품. PDP/옵션/혜택/리뷰/배송 문구/재고표시 점검 우선순위.",
            df_html(df_fix, 20)
        ),
        card(
            "🚨 이상 징후 (Alerts)",
            "어제 기준 급변 지표(없으면 정상).",
            df_html(df_alerts, 10)
        ),
        card(
            "📊 채널별 Daily 성과",
            "어제 기준 유입/성과 흐름(유입 감소 vs 상품/전환 문제 분리).",
            df_html(df_channel, 15)
        ),
        card(
            "🛒 Abandon Recovery 요약",
            "어제 기준 이탈/복구 요약(결제/혜택/배송/재고/옵션/UX 이슈 신호).",
            df_html(df_abandon, 15)
        ),
    ]

    html = f"""
    <h2 style="margin:0;">MD Daily Digest</h2>
    <p style="margin:6px 0 14px;color:#475467;">기준일: {yesterday} (KST)</p>
    {''.join(blocks)}
    """

    attachments = [
        (f"md_high_intent_items_daily_{yesterday}.csv", df_to_csv_bytes(df_hot)),
        (f"md_low_cvr_high_view_items_weekly_latest_{yesterday}.csv", df_to_csv_bytes(df_fix)),
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

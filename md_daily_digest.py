#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Columbia KR - MD Digest (DAILY)
- 어제(KST) 기준 Daily 모니터링
- 주간 의사결정 ❌ / 즉시 액션용
- Gmail / Outlook SMTP

DATA (mart):
- alerts_daily
- segment_by_channel_daily
- daily_behavior_segments
- abandon_recovery_summary_daily

주의:
- 테이블마다 날짜 컬럼명이 다를 수 있음(date vs snapshot_dt 등)
  → 이 코드는 INFORMATION_SCHEMA로 날짜 컬럼을 자동 감지해서 필터링함.
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
    If not, return latest snapshot (MAX of date-like col) or LIMIT fallback.
    """
    cols = _get_table_columns(table_fqn)
    date_col = _pick_date_column(cols)

    if date_col:
        # 1) try exact date filter
        sql = f"""
          SELECT *
          FROM `{table_fqn}`
          WHERE {date_col} = '{target_date}'
        """
        df = bq_query_df(sql)

        # 2) if empty, fallback to latest date_col
        if df is None or df.empty:
            sql2 = f"""
              SELECT *
              FROM `{table_fqn}`
              WHERE {date_col} = (SELECT MAX({date_col}) FROM `{table_fqn}`)
            """
            df = bq_query_df(sql2)
        return df

    # No obvious date col: fallback (avoid breaking daily pipeline)
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
      <div style="font-weight:800;">{title}</div>
      <div style="font-size:12px;color:#667085;margin-top:4px;">{desc}</div>
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

    alerts_fqn  = f"{BQ_PROJECT}.{BQ_DATASET}.alerts_daily"
    channel_fqn = f"{BQ_PROJECT}.{BQ_DATASET}.segment_by_channel_daily"
    behavior_fqn= f"{BQ_PROJECT}.{BQ_DATASET}.daily_behavior_segments"
    abandon_fqn = f"{BQ_PROJECT}.{BQ_DATASET}.abandon_recovery_summary_daily"

    # ✅ 날짜컬럼 자동 감지해서 가져오기 (date vs snapshot_dt 문제 해결)
    df_alerts = read_daily_table(alerts_fqn, yesterday)
    df_channel = read_daily_table(channel_fqn, yesterday)
    df_behavior = read_daily_table(behavior_fqn, yesterday)
    df_abandon = read_daily_table(abandon_fqn, yesterday)

    blocks = [
        card("🚨 이상 징후 (Alerts)", "어제 기준 급변 지표", df_html(df_alerts, 10)),
        card("📊 채널별 Daily 성과", "어제 기준 유입/성과 흐름", df_html(df_channel, 15)),
        card("👥 유저 행동 세그먼트", "어제 기준 유저 질/관여도(최근 7일/3일 윈도우)", df_html(df_behavior, 15)),
        card("🛒 Abandon Recovery 요약", "어제 기준 이탈/복구 요약", df_html(df_abandon, 15)),
    ]

    html = f"""
    <h2 style="margin:0;">MD Daily Digest</h2>
    <p style="margin:6px 0 14px;color:#475467;">기준일: {yesterday} (KST)</p>
    {''.join(blocks)}
    """

    attachments = [
        (f"alerts_daily_{yesterday}.csv", df_to_csv_bytes(df_alerts)),
        (f"segment_by_channel_daily_{yesterday}.csv", df_to_csv_bytes(df_channel)),
        (f"daily_behavior_segments_{yesterday}.csv", df_to_csv_bytes(df_behavior)),
        (f"abandon_recovery_summary_daily_{yesterday}.csv", df_to_csv_bytes(df_abandon)),
    ]

    send_mail(
        subject=f"[MD Daily] Alerts/채널/세그먼트/Abandon 요약 ({yesterday})",
        html=html,
        recipients=MD_DAILY_RECIPIENTS,
        attachments=attachments,
    )


if __name__ == "__main__":
    run_md_daily()

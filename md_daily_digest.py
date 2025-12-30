#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Columbia KR - MD Digest (DAILY)
- 최근 24h / 어제 기준 Daily 모니터링
- 주간 의사결정 ❌ / 즉시 액션용
- Gmail / Outlook SMTP

DATA:
- checkout_abandon_last24h
- alerts_daily
- segment_by_channel_daily
"""

import os
import json
import smtplib
import pandas as pd
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from typing import List, Tuple
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication


# -----------------------
# Config
# -----------------------
BQ_PROJECT = os.getenv("BQ_PROJECT", "columbia-ga4").strip()
BQ_DATASET = os.getenv("BQ_DATASET", "mart").strip()

SMTP_PROVIDER = os.getenv("SMTP_PROVIDER", "gmail").lower()
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


# -----------------------
# SMTP
# -----------------------
def _smtp_host_port():
    if SMTP_PROVIDER == "gmail":
        return ("smtp.gmail.com", 587)
    return ("smtp.office365.com", 587)


def send_mail(subject, html, recipients, attachments):
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
        server.starttls()
        server.login(SMTP_USER, SMTP_PASS)
        server.sendmail(SMTP_USER, recipients, msg.as_string())


# -----------------------
# HTML
# -----------------------
def df_html(df, n=10):
    if df.empty:
        return "<div style='color:#999;'>데이터 없음</div>"
    return df.head(n).to_html(index=False, border=0)


def card(title, desc, body):
    return f"""
    <div style="background:#fff;border:1px solid #e6eaf2;border-radius:12px;padding:14px;margin-bottom:12px;">
      <b>{title}</b><br/>
      <span style="font-size:12px;color:#667085">{desc}</span>
      <div style="margin-top:8px;">{body}</div>
    </div>
    """


# -----------------------
# MAIN
# -----------------------
def run_md_daily():
    kst = ZoneInfo("Asia/Seoul")
    today = datetime.now(kst).date()
    yesterday = today - timedelta(days=1)

    df_alerts = bq_query_df(f"""
      SELECT *
      FROM `{BQ_PROJECT}.{BQ_DATASET}.alerts_daily`
      WHERE date = '{yesterday}'
    """)

    df_abandon = bq_query_df(f"""
      SELECT *
      FROM `{BQ_PROJECT}.{BQ_DATASET}.checkout_abandon_last24h`
    """)

    df_channel = bq_query_df(f"""
      SELECT *
      FROM `{BQ_PROJECT}.{BQ_DATASET}.segment_by_channel_daily`
      WHERE date = '{yesterday}'
    """)

    blocks = [
        card("🚨 이상 징후 (Alerts)", "어제 기준 급변 지표", df_html(df_alerts)),
        card("🛒 Checkout Abandon (Last 24h)", "최근 24시간 이탈", df_html(df_abandon)),
        card("📊 채널별 Daily 성과", "어제 기준", df_html(df_channel)),
    ]

    html = f"""
    <h2>MD Daily Digest</h2>
    <p>기준일: {yesterday}</p>
    {''.join(blocks)}
    """

    attachments = [
        (f"alerts_daily_{yesterday}.csv", df_alerts.to_csv(index=False).encode("utf-8-sig")),
        (f"checkout_abandon_last24h_{today}.csv", df_abandon.to_csv(index=False).encode("utf-8-sig")),
        (f"segment_by_channel_daily_{yesterday}.csv", df_channel.to_csv(index=False).encode("utf-8-sig")),
    ]

    send_mail(
        subject=f"[MD Daily] 이상징후/이탈/채널 요약 ({yesterday})",
        html=html,
        recipients=MD_DAILY_RECIPIENTS,
        attachments=attachments,
    )


if __name__ == "__main__":
    run_md_daily()

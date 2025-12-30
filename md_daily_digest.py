#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Columbia KR - MD Digest (DAILY)
- 최근 24h / 어제 기준 Daily 모니터링
- 주간 의사결정 ❌ / 즉시 액션용
- Gmail / Outlook SMTP

DATA:
- alerts_daily (어제)
- segment_by_channel_daily (어제)
- daily_behavior_segments (어제)
- abandon_recovery_summary_daily (어제)  ✅ checkout_abandon_last24h 대체
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

SMTP_PROVIDER = os.getenv("SMTP_PROVIDER", "gmail").lower().strip()
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
# HTML
# -----------------------
def df_html(df: pd.DataFrame, n: int = 10) -> str:
    if df is None or df.empty:
        return "<div style='color:#999;font-size:12px;'>데이터 없음</div>"
    d = df.head(n).copy()
    return d.to_html(index=False, border=0)


def card(title: str, desc: str, body: str) -> str:
    return f"""
    <div style="background:#fff;border:1px solid #e6eaf2;border-radius:12px;padding:14px;margin-bottom:12px;">
      <div style="font-weight:800;">{title}</div>
      <div style="font-size:12px;color:#667085;margin-top:4px;">{desc}</div>
      <div style="margin-top:10px;">{body}</div>
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

    df_channel = bq_query_df(f"""
      SELECT *
      FROM `{BQ_PROJECT}.{BQ_DATASET}.segment_by_channel_daily`
      WHERE date = '{yesterday}'
    """)

    df_behavior = bq_query_df(f"""
      SELECT *
      FROM `{BQ_PROJECT}.{BQ_DATASET}.daily_behavior_segments`
      WHERE date = '{yesterday}'
    """)

    df_abandon_summary = bq_query_df(f"""
      SELECT *
      FROM `{BQ_PROJECT}.{BQ_DATASET}.abandon_recovery_summary_daily`
      WHERE date = '{yesterday}'
    """)

    blocks = [
        card("🚨 이상 징후 (Alerts)", "어제 기준 급변 지표", df_html(df_alerts, 10)),
        card("📊 채널별 Daily 성과", "어제 기준 유입/성과 흐름", df_html(df_channel, 15)),
        card("👥 유저 행동 세그먼트", "어제 기준 유저 질 변화", df_html(df_behavior, 15)),
        card("🛒 Abandon Recovery 요약", "어제 기준 이탈/복구 요약", df_html(df_abandon_summary, 15)),
    ]

    html = f"""
    <h2 style="margin:0;">MD Daily Digest</h2>
    <p style="margin:6px 0 14px;color:#475467;">기준일: {yesterday} (KST)</p>
    {''.join(blocks)}
    """

    attachments = [
        (f"alerts_daily_{yesterday}.csv", df_alerts.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")),
        (f"segment_by_channel_daily_{yesterday}.csv", df_channel.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")),
        (f"daily_behavior_segments_{yesterday}.csv", df_behavior.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")),
        (f"abandon_recovery_summary_daily_{yesterday}.csv", df_abandon_summary.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")),
    ]

    send_mail(
        subject=f"[MD Daily] Alerts/채널/세그먼트/Abandon 요약 ({yesterday})",
        html=html,
        recipients=MD_DAILY_RECIPIENTS,
        attachments=attachments,
    )


if __name__ == "__main__":
    run_md_daily()

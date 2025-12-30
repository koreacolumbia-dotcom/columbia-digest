#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Columbia KR - MD Digest (WEEKLY)
- BigQuery 주간 집계 결과 요약 + CSV 자동 첨부
- 최근 완료 주차(week_start_dt ~ week_end_dt) 기준
- Outlook / Gmail SMTP 발송

ENV:
  BQ_PROJECT, BQ_DATASET
  GOOGLE_APPLICATION_CREDENTIALS or GCP_SA_JSON
  SMTP_PROVIDER=gmail|outlook
  SMTP_USER, SMTP_PASS
  MD_WEEKLY_RECIPIENTS="a@x.com,b@x.com"
"""

import os
import json
import smtplib
import pandas as pd
from datetime import datetime
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

MD_WEEKLY_RECIPIENTS = [
    e.strip() for e in os.getenv("hugh.kang@columbia.com", "").split(",") if e.strip()
]


# -----------------------
# BigQuery
# -----------------------
def _build_bq_client():
    sa_json = os.getenv("GCP_SA_JSON", "").strip()
    from google.cloud import bigquery

    if sa_json:
        from google.oauth2 import service_account
        info = json.loads(sa_json)
        creds = service_account.Credentials.from_service_account_info(info)
        return bigquery.Client(project=BQ_PROJECT, credentials=creds)

    return bigquery.Client(project=BQ_PROJECT)


def bq_query_df(sql: str) -> pd.DataFrame:
    client = _build_bq_client()
    return client.query(sql).result().to_dataframe()


# -----------------------
# Mail (SMTP)
# -----------------------
def _smtp_host_port():
    if SMTP_PROVIDER == "outlook":
        return ("smtp.office365.com", 587)
    if SMTP_PROVIDER == "gmail":
        return ("smtp.gmail.com", 587)
    return ("smtp.gmail.com", 587)


def send_email_html(
    subject: str,
    html_body: str,
    recipients: List[str],
    attachments: List[Tuple[str, bytes]],
):
    if not recipients:
        print("[WARN] MD_WEEKLY_RECIPIENTS empty - skip send")
        return

    if not (SMTP_USER and SMTP_PASS):
        raise RuntimeError("SMTP_USER / SMTP_PASS missing")

    host, port = _smtp_host_port()

    msg = MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"] = SMTP_USER
    msg["To"] = ", ".join(recipients)

    alt = MIMEMultipart("alternative")
    msg.attach(alt)
    alt.attach(MIMEText("MD Weekly Digest (HTML). Open in Outlook.", "plain", "utf-8"))
    alt.attach(MIMEText(html_body, "html", "utf-8"))

    for fname, fbytes in attachments:
        part = MIMEApplication(fbytes, Name=fname)
        part["Content-Disposition"] = f'attachment; filename="{fname}"'
        msg.attach(part)

    with smtplib.SMTP(host, port) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASS)
        server.sendmail(SMTP_USER, recipients, msg.as_string())

    print(f"[OK] Weekly mail sent → {len(recipients)} recipients")


# -----------------------
# HTML helpers
# -----------------------
def df_to_html_table(df: pd.DataFrame, max_rows: int = 15) -> str:
    if df is None or df.empty:
        return "<div style='color:#999;font-size:12px;'>데이터 없음</div>"

    d = df.head(max_rows).copy()
    html = d.to_html(index=False, border=0, escape=False)
    html = html.replace(
        '<table border="0" class="dataframe">',
        '<table style="width:100%; border-collapse:collapse; font-size:12px; table-layout:fixed;">',
    )
    html = html.replace(
        "<th>",
        "<th style='text-align:left;padding:6px 8px;background:#f3f6fb;"
        "border-bottom:1px solid #e6eaf2;word-break:keep-all;'>",
    )
    html = html.replace(
        "<td>",
        "<td style='text-align:left;padding:6px 8px;border-bottom:1px solid #f0f2f7;"
        "word-break:keep-all;'>",
    )
    return html


def card(title: str, desc: str, inner_html: str) -> str:
    return f"""
    <div style="background:#ffffff;border:1px solid #e6eaf2;
                border-radius:14px;padding:14px;margin-bottom:12px;">
      <div style="font-size:13px;font-weight:800;color:#0b4f6c;">{title}</div>
      <div style="font-size:12px;color:#667085;margin:6px 0 10px;">{desc}</div>
      {inner_html}
    </div>
    """


def build_html(date_label: str, blocks: List[str]) -> str:
    return f"""<!doctype html>
<html lang="ko">
<body style="margin:0;background:#f5f7fb;font-family:Segoe UI,Apple SD Gothic Neo;">
  <div style="max-width:920px;margin:0 auto;padding:22px;">
    <div style="background:#fff;border-radius:18px;padding:18px;border:1px solid #e6eaf2;">
      <h2 style="margin:0;color:#0055a5;">MD Weekly Digest</h2>
      <p style="font-size:13px;color:#475467;">
        집계 기준: 최근 완료 주차 (week_start_dt ~ week_end_dt)<br/>
        생성일: {date_label}
      </p>
    </div>
    <div style="margin-top:14px;">
      {"".join(blocks)}
    </div>
    <div style="font-size:11px;color:#98a2b3;text-align:right;margin-top:12px;">
      Generated by BigQuery (mart)
    </div>
  </div>
</body>
</html>"""


def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")


# -----------------------
# Main
# -----------------------
def run_md_weekly():
    today = datetime.now().strftime("%Y-%m-%d")

    v_high = f"{BQ_PROJECT}.{BQ_DATASET}.v_md_high_cvr_items"
    v_low  = f"{BQ_PROJECT}.{BQ_DATASET}.v_md_low_cvr_items"
    t_item = f"{BQ_PROJECT}.{BQ_DATASET}.pdp_to_atc_item_weekly"

    df_high = bq_query_df(f"SELECT * FROM `{v_high}`")
    df_low  = bq_query_df(f"SELECT * FROM `{v_low}`")
    df_item = bq_query_df(f"SELECT * FROM `{t_item}`")

    latest_week = df_item["week_start_dt"].max() if not df_item.empty else None

    def only_latest(df):
        if df is None or df.empty or latest_week is None:
            return df
        return df[df["week_start_dt"] == latest_week]

    blocks = [
        card("노출 확대 후보 (High CVR)",
             "최근 완료 주차 기준, 상단/기획전 확장 후보",
             df_to_html_table(only_latest(df_high))),
        card("전환 개선 후보 (Low CVR)",
             "노출 대비 PDP→Cart 전환이 약한 상품",
             df_to_html_table(only_latest(df_low))),
    ]

    html = build_html(today, blocks)

    attachments = [
        (f"md_high_cvr_items_weekly_{today}.csv", df_to_csv_bytes(df_high)),
        (f"md_low_cvr_items_weekly_{today}.csv", df_to_csv_bytes(df_low)),
        (f"pdp_to_atc_item_weekly_{today}.csv", df_to_csv_bytes(df_item)),
    ]

    subject = f"[MD Weekly] PDP→Cart 기반 상품 액션 후보 ({today})"
    send_email_html(subject, html, MD_WEEKLY_RECIPIENTS, attachments)


if __name__ == "__main__":
    run_md_weekly()

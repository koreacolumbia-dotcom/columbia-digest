#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Columbia KR - MD Digest (DAILY)
- BigQuery 결과 요약 + CSV 자동 첨부
- Outlook(HTML) 보기 최적화

Required ENV:
  BQ_PROJECT=columbia-ga4 (default)
  BQ_DATASET=mart (default)
  GOOGLE_APPLICATION_CREDENTIALS=/path/key.json  (recommended)
    or GCP_SA_JSON=<service account json string>

  SMTP_PROVIDER=outlook
  SMTP_USER=...
  SMTP_PASS=...
  MD_DAILY_RECIPIENTS="a@x.com,b@x.com"
"""

import os
import json
import smtplib
import pandas as pd
from datetime import datetime
from typing import List, Tuple, Optional
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.application import MIMEApplication

# =======================
# Service Account (GCP)
# =======================
GCP_SA_JSON = os.getenv("GCP_SA_JSON", "").strip()
if GCP_SA_JSON:
    SERVICE_ACCOUNT_FILE = "/tmp/gcp_service_account.json"
    with open(SERVICE_ACCOUNT_FILE, "w", encoding="utf-8") as f:
        f.write(GCP_SA_JSON)

# -----------------------
# Config
# -----------------------
BQ_PROJECT = os.getenv("BQ_PROJECT", "columbia-ga4").strip()
BQ_DATASET = os.getenv("BQ_DATASET", "mart").strip()

SMTP_PROVIDER = os.getenv("SMTP_PROVIDER", "gmail").lower()  # "gmail" or "outlook"
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER", "koreacolumbia@gmail.com")
SMTP_PASS = os.getenv("SMTP_PASS", "xxopfytdkxcyhisa")

MD_DAILY_RECIPIENTS = [
    e.strip() for e in os.getenv("MD_DAILY_RECIPIENTS", "hugh.kang@columbia.com").split(",") if e.strip()
]

# -----------------------
# BigQuery client
# -----------------------
def _build_bq_client():
    from google.cloud import bigquery

    if SERVICE_ACCOUNT_FILE and os.path.exists(SERVICE_ACCOUNT_FILE):
        from google.oauth2 import service_account
        creds = service_account.Credentials.from_service_account_file(
            SERVICE_ACCOUNT_FILE,
            scopes=["https://www.googleapis.com/auth/cloud-platform"],
        )
        project = getattr(creds, "project_id", None) or BQ_PROJECT
        return bigquery.Client(project=project, credentials=creds)

    return bigquery.Client(project=BQ_PROJECT)

def bq_query_df(sql: str) -> pd.DataFrame:
    client = _build_bq_client()
    job = client.query(sql)
    return job.result().to_dataframe()

# -----------------------
# Mail (Outlook friendly + attachments)
# -----------------------
def _smtp_host_port():
    if SMTP_PROVIDER == "outlook":
        return ("smtp.office365.com", 587)
    if SMTP_PROVIDER == "gmail":
        return ("smtp.gmail.com", 587)
    return (os.getenv("SMTP_HOST", "smtp.office365.com"), int(os.getenv("SMTP_PORT", "587")))

def send_email_html(subject: str, html_body: str, recipients: List[str], attachments: List[Tuple[str, bytes]]):
    if not recipients:
        print("[WARN] recipients empty - skip send")
        return
    if not (SMTP_USER and SMTP_PASS):
        print("[WARN] SMTP_USER/SMTP_PASS missing - preview only")
        print(subject)
        print(html_body[:3000])
        print("attachments:", [a[0] for a in attachments])
        return

    host, port = _smtp_host_port()

    msg = MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"] = SMTP_USER
    msg["To"] = ", ".join(recipients)

    alt = MIMEMultipart("alternative")
    msg.attach(alt)
    alt.attach(MIMEText("MD Digest (HTML). If you can't see it, open in Outlook.", "plain", "utf-8"))
    alt.attach(MIMEText(html_body, "html", "utf-8"))

    for fname, fbytes in attachments:
        part = MIMEApplication(fbytes, Name=fname)
        part["Content-Disposition"] = f'attachment; filename="{fname}"'
        msg.attach(part)

    with smtplib.SMTP(host, port) as server:
        server.starttls()
        server.login(SMTP_USER, SMTP_PASS)
        server.sendmail(SMTP_USER, recipients, msg.as_string())

# -----------------------
# HTML helpers
# -----------------------
def df_to_html_table(df: pd.DataFrame, max_rows: int = 10) -> str:
    if df is None or df.empty:
        return "<div style='color:#999;font-size:12px;'>데이터 없음</div>"

    d = df.head(max_rows).copy()
    # Outlook-friendly inline table
    html = d.to_html(index=False, border=0, escape=False)
    html = html.replace(
        '<table border="0" class="dataframe">',
        '<table style="width:100%; border-collapse:collapse; font-size:12px; table-layout:fixed;">'
    )
    html = html.replace(
        "<th>",
        "<th style='text-align:left;padding:6px 8px;background:#f3f6fb;border-bottom:1px solid #e6eaf2;"
        "word-break:keep-all;white-space:normal;overflow-wrap:anywhere;'>"
    )
    html = html.replace(
        "<td>",
        "<td style='text-align:left;padding:6px 8px;border-bottom:1px solid #f0f2f7;"
        "word-break:keep-all;white-space:normal;overflow-wrap:anywhere;'>"
    )
    return html

def card(title: str, desc: str, inner_html: str) -> str:
    return f"""
    <div style="background:#ffffff;border:1px solid #e6eaf2;border-radius:14px;padding:14px 14px;margin-bottom:12px;">
      <div style="font-size:13px;font-weight:800;color:#0b4f6c;margin-bottom:4px;">{title}</div>
      <div style="font-size:12px;color:#667085;line-height:1.5;margin-bottom:10px;">{desc}</div>
      {inner_html}
    </div>
    """

def build_html(date_label: str, blocks: List[str]) -> str:
    body = "\n".join(blocks)
    return f"""<!doctype html>
<html lang="ko">
<head><meta charset="utf-8"></head>
<body style="margin:0;background:#f5f7fb;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI','Noto Sans KR',Arial,sans-serif;">
  <div style="max-width:920px;margin:0 auto;padding:22px 12px;">
    <div style="background:#ffffff;border:1px solid #e6eaf2;border-radius:18px;padding:18px 18px;">
      <div style="font-size:18px;font-weight:900;color:#0055a5;">MD Daily Digest</div>
      <div style="font-size:13px;color:#475467;margin-top:4px;">{date_label} 기준 · 주간 집계 최신 주차(노이즈 제거)로 Daily 참고용</div>
      <div style="font-size:12px;color:#667085;margin-top:10px;line-height:1.6;">
        - 목적: 오늘 ‘노출 확대’/‘전환 개선’이 필요한 상품을 빠르게 체크<br/>
        - 의사결정은 Weekly 메일(전체 주차 비교)을 기준으로 권장
      </div>
    </div>

    <div style="margin-top:14px;">
      {body}
    </div>

    <div style="font-size:11px;color:#98a2b3;text-align:right;margin-top:12px;">
      Generated by BigQuery (mart) · mailed via Python
    </div>
  </div>
</body>
</html>"""

# -----------------------
# Data logic (DAILY = latest week only)
# -----------------------
def latest_week_filter_sql(view_fqn: str) -> str:
    # assumes the view has week_start_dt
    return f"""
    SELECT *
    FROM `{view_fqn}`
    WHERE week_start_dt = (SELECT MAX(week_start_dt) FROM `{view_fqn}`)
    """

def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")

def run_md_daily():
    today = datetime.now().strftime("%Y-%m-%d")

    v_high = f"{BQ_PROJECT}.{BQ_DATASET}.v_md_high_cvr_items"
    v_low  = f"{BQ_PROJECT}.{BQ_DATASET}.v_md_low_cvr_items"
    t_item = f"{BQ_PROJECT}.{BQ_DATASET}.pdp_to_atc_item_weekly"

    # 최신 주차만
    df_high = bq_query_df(latest_week_filter_sql(v_high))
    df_low  = bq_query_df(latest_week_filter_sql(v_low))
    df_item = bq_query_df(f"""
      SELECT *
      FROM `{t_item}`
      WHERE week_start_dt = (SELECT MAX(week_start_dt) FROM `{t_item}`)
    """)

    # 메일 본문 요약(상위 10줄씩)
    blocks = []
    blocks.append(card(
        "노출 확대 후보 (High CVR)",
        "PDP→Cart 전환이 높은데(또는 안정적인데) 노출이 과하지 않은 상품. 상단 슬롯/기획전 배치 후보.",
        df_to_html_table(df_high, 10)
    ))
    blocks.append(card(
        "전환 개선 후보 (Low CVR)",
        "조회/유저는 있는데 PDP→Cart가 약한 상품. 가격/옵션/혜택/리뷰/배송 정보 노출을 점검.",
        df_to_html_table(df_low, 10)
    ))
    blocks.append(card(
        "상품 퍼널 근거 (PDP→ATC Item Weekly · 최신 주차)",
        "위 후보들의 근거 테이블(상품별 PDP/ATC/전환율). 필요 시 필터링해 상세 검토.",
        df_to_html_table(df_item, 10)
    ))

    html = build_html(f"{today}", blocks)

    # 첨부파일 3개
    attachments = [
        (f"md_high_cvr_items_{today}.csv", df_to_csv_bytes(df_high)),
        (f"md_low_cvr_items_{today}.csv", df_to_csv_bytes(df_low)),
        (f"pdp_to_atc_item_weekly_latest_{today}.csv", df_to_csv_bytes(df_item)),
    ]

    subject = f"[MD Daily] PDP→Cart 기반 상품 액션 후보 ({today})"
    send_email_html(subject, html, MD_DAILY_RECIPIENTS, attachments)

if __name__ == "__main__":
    run_md_daily()

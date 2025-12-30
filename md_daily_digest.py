#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Columbia KR - MD Digest (DAILY)
- BigQuery 결과 요약 + CSV 자동 첨부
- Outlook/Gmail(HTML) 보기 최적화
- Gmail SMTP로 메일 발송

Required ENV:
  # BigQuery
  BQ_PROJECT=columbia-ga4 (default)
  BQ_DATASET=mart (default)
  GCP_SA_JSON=<service account json string>

  # SMTP (Gmail 권장)
  SMTP_PROVIDER=gmail
  SMTP_USER=koreacolumbia@gmail.com
  SMTP_PASS=<gmail app password>
  MD_DAILY_RECIPIENTS="a@x.com,b@x.com"
"""

import os
import json
import smtplib
import pandas as pd
from datetime import datetime
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


def _smtp_host_port():
    if SMTP_PROVIDER == "gmail":
        return ("smtp.gmail.com", 587)
    if SMTP_PROVIDER == "outlook":
        return ("smtp.office365.com", 587)
    host = os.getenv("SMTP_HOST", "").strip()
    port = int(os.getenv("SMTP_PORT", "587"))
    if not host:
        raise RuntimeError("SMTP_HOST missing (when SMTP_PROVIDER is not gmail/outlook)")
    return (host, port)


# -----------------------
# BigQuery client
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
    job = client.query(sql)
    return job.result().to_dataframe()


# -----------------------
# Mail (SMTP)
# -----------------------
def send_email_html(subject: str, html_body: str, recipients: List[str], attachments: List[Tuple[str, bytes]]):
    if not recipients:
        print("[WARN] MD_DAILY_RECIPIENTS empty - skip send")
        return
    if not SMTP_USER:
        raise RuntimeError("SMTP_USER missing (check GitHub Secrets)")
    if not SMTP_PASS:
        raise RuntimeError("SMTP_PASS missing (use Gmail App Password)")

    host, port = _smtp_host_port()

    msg = MIMEMultipart("mixed")
    msg["Subject"] = subject
    msg["From"] = SMTP_USER
    msg["To"] = ", ".join(recipients)

    alt = MIMEMultipart("alternative")
    msg.attach(alt)
    alt.attach(MIMEText("MD Digest (HTML). If you can't see it, open in a mail client.", "plain", "utf-8"))
    alt.attach(MIMEText(html_body, "html", "utf-8"))

    for fname, fbytes in attachments:
        part = MIMEApplication(fbytes, Name=fname)
        part["Content-Disposition"] = f'attachment; filename="{fname}"'
        msg.attach(part)

    with smtplib.SMTP(host, port, timeout=30) as server:
        server.ehlo()
        server.starttls()
        server.ehlo()
        server.login(SMTP_USER, SMTP_PASS)
        server.sendmail(SMTP_USER, recipients, msg.as_string())

    print(f"[OK] SMTP mail sent via {SMTP_PROVIDER}: from={SMTP_USER}, to={len(recipients)}, attachments={len(attachments)}")


# -----------------------
# HTML helpers
# -----------------------
def df_to_html_table(df: pd.DataFrame, max_rows: int = 10) -> str:
    if df is None or df.empty:
        return "<div style='color:#999;font-size:12px;'>데이터 없음</div>"

    d = df.head(max_rows).copy()
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
      Generated by BigQuery (mart) · mailed via Gmail SMTP
    </div>
  </div>
</body>
</html>"""


# -----------------------
# Data logic
# -----------------------
def latest_week_filter_sql(view_fqn: str) -> str:
    return f"""
    SELECT *
    FROM `{view_fqn}`
    WHERE week_start_dt = (SELECT MAX(week_start_dt) FROM `{view_fqn}`)
    """


def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    return df.to_csv(index=False, encoding="utf-8-sig").encode("utf-8-sig")


def run_md_daily():
    kst = ZoneInfo("Asia/Seoul")
    today = datetime.now(tz=kst).strftime("%Y-%m-%d")

    v_high = f"{BQ_PROJECT}.{BQ_DATASET}.v_md_high_cvr_items"
    v_low  = f"{BQ_PROJECT}.{BQ_DATASET}.v_md_low_cvr_items"
    t_item = f"{BQ_PROJECT}.{BQ_DATASET}.pdp_to_atc_item_weekly"

    df_high = bq_query_df(latest_week_filter_sql(v_high))
    df_low  = bq_query_df(latest_week_filter_sql(v_low))
    df_item = bq_query_df(f"""
      SELECT *
      FROM `{t_item}`
      WHERE week_start_dt = (SELECT MAX(week_start_dt) FROM `{t_item}`)
    """)

    blocks: List[str] = []
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

    html = build_html(today, blocks)

    attachments = [
        (f"md_high_cvr_items_{today}.csv", df_to_csv_bytes(df_high)),
        (f"md_low_cvr_items_{today}.csv", df_to_csv_bytes(df_low)),
        (f"pdp_to_atc_item_weekly_latest_{today}.csv", df_to_csv_bytes(df_item)),
    ]

    subject = f"[MD Daily] PDP→Cart 기반 상품 액션 후보 ({today})"
    send_email_html(subject, html, MD_DAILY_RECIPIENTS, attachments)


if __name__ == "__main__":
    run_md_daily()

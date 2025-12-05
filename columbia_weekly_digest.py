# ============================================================================
# COLUMBIA KOREA – WEEKLY DIGEST (PART 1)
# Base setting / GA4 / Date logic / KPI engine
# ============================================================================

import os
import smtplib
import pandas as pd
from datetime import datetime, timedelta

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

from google.analytics.data_v1beta import BetaAnalyticsDataClient
from google.analytics.data_v1beta.types import (
    DateRange, Dimension, Metric, RunReportRequest
)
from google.oauth2 import service_account


# ============================================================================
# 0) ENV CONFIG
# ============================================================================

GA4_PROPERTY_ID = os.getenv("GA4_PROPERTY_ID", "").strip()

SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", "587"))
SMTP_USER = os.getenv("SMTP_USER")
SMTP_PASS = os.getenv("SMTP_PASS")

WEEKLY_RECIPIENTS = [
    x.strip()
    for x in os.getenv("WEEKLY_RECIPIENTS", "hugh.kang@columbia.com").split(",")
    if x.strip()
]

SERVICE_ACCOUNT_FILE = os.getenv(
    "GA4_SERVICE_ACCOUNT_FILE",
    "ga_service_account.json"
)


# ============================================================================
# 1) UTILITIES
# ============================================================================

def safe_int(x):
    try:
        return int(float(x))
    except:
        return 0


def safe_float(x):
    try:
        return float(x)
    except:
        return 0.0


def pct(curr, prev):
    if prev == 0:
        return 0.0
    try:
        return round((curr - prev) / prev * 100, 1)
    except:
        return 0.0


def money(v):
    return f"{int(round(safe_float(v))):,}원"


def money_m(v):
    return f"{int(round(safe_float(v) / 10000)):,}만원"


# ============================================================================
# 2) GA4 CLIENT
# ============================================================================

def ga_client():
    if not GA4_PROPERTY_ID:
        raise SystemExit("GA4_PROPERTY_ID empty.")
    if not os.path.exists(SERVICE_ACCOUNT_FILE):
        raise SystemExit(f"GA service account file missing: {SERVICE_ACCOUNT_FILE}")

    cred = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/analytics.readonly"]
    )

    return BetaAnalyticsDataClient(credentials=cred)


def ga_run(dimensions, metrics, start_date, end_date, limit=None):
    client = ga_client()

    req = RunReportRequest(
        property=f"properties/{GA4_PROPERTY_ID}",
        date_ranges=[DateRange(start_date=start_date, end_date=end_date)],
        dimensions=[Dimension(name=d) for d in dimensions],
        metrics=[Metric(name=m) for m in metrics],
        limit=limit or 0,
    )

    res = client.run_report(req)

    headers = (
        [h.name for h in res.dimension_headers] +
        [h.name for h in res.metric_headers]
    )

    rows = []
    for r in res.rows:
        rows.append(
            [d.value for d in r.dimension_values] +
            [m.value for m in r.metric_values]
        )

    df = pd.DataFrame(rows, columns=headers)

    for c in df.columns:
        try:
            df[c] = pd.to_numeric(df[c])
        except:
            pass

    return df


# ============================================================================
# 3) DATE RANGES (WEEKLY FIXED)
# ============================================================================

def build_week_ranges():
    """
    월요일 실행 기준

    this : 직전주 Mon ~ Sun
    ld   : 직전주 이전주 (W-1)
    lw   : 그 이전주 (W-2)
    ly   : 전년 동일 주차
    """
    today = datetime.now()
    weekday = today.weekday()

    this_mon = today - timedelta(days=weekday + 7)
    this_mon = this_mon.date()
    this_sun = this_mon + timedelta(days=6)

    ld_mon = this_mon - timedelta(days=7)
    lw_mon = this_mon - timedelta(days=14)

    try:
        ly_mon = this_mon.replace(year=this_mon.year - 1)
    except:
        ly_mon = this_mon - timedelta(days=365)

    def r(mon):
        sun = mon + timedelta(days=6)
        return mon.strftime("%Y-%m-%d"), sun.strftime("%Y-%m-%d")

    def f(d):
        return d.strftime("%Y-%m-%d")

    label = f"{f(this_mon)} ~ {f(this_sun)}"

    return {
        "label": label,
        "this": r(this_mon),
        "ld": r(ld_mon),
        "lw": r(lw_mon),
        "ly": r(ly_mon),
    }


# ============================================================================
# 4) KPI SOURCE
# ============================================================================

def src_kpi(start, end):
    df = ga_run(
        dimensions=[],
        metrics=["sessions", "transactions", "purchaseRevenue", "newUsers"],
        start_date=start,
        end_date=end,
    )

    if df.empty:
        return dict(
            sessions=0,
            transactions=0,
            purchaseRevenue=0,
            newUsers=0,
        )

    r = df.iloc[0]

    return dict(
        sessions=safe_int(r["sessions"]),
        transactions=safe_int(r["transactions"]),
        purchaseRevenue=safe_float(r["purchaseRevenue"]),
        newUsers=safe_int(r["newUsers"]),
    )


def src_channel_uv(start, end):
    df = ga_run(
        dimensions=["sessionDefaultChannelGroup"],
        metrics=["sessions"],
        start_date=start,
        end_date=end,
    )

    if df.empty:
        return dict(
            total_uv=0,
            organic_uv=0,
            nonorganic_uv=0,
            organic_share=0
        )

    df["sessions"] = pd.to_numeric(df["sessions"]).fillna(0).astype(int)

    total = int(df["sessions"].sum())
    organic = int(
        df.loc[df["sessionDefaultChannelGroup"] == "Organic Search", "sessions"].sum()
    )
    nonorganic = total - organic
    share = round((organic / total) * 100, 1) if total > 0 else 0

    return dict(
        total_uv=total,
        organic_uv=organic,
        nonorganic_uv=nonorganic,
        organic_share=share
    )


def build_kpi_pack():

    ranges = build_week_ranges()

    k_this = src_kpi(*ranges["this"])
    k_ld   = src_kpi(*ranges["ld"])
    k_lw   = src_kpi(*ranges["lw"])
    k_ly   = src_kpi(*ranges["ly"])

    ch_this = src_channel_uv(*ranges["this"])
    ch_ld   = src_channel_uv(*ranges["ld"])
    ch_lw   = src_channel_uv(*ranges["lw"])
    ch_ly   = src_channel_uv(*ranges["ly"])

    def cvr(k):
        return round((k["transactions"] / k["sessions"] * 100), 2) if k["sessions"] else 0

    def aov(k):
        return round(k["purchaseRevenue"] / k["transactions"], 0) if k["transactions"] else 0

    pack = {

        "range_label": ranges["label"],

        "revenue": {
            "today": k_this["purchaseRevenue"],
            "ld": k_ld["purchaseRevenue"],
            "lw": k_lw["purchaseRevenue"],
            "ly": k_ly["purchaseRevenue"],
        },

        "uv": {
            "today": k_this["sessions"],
            "ld": k_ld["sessions"],
            "lw": k_lw["sessions"],
            "ly": k_ly["sessions"],
        },

        "orders": {
            "today": k_this["transactions"],
            "ld": k_ld["transactions"],
            "lw": k_lw["transactions"],
            "ly": k_ly["transactions"],
        },

        "new": {
            "today": k_this["newUsers"],
            "ld": k_ld["newUsers"],
            "lw": k_lw["newUsers"],
            "ly": k_ly["newUsers"],
        },

        "cvr": {
            "today": cvr(k_this),
            "ld": cvr(k_ld),
            "lw": cvr(k_lw),
            "ly": cvr(k_ly),
        },

        "aov": {
            "today": aov(k_this),
            "ld": aov(k_ld),
            "lw": aov(k_lw),
            "ly": aov(k_ly),
        },

        "organic_uv": {
            "today": ch_this["organic_uv"],
            "ld": ch_ld["organic_uv"],
            "lw": ch_lw["organic_uv"],
            "ly": ch_ly["organic_uv"],
        },

        "nonorganic_uv": {
            "today": ch_this["nonorganic_uv"],
            "ld": ch_ld["nonorganic_uv"],
            "lw": ch_lw["nonorganic_uv"],
            "ly": ch_ly["nonorganic_uv"],
        },

        "organic_share": {
            "today": ch_this["organic_share"],
            "ld": ch_ld["organic_share"],
            "lw": ch_lw["organic_share"],
            "ly": ch_ly["organic_share"],
        }

    }

    return pack

# ============================================================================
# COLUMBIA KOREA – WEEKLY DIGEST (PART 2)
# Insight Engine & Action Generator
# ============================================================================

# ============================================================================
# 5) INSIGHT TEXT GENERATOR
# ============================================================================

def build_4step_insight(title, metric_name, today, prev):
    """
    WHAT → WHY → RISK → FOCUS 4단 자동 문장 생성
    """
    delta_pct = pct(today, prev)

    # WHAT
    what = (
        f"{title}는 전주 대비 {delta_pct:+.1f}% "
        f"{'상승' if delta_pct >= 0 else '감소'}했습니다."
    )

    # WHY
    if metric_name == "revenue":
        why = (
            "이는 전환율과 객단가 복합 변화에 의해 매출 구조가 재편된 결과로,"
            " 단순 유입 증가보다는 효율 개선이 주요 원인으로 해석됩니다."
        )
    elif metric_name == "cvr":
        why = (
            "유입 대비 구매 연결력이 달라지면서 퍼널 효율이 직접적으로 변화한 상황입니다."
        )
    elif metric_name == "uv":
        why = (
            "채널 믹스 및 광고 집행 영향으로 유입 볼륨 구조가 조정된 결과로 보입니다."
        )
    elif metric_name == "aov":
        why = (
            "단가대 높은 SKU 노출 확대로 주문 평균 금액 구성이 변화했습니다."
        )
    else:
        why = (
            "기타 KPI 흐름과 함께 복합적으로 상호 작용하며 결과가 나타났습니다."
        )

    # RISK or OPPORTUNITY
    if delta_pct >= 5:
        risk = (
            "단기 급등 구간으로 판단되며, "
            "일시적 프로모션 효과 또는 일회성 수요 반영 가능성이 있습니다."
        )
    elif delta_pct <= -5:
        risk = (
            "감소폭이 커 구조적 저하 구간 가능성이 존재하며 "
            "채널·퍼널 상세 점검이 필요한 상황입니다."
        )
    else:
        risk = (
            "완만한 변동 구간으로, "
            "추세인지 노이즈인지는 추가 관찰이 필요합니다."
        )

    # FOCUS
    if metric_name == "revenue":
        focus = (
            "전환율 유지와 고단가 상품 노출 확장을 동시에 병행해 "
            "수익 구조 안정화에 집중해야 합니다."
        )
    elif metric_name == "cvr":
        focus = (
            "Checkout 단계 UX·혜택 체감 요소 개선 테스트를 "
            "단기 집중 과제로 설정합니다."
        )
    elif metric_name == "uv":
        focus = (
            "Paid/Organic 채널 유입 확대 실험을 통해 "
            "상단 퍼널 볼륨 복원을 시도합니다."
        )
    elif metric_name == "aov":
        focus = (
            "프리미엄 SKU 및 세트 프로모션 강화로 "
            "평균 객단가 상승 흐름을 유지합니다."
        )
    else:
        focus = (
            "관련 KPI 이동 원인 규명을 위한 세부 지표 분석을 병행합니다."
        )

    return {
        "title": title,
        "what": what,
        "why": why,
        "risk": risk,
        "focus": focus,
    }


def build_weekly_insight_blocks(kpi_pack):
    """
    KPI 주요 항목 4개 기준 인사이트 생성:
     - Revenue
     - UV
     - CVR
     - AOV
    """

    insight_blocks = []

    insight_blocks.append(
        build_4step_insight(
            "Revenue",
            "revenue",
            kpi_pack["revenue"]["today"],
            kpi_pack["revenue"]["lw"]
        )
    )

    insight_blocks.append(
        build_4step_insight(
            "UV",
            "uv",
            kpi_pack["uv"]["today"],
            kpi_pack["uv"]["lw"]
        )
    )

    insight_blocks.append(
        build_4step_insight(
            "CVR",
            "cvr",
            kpi_pack["cvr"]["today"],
            kpi_pack["cvr"]["lw"]
        )
    )

    insight_blocks.append(
        build_4step_insight(
            "AOV",
            "aov",
            kpi_pack["aov"]["today"],
            kpi_pack["aov"]["lw"]
        )
    )

    return insight_blocks


# ============================================================================
# 6) ACTION GENERATOR
# ============================================================================

def build_action_list(kpi_pack):
    """
    KPI 9개 조합 기반 실행 액션 6~8개 자동 생성
    """

    actions = []

    # Revenue
    if kpi_pack["revenue"]["today"] < kpi_pack["revenue"]["lw"]:
        actions.append(
            "매출 감소 구간으로, 주요 채널 예산 비중 재조정 및 "
            "메인 기획전 상단 노출 강화 테스트 필요"
        )
    else:
        actions.append(
            "성과 우수 SKU 중심으로 메인 슬롯 노출 확대 및 "
            "유사 SKU 세트화 구성 실험 진행"
        )

    # UV
    if kpi_pack["uv"]["today"] < kpi_pack["uv"]["lw"]:
        actions.append(
            "유입 하락 대응을 위해 Paid Search·Display 입찰 확대 및 "
            "콘텐츠 유입용 SEO Landing 보강"
        )
    else:
        actions.append(
            "효율 유지 채널 중심으로 타겟 확장 실험 진행"
        )

    # CVR
    if kpi_pack["cvr"]["today"] < kpi_pack["cvr"]["lw"]:
        actions.append(
            "Checkout 단계 UX 점검 → 배송비 노출 위치/혜택 카피 A/B 테스트 시행"
        )
    else:
        actions.append(
            "전환 상위 랜딩 페이지 포맷을 타 기획전에 적용 테스트"
        )

    # AOV
    if kpi_pack["aov"]["today"] < kpi_pack["aov"]["lw"]:
        actions.append(
            "고단가 SKU 묶음 구성 및 추천 모듈 상단 배치 실험"
        )
    else:
        actions.append(
            "프리미엄 SKU 중심 메인 비중 상향 유지"
        )

    # Organic Share
    if kpi_pack["organic_share"]["today"] < kpi_pack["organic_share"]["lw"]:
        actions.append(
            "콘텐츠형 기획전 및 리뷰 SEO 노출 강화 필요"
        )
    else:
        actions.append(
            "SEO 주요 키워드 랜딩 추가 제작"
        )

    # New Users
    if kpi_pack["new"]["today"] < kpi_pack["new"]["lw"]:
        actions.append(
            "신규 가입 유도형 쿠폰 UX 및 광고 소재 메시지 재점검"
        )
    else:
        actions.append(
            "신규 가입 이벤트 유지 및 확장 테스트"
        )

    # Padding to at least 6
    filler = [
        "주간 테스트 항목 성과 Tagging 결과 취합",
        "예산 효율 상위 캠페인 확장 편성",
        "상품/채널 단위 성과 리포트 자동 추적"
    ]

    i = 0
    while len(actions) < 8:
        actions.append(filler[i % len(filler)])
        i += 1

    return actions[:8]

# ============================================================================
# COLUMBIA KOREA – WEEKLY DIGEST (PART 3)
# HTML Render + Email Sender + Main Runner
# ============================================================================

# ============================================================================
# 7) HTML COMPONENTS
# ============================================================================

def render_kpi_card(title, unit, block, is_percent=False):
    """
    KPI 카드 1개 HTML
    """
    t = block["today"]

    def fmt(val):
        if is_percent:
            return f"{val:.2f}%" if isinstance(val, float) else f"{val}%"
        return f"{val:,}{unit}"

    def pct_span(v, base):
        p = pct(v, base)
        color = "#1b7f4d" if p >= 0 else "#c53030"
        bg = "#e7f5ec" if p >= 0 else "#fdeaea"
        sign = "+" if p >= 0 else ""
        return (
            f"<span style='background:{bg}; color:{color}; "
            f"border-radius:999px;padding:2px 7px;font-size:10px;'>"
            f"{sign}{p:.1f}%</span>"
        )

    row = f"""
    <div style="border:1px solid #e1e7f5;border-radius:16px;
                padding:14px 16px;height:150px;background:#fff;">
        <div style="font-size:11px;color:#777">{title}</div>
        <div style="font-size:20px;font-weight:700;margin:4px 0">
            {fmt(t)}
        </div>
        <div style="font-size:10px;color:#999;margin-bottom:4px">
            LD {fmt(block['ld'])} · LW {fmt(block['lw'])} · LY {fmt(block['ly'])}
        </div>
        <div>
            {pct_span(t, block['ld'])}
            {pct_span(t, block['lw'])}
            {pct_span(t, block['ly'])}
        </div>
    </div>
    """
    return row


def render_insight_block(block):
    """
    WHAT/WHY/RISK/FOCUS HTML
    """
    return f"""
    <div style="border:1px solid #e1e7f5;padding:12px 14px;
                border-radius:14px;background:#fff;margin-bottom:8px;">
        <b>{block['title']}</b><br>
        <b>WHAT</b> {block['what']}<br>
        <b>WHY</b> {block['why']}<br>
        <b>RISK</b> {block['risk']}<br>
        <b>FOCUS</b> {block['focus']}
    </div>
    """


# ============================================================================
# 8) MAIN HTML TEMPLATE
# ============================================================================

def compose_weekly_html(kpi_pack):

    # --- INSIGHTS ---
    insight_blocks = build_weekly_insight_blocks(kpi_pack)
    ins_html = "".join(render_insight_block(b) for b in insight_blocks)

    # --- ACTIONS ---
    actions = build_action_list(kpi_pack)
    act_html = "".join(f"<li style='margin-bottom:4px;'>{a}</li>" for a in actions)

    # --- KPI SNAPSHOT ---
    cards_html = "".join([
        render_kpi_card("매출", "원", kpi_pack["revenue"]),
        render_kpi_card("UV", "명", kpi_pack["uv"]),
        render_kpi_card("CVR", "", kpi_pack["cvr"], True),

        render_kpi_card("구매수", "건", kpi_pack["orders"]),
        render_kpi_card("AOV", "원", kpi_pack["aov"]),
        render_kpi_card("신규", "명", kpi_pack["new"]),

        render_kpi_card("오가닉 UV", "명", kpi_pack["organic_uv"]),
        render_kpi_card("비오가닉 UV", "명", kpi_pack["nonorganic_uv"]),
        render_kpi_card("오가닉 비중", "%", kpi_pack["organic_share"], True),
    ])

    body = f"""
<html>
<body style="background:#f5f7fb;font-family:Arial,sans-serif;">

<h2>COLUMBIA WEEKLY ECOM DIGEST</h2>
<div>{kpi_pack['range_label']}</div>

<hr>

<h3>01 · INSIGHTS</h3>
{ins_html}

<h4>EXECUTION ACTIONS</h4>
<ul style="font-size:12px;">
{act_html}
</ul>

<hr>

<h3>02 · WEEKLY KPI SNAPSHOT</h3>

<div style="display:grid;grid-template-columns:repeat(3,1fr);gap:10px;">
{cards_html}
</div>

<hr>

<h3>03 · FUNNEL · TRAFFIC · PRODUCT · SEARCH</h3>
<p style="font-size:13px;color:#666;">
해당 섹션은 향후 Sankey / Heatmap / Search Funnel 등으로 시각화 예정입니다.
</p>

<hr>

<h3>04 · VISUAL STORY (TEXT PREVIEW)</h3>
{ins_html}

<footer style="font-size:10px;color:#888;text-align:right;">
Columbia Sportswear Korea – Automated Weekly Digest
</footer>

</body>
</html>
"""

    return body


# ============================================================================
# 9) EMAIL SENDER
# ============================================================================

def send_weekly_mail(subject, html):

    if not SMTP_USER or not SMTP_PASS:
        print("[WARN] SMTP 설정 없음. 메일 발송 대신 미리보기 출력:")
        print(html[:2000])
        return

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = SMTP_USER
    msg["To"] = ", ".join(WEEKLY_RECIPIENTS)

    msg.attach(MIMEText(html, "html", "utf-8"))

    server = smtplib.SMTP(SMTP_HOST, SMTP_PORT)
    server.starttls()
    server.login(SMTP_USER, SMTP_PASS)
    server.sendmail(SMTP_USER, WEEKLY_RECIPIENTS, msg.as_string())
    server.quit()

    print("[INFO] Weekly digest mail sent.")


# ============================================================================
# 10) MAIN BLOCK
# ============================================================================

def run_weekly_digest():

    print("[START] WEEKLY DIGEST")

    kpi_pack = build_kpi_pack()

    html = compose_weekly_html(kpi_pack)

    subject = f"[COLUMBIA] Weekly Digest — {kpi_pack['range_label']}"

    send_weekly_mail(subject, html)

    print("[DONE] WEEKLY DIGEST")


if __name__ == "__main__":
    run_weekly_digest()

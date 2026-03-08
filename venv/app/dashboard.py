import streamlit as st
import requests
import pandas as pd
import plotly.express as px
import io
from datetime import timedelta
import os

st.set_page_config(
    page_title="MSME Business Insights",
    page_icon="📊",
    layout="wide",
)

st.markdown(
    """
    
    
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Fraunces:wght@600;700&family=IBM+Plex+Sans:wght@400;500;600&family=JetBrains+Mono:wght@500&display=swap');
    :root {
        --ink: #e2e8f0;
        --muted: #94a3b8;
        --accent: #38bdf8;
        --accent-2: #22c55e;
        --bg: #0b1220;
        --panel: #0f172a;
        --line: #1f2937;
    }
    html, body, [class*="css"]  {
        font-family: 'IBM Plex Sans', sans-serif;
        color: var(--ink);
    }
    .stApp {
        background:
            radial-gradient(1200px 600px at 10% -10%, #0b1b2a 0%, transparent 60%),
            radial-gradient(900px 500px at 90% 10%, #0c1f2f 0%, transparent 50%),
            var(--bg);
    }
    .hero {
        background: var(--panel);
        border: 1px solid var(--line);
        border-radius: 16px;
        padding: 18px 20px;
        box-shadow: 0 8px 26px rgba(2, 6, 23, 0.6);
        animation: rise 600ms ease-out;
    }
    .hero h1 {
        font-family: 'Fraunces', serif;
        font-weight: 700;
        font-size: 32px;
        margin: 0 0 6px 0;
    }
    .hero p {
        margin: 0;
        color: var(--muted);
        font-size: 15px;
    }
    .kpi {
        background: var(--panel);
        border: 1px solid var(--line);
        border-radius: 14px;
        padding: 14px 16px;
        box-shadow: 0 6px 18px rgba(2, 6, 23, 0.5);
        animation: rise 650ms ease-out;
    }
    .kpi-title {
        color: var(--muted);
        font-size: 12px;
        text-transform: uppercase;
        letter-spacing: 0.08em;
    }
    .kpi-value {
        font-size: 22px;
        font-weight: 600;
        margin-top: 6px;
        font-family: 'JetBrains Mono', monospace;
    }
    .chip {
        display: inline-block;
        background: rgba(56, 189, 248, 0.12);
        color: var(--accent);
        padding: 4px 10px;
        border-radius: 999px;
        font-size: 12px;
        border: 1px solid rgba(56, 189, 248, 0.35);
    }
    .section-title {
        font-family: 'Fraunces', serif;
        font-size: 22px;
        margin: 12px 0 8px 0;
    }
    .stTabs [data-baseweb="tab"] {
        font-weight: 600;
    }
    .stTabs [aria-selected="true"] {
        color: var(--accent);
    }
    @keyframes rise {
        from { transform: translateY(6px); opacity: 0; }
        to { transform: translateY(0); opacity: 1; }
    }
    </style>


    """,
    unsafe_allow_html=True,
)

st.markdown(
    """
    
    <div class="hero">
        <span class="chip">MSME Finance Desk</span>
        <h1>Business Performance Ledger</h1>
        <p>Track revenue flows, product contribution, and operational signals in one place.</p>
    </div>

    """,
    unsafe_allow_html=True,
)

API_BASE_URL = os.getenv("MSME_API_BASE_URL", "http://127.0.0.1:8000")
API_TOKEN = os.getenv("MSME_API_TOKEN")
DASH_PASSWORD = os.getenv("MSME_DASH_PASSWORD")

if DASH_PASSWORD:
    if "auth_ok" not in st.session_state:
        st.session_state.auth_ok = False

    if not st.session_state.auth_ok:
        st.subheader("Login")
        pwd = st.text_input("Password", type="password")
        if st.button("Login"):
            if pwd == DASH_PASSWORD:
                st.session_state.auth_ok = True
            else:
                st.error("Invalid password")
        st.stop()

with st.sidebar:
    st.subheader("Data Input")
    uploaded_file = st.file_uploader("Upload sales CSV", type=["csv"])
    st.caption(
        "Expected columns: product (text), revenue (number), date (YYYY-MM-DD). Optional: quantity (number)."
    )

REQUIRED_COLUMNS = {"product", "revenue", "date"}

sample_csv = (
    "product,revenue,quantity,date\n"
    "A,100,2,2026-03-01\n"
    "B,250,5,2026-03-02\n"
    "A,80,1,2026-03-03\n"
    "C,40,1,2026-03-03\n"
)
with st.sidebar:
    st.download_button(
        label="Download Sample CSV",
        data=sample_csv.encode("utf-8"),
        file_name="sample_sales.csv",
        mime="text/csv",
    )


@st.cache_data(show_spinner=False)
def post_analyze(filename, file_bytes, api_token, base_url):
    headers = {"X-API-Token": api_token} if api_token else None
    resp = requests.post(
        f"{base_url}/analyze",
        files={"file": (filename, file_bytes, "text/csv")},
        headers=headers,
        timeout=30,
    )
    return {
        "status": resp.status_code,
        "json": resp.json() if resp.status_code == 200 else None,
    }


@st.cache_data(show_spinner=False)
def post_export_cleaned(filename, file_bytes, api_token, base_url):
    headers = {"X-API-Token": api_token} if api_token else None
    resp = requests.post(
        f"{base_url}/export/cleaned",
        files={"file": (filename, file_bytes, "text/csv")},
        headers=headers,
        timeout=30,
    )
    return {"status": resp.status_code, "content": resp.content}


@st.cache_data(show_spinner=False)
def post_export_summary(filename, file_bytes, api_token, base_url):
    headers = {"X-API-Token": api_token} if api_token else None
    resp = requests.post(
        f"{base_url}/export/summary",
        files={"file": (filename, file_bytes, "text/csv")},
        headers=headers,
        timeout=30,
    )
    return {"status": resp.status_code, "content": resp.content}


@st.cache_data(show_spinner=False)
def get_history(limit, api_token, base_url):
    headers = {"X-API-Token": api_token} if api_token else None
    resp = requests.get(
        f"{base_url}/history?limit={limit}",
        headers=headers,
        timeout=30,
    )
    return {"status": resp.status_code, "json": resp.json() if resp.status_code == 200 else None}


@st.cache_data(show_spinner=False)
def post_ai_summary(payload, api_token, base_url):
    headers = {"X-API-Token": api_token} if api_token else None
    resp = requests.post(
        f"{base_url}/ai/summary",
        headers=headers,
        json=payload,
        timeout=30,
    )
    return {"status": resp.status_code, "json": resp.json() if resp.status_code == 200 else None}

if uploaded_file is not None:
    data = uploaded_file.getvalue()
    df = pd.read_csv(io.BytesIO(data))
    df.columns = df.columns.str.lower().str.strip()

    missing = REQUIRED_COLUMNS.difference(set(df.columns))
    if missing:
        st.error(f"Missing required columns: {', '.join(sorted(missing))}")
        st.stop()

    if "revenue" in df.columns:
        df["revenue"] = pd.to_numeric(df["revenue"], errors="coerce")
    if "quantity" in df.columns:
        df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce")

    df["date"] = pd.to_datetime(df["date"], errors="coerce")
    valid_dates = df["date"].dropna()
    if valid_dates.empty:
        st.error("No valid dates found in the date column.")
        st.stop()

    min_date = valid_dates.min().date()
    max_date = valid_dates.max().date()
    with st.sidebar:
        st.subheader("Filters")
        start_date, end_date = st.date_input(
            "Date range",
            value=(min_date, max_date),
            min_value=min_date,
            max_value=max_date,
        )

    filtered_df = df[
        (df["date"].dt.date >= start_date) & (df["date"].dt.date <= end_date)
    ].copy()

    if filtered_df.empty:
        st.warning("No data available for the selected date range.")
        st.stop()

    total_revenue = filtered_df["revenue"].sum()
    avg_revenue = filtered_df["revenue"].mean()
    total_quantity = filtered_df["quantity"].sum() if "quantity" in filtered_df.columns else None
    product_sales = filtered_df.groupby("product")["revenue"].sum().reset_index()
    top_product_share = None
    if not product_sales.empty and total_revenue and total_revenue > 0:
        top_product_share = product_sales["revenue"].max() / total_revenue

    tabs = st.tabs(["Overview", "Trends", "Insights", "History", "Downloads"])

    with tabs[0]:
        st.markdown('<div class="section-title">Key Metrics</div>', unsafe_allow_html=True)
        c1, c2, c3, c4 = st.columns(4)
        c1.markdown(
            f"<div class='kpi'><div class='kpi-title'>Total Revenue</div>"
            f"<div class='kpi-value'>{total_revenue:,.2f}</div></div>",
            unsafe_allow_html=True,
        )
        c2.markdown(
            f"<div class='kpi'><div class='kpi-title'>Avg Revenue</div>"
            f"<div class='kpi-value'>{avg_revenue:,.2f}</div></div>"
            if pd.notna(avg_revenue)
            else "<div class='kpi'><div class='kpi-title'>Avg Revenue</div><div class='kpi-value'>N/A</div></div>",
            unsafe_allow_html=True,
        )
        c3.markdown(
            f"<div class='kpi'><div class='kpi-title'>Total Items</div>"
            f"<div class='kpi-value'>{int(total_quantity)}</div></div>"
            if total_quantity is not None
            else "<div class='kpi'><div class='kpi-title'>Total Items</div><div class='kpi-value'>N/A</div></div>",
            unsafe_allow_html=True,
        )
        c4.markdown(
            f"<div class='kpi'><div class='kpi-title'>Top Product Share</div>"
            f"<div class='kpi-value'>{top_product_share:.1%}</div></div>"
            if top_product_share is not None
            else "<div class='kpi'><div class='kpi-title'>Top Product Share</div><div class='kpi-value'>N/A</div></div>",
            unsafe_allow_html=True,
        )

        st.markdown('<div class="section-title">Revenue by Product</div>', unsafe_allow_html=True)
        fig = px.bar(
            product_sales,
            x="product",
            y="revenue",
            title="",
        )
        st.plotly_chart(fig, width="stretch")

        with st.expander("Raw Data Preview"):
            st.dataframe(filtered_df)
        with st.expander("Schema Preview"):
            st.write(filtered_df.dtypes)

    with tabs[1]:
        st.markdown('<div class="section-title">Sales Trend</div>', unsafe_allow_html=True)
        sales_trend = (
            filtered_df.dropna(subset=["date"])
            .groupby("date")["revenue"]
            .sum()
            .reset_index()
        )
        fig2 = px.line(
            sales_trend,
            x="date",
            y="revenue",
            title="",
        )
        st.plotly_chart(fig2, width="stretch")

        st.markdown('<div class="section-title">Trend Change (Last 7 Days)</div>', unsafe_allow_html=True)
        max_dt = sales_trend["date"].max()
        if pd.notna(max_dt):
            last_7_start = max_dt - timedelta(days=6)
            prev_7_start = last_7_start - timedelta(days=7)
            prev_7_end = last_7_start - timedelta(days=1)

            last_7 = sales_trend[
                (sales_trend["date"] >= last_7_start) & (sales_trend["date"] <= max_dt)
            ]["revenue"].sum()
            prev_7 = sales_trend[
                (sales_trend["date"] >= prev_7_start) & (sales_trend["date"] <= prev_7_end)
            ]["revenue"].sum()

            if prev_7 > 0:
                change = (last_7 - prev_7) / prev_7
                st.write(f"Revenue change vs previous 7 days: {change:+.1%}")
            else:
                st.write("Not enough data to compute a 7-day comparison.")
        else:
            st.write("Not enough data to compute a 7-day comparison.")

    filtered_csv = filtered_df.to_csv(index=False).encode("utf-8")

    response = post_analyze(uploaded_file.name, filtered_csv, API_TOKEN, API_BASE_URL)

    if response["status"] == 200:
        api_data = response["json"]

        with tabs[2]:
            st.markdown('<div class="section-title">Insights</div>', unsafe_allow_html=True)
            for insight in api_data.get("insights", []):
                st.write("- ", insight)

            st.markdown('<div class="section-title">Recommendations</div>', unsafe_allow_html=True)
            for rec in api_data.get("recommendations", []):
                st.write("- ", rec)

            st.markdown('<div class="section-title">AI Summary</div>', unsafe_allow_html=True)
            ai_enabled = st.toggle("Generate AI Summary", value=False)
            if ai_enabled:
                ai_payload = {
                    "analysis": api_data.get("analysis", {}),
                    "insights": api_data.get("insights", []),
                    "recommendations": api_data.get("recommendations", []),
                }
                ai_resp = post_ai_summary(ai_payload, API_TOKEN, API_BASE_URL)
                if ai_resp["status"] == 200:
                    st.write(ai_resp["json"].get("summary", ""))
                else:
                    st.error("AI summary failed. Check API key and server logs.")

        with tabs[3]:
            history_resp = get_history(limit=20, api_token=API_TOKEN, base_url=API_BASE_URL)
            if history_resp["status"] == 200:
                history = history_resp["json"].get("history", [])
                st.markdown('<div class="section-title">Recent Analyses</div>', unsafe_allow_html=True)
                if history:
                    labels = [
                        f"{item['id']} | {item['created_at']} | rows: {item['rows']}"
                        for item in history
                    ]
                    selected_label = st.selectbox("Select a run", labels)
                    selected_index = labels.index(selected_label)
                    selected = history[selected_index]

                    st.write("Details")
                    st.json(
                        {
                            "id": selected["id"],
                            "created_at": selected["created_at"],
                            "rows": selected["rows"],
                            "columns": selected["columns"],
                            "date_min": selected["date_min"],
                            "date_max": selected["date_max"],
                            "analysis": selected["analysis"],
                            "insights": selected["insights"],
                            "recommendations": selected["recommendations"],
                            "raw_path": selected.get("raw_path"),
                            "cleaned_path": selected.get("cleaned_path"),
                        }
                    )

                    st.write("Compare Two Runs")
                    compare_labels = st.multiselect(
                        "Pick two runs to compare",
                        labels,
                        default=labels[:2] if len(labels) >= 2 else labels,
                    )
                    if len(compare_labels) == 2:
                        a = history[labels.index(compare_labels[0])]
                        b = history[labels.index(compare_labels[1])]

                        def get_metric(item, key):
                            return item.get("analysis", {}).get(key)

                        def delta(a_val, b_val):
                            if a_val is None or b_val is None:
                                return "N/A"
                            return f"{b_val - a_val:,.2f}"

                        st.dataframe(
                            [
                                {
                                    "metric": "total_revenue",
                                    "run_a": get_metric(a, "total_revenue"),
                                    "run_b": get_metric(b, "total_revenue"),
                                    "delta": delta(get_metric(a, "total_revenue"), get_metric(b, "total_revenue")),
                                },
                                {
                                    "metric": "average_revenue",
                                    "run_a": get_metric(a, "average_revenue"),
                                    "run_b": get_metric(b, "average_revenue"),
                                    "delta": delta(get_metric(a, "average_revenue"), get_metric(b, "average_revenue")),
                                },
                                {
                                    "metric": "total_quantity",
                                    "run_a": get_metric(a, "total_quantity"),
                                    "run_b": get_metric(b, "total_quantity"),
                                    "delta": delta(get_metric(a, "total_quantity"), get_metric(b, "total_quantity")),
                                },
                            ]
                        )
                    elif len(compare_labels) > 0:
                        st.info("Select exactly two runs to compare.")
                else:
                    st.write("No history yet.")

        with tabs[4]:
            st.markdown('<div class="section-title">Exports</div>', unsafe_allow_html=True)
            cleaned_resp = post_export_cleaned(
                uploaded_file.name, filtered_csv, API_TOKEN, API_BASE_URL
            )
            if cleaned_resp["status"] == 200:
                st.download_button(
                    label="Download Cleaned CSV",
                    data=cleaned_resp["content"],
                    file_name="cleaned_sales.csv",
                    mime="text/csv",
                )
            else:
                st.error("Failed to generate cleaned CSV")

            summary_resp = post_export_summary(
                uploaded_file.name, filtered_csv, API_TOKEN, API_BASE_URL
            )
            if summary_resp["status"] == 200:
                st.download_button(
                    label="Download Summary JSON",
                    data=summary_resp["content"],
                    file_name="insights_summary.json",
                    mime="application/json",
                )
            else:
                st.error("Failed to generate summary JSON")

    else:
        st.error("Analysis failed")

import streamlit as st
import pandas as pd
import requests
import pytz
from deltalake import DeltaTable, write_deltalake
from datetime import datetime

# ───────────────────────────────────────────
# CONFIG
# ───────────────────────────────────────────
TENANT_ID     = st.secrets["TENANT_ID"]
CLIENT_ID     = st.secrets["CLIENT_ID"]
CLIENT_SECRET = st.secrets["CLIENT_SECRET"]
WORKSPACE_ID  = "69a84913-d8e5-4ca9-970e-85e3ddc68f14"
LAKEHOUSE_ID  = "961d3727-8929-45b1-835d-95568f2ebe59"

ONELAKE_BASE = (
    f"abfss://{WORKSPACE_ID}@onelake.dfs.fabric.microsoft.com"
    f"/{LAKEHOUSE_ID}/Tables"
)

# ───────────────────────────────────────────
# STYLE — Warm Beige / Cream Minimal
# ───────────────────────────────────────────
def apply_style():
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=DM+Sans:wght@300;400;500;600&family=DM+Mono:wght@400;500&display=swap');

    html, body, [class*="css"] {
        font-family: 'DM Sans', sans-serif;
    }

    /* Background */
    .stApp {
        background-color: #f5f0eb;
        color: #3a2e28;
    }

    /* Hide default UI */
    #MainMenu, footer, header { visibility: hidden; }

    /* Title */
    h1 {
        font-family: 'DM Sans', sans-serif !important;
        font-weight: 600 !important;
        font-size: 1.5rem !important;
        color: #3a2e28 !important;
        letter-spacing: -0.02em !important;
        padding-bottom: 0.5rem !important;
        border-bottom: 1px solid #d0c8b6 !important;
        margin-bottom: 0.25rem !important;
    }

    /* Caption */
    .element-container p small {
        color: #a09080 !important;
        font-size: 0.78rem !important;
    }

    /* Subheader */
    h3 {
        font-family: 'DM Sans', sans-serif !important;
        font-weight: 500 !important;
        font-size: 0.75rem !important;
        color: #a09080 !important;
        text-transform: uppercase !important;
        letter-spacing: 0.12em !important;
        margin-bottom: 1.2rem !important;
    }

    /* Label */
    label {
        color: #7a6a5a !important;
        font-size: 0.75rem !important;
        font-weight: 500 !important;
        text-transform: uppercase !important;
        letter-spacing: 0.08em !important;
    }

    /* Selectbox */
    .stSelectbox > div > div {
        background-color: #ede7df !important;
        border: 1px solid #d0c8b6 !important;
        border-radius: 8px !important;
        color: #3a2e28 !important;
        font-family: 'DM Mono', monospace !important;
        font-size: 0.88rem !important;
    }
    .stSelectbox > div > div:hover {
        border-color: #c4a882 !important;
    }
    .stSelectbox > div > div:focus-within {
        border-color: #c4a882 !important;
        box-shadow: 0 0 0 2px rgba(196,168,130,0.2) !important;
    }
    /* dropdown list items */
    .stSelectbox [data-baseweb="select"] input {
        color: #3a2e28 !important;
    }
    [data-baseweb="popover"] {
        background-color: #ede7df !important;
    }
    [data-baseweb="menu"] {
        background-color: #ede7df !important;
    }
    [data-baseweb="menu"] li {
        color: #3a2e28 !important;
        font-family: 'DM Mono', monospace !important;
        font-size: 0.85rem !important;
    }
    [data-baseweb="menu"] li:hover {
        background-color: #e2d3cd !important;
    }

    /* Radio */
    .stRadio > div {
        gap: 0.5rem !important;
    }
    .stRadio > div > label {
        background-color: #ede7df !important;
        border: 1px solid #d0c8b6 !important;
        border-radius: 8px !important;
        padding: 0.4rem 1rem !important;
        color: #7a6a5a !important;
        font-size: 0.82rem !important;
        text-transform: none !important;
        letter-spacing: 0 !important;
        cursor: pointer !important;
        transition: all 0.15s !important;
    }
    .stRadio > div > label:has(input:checked) {
        background-color: #e6cdb5 !important;
        border-color: #c4a882 !important;
        color: #3a2e28 !important;
        font-weight: 500 !important;
    }

    /* Button Save */
    .stButton > button[kind="primary"] {
        background: linear-gradient(135deg, #c4a882, #b8956e) !important;
        color: #fff8f0 !important;
        border: none !important;
        border-radius: 8px !important;
        font-family: 'DM Sans', sans-serif !important;
        font-weight: 500 !important;
        font-size: 0.88rem !important;
        letter-spacing: 0.03em !important;
        padding: 0.5rem 1rem !important;
        transition: all 0.2s !important;
        box-shadow: 0 4px 12px rgba(180,140,100,0.25) !important;
    }
    .stButton > button[kind="primary"]:hover {
        background: linear-gradient(135deg, #d4b892, #c4a882) !important;
        box-shadow: 0 6px 16px rgba(180,140,100,0.35) !important;
        transform: translateY(-1px) !important;
    }

    /* Button Refresh */
    .stButton > button[kind="secondary"] {
        background-color: transparent !important;
        color: #a09080 !important;
        border: 1px solid #d0c8b6 !important;
        border-radius: 8px !important;
        font-size: 0.82rem !important;
        transition: all 0.15s !important;
    }
    .stButton > button[kind="secondary"]:hover {
        border-color: #c4a882 !important;
        color: #3a2e28 !important;
    }

    /* Dataframe */
    .stDataFrame {
        border: 1px solid #d0c8b6 !important;
        border-radius: 10px !important;
        overflow: hidden !important;
    }
    iframe[data-testid="stDataFrameResizable"] {
        background-color: #f5f0eb !important;
    }

    /* Warning */
    .stAlert {
        background-color: #ede7df !important;
        border: 1px solid #d0c8b6 !important;
        border-radius: 8px !important;
        color: #7a6a5a !important;
    }

    /* Spinner */
    .stSpinner > div {
        border-top-color: #c4a882 !important;
    }

    /* Scrollbar */
    ::-webkit-scrollbar { width: 5px; height: 5px; }
    ::-webkit-scrollbar-track { background: #f5f0eb; }
    ::-webkit-scrollbar-thumb { background: #d0c8b6; border-radius: 10px; }
    </style>
    """, unsafe_allow_html=True)

# ───────────────────────────────────────────
# AUTH
# ───────────────────────────────────────────
@st.cache_data(ttl=3000)
def get_token():
    url = f"https://login.microsoftonline.com/{TENANT_ID}/oauth2/v2.0/token"
    res = requests.post(url, data={
        "grant_type":    "client_credentials",
        "client_id":     CLIENT_ID,
        "client_secret": CLIENT_SECRET,
        "scope":         "https://storage.azure.com/.default",
    })
    if not res.ok:
        st.error(f"Token error {res.status_code}: {res.text}")
        st.stop()
    return res.json()["access_token"]

def storage_options():
    return {
        "bearer_token":        get_token(),
        "use_fabric_endpoint": "true",
    }

# ───────────────────────────────────────────
# DATA
# ───────────────────────────────────────────
@st.cache_data(ttl=600)
def load_all_docs():
    opts = storage_options()
    df = (
        DeltaTable(f"{ONELAKE_BASE}/gold_contract_management", storage_options=opts)
        .to_pandas()[["purchasing_doc_no"]]
        .drop_duplicates()
        .sort_values("purchasing_doc_no")
    )
    return df

@st.cache_data(ttl=600)
def load_saved():
    opts = storage_options()
    try:
        df = (
            DeltaTable(f"{ONELAKE_BASE}/gold_manual_contract_status", storage_options=opts)
            .to_pandas()
            .sort_values("updated_timestamp", ascending=False)
        )
        df["updated_timestamp"] = (
            pd.to_datetime(df["updated_timestamp"], utc=True)
            .dt.tz_convert("Asia/Bangkok")
            .dt.tz_localize(None)
        )
        return df
    except Exception:
        return pd.DataFrame(columns=["purchasing_doc_no", "user_status", "purchaser_status", "updated_timestamp"])

def save_status(doc_no: str, user_status: str, purchaser_status: str):
    opts = storage_options()
    new_row = pd.DataFrame([{
        "purchasing_doc_no": doc_no,
        "user_status":       user_status,
        "purchaser_status":  purchaser_status,
        "updated_timestamp": datetime.now(pytz.timezone("Asia/Bangkok")),
    }])
    try:
        existing = DeltaTable(
            f"{ONELAKE_BASE}/gold_manual_contract_status",
            storage_options=opts
        ).to_pandas()
        existing = existing[existing["purchasing_doc_no"] != doc_no]
        merged = pd.concat([existing, new_row], ignore_index=True)
        write_deltalake(
            f"{ONELAKE_BASE}/gold_manual_contract_status",
            merged,
            mode="overwrite",
            storage_options=opts,
        )
    except Exception:
        write_deltalake(
            f"{ONELAKE_BASE}/gold_manual_contract_status",
            new_row,
            mode="append",
            storage_options=opts,
        )

# ───────────────────────────────────────────
# SESSION STATE
# ───────────────────────────────────────────
if "saved_data" not in st.session_state:
    st.session_state.saved_data = None
if st.session_state.saved_data is None:
    st.session_state.saved_data = load_saved()

# ───────────────────────────────────────────
# UI
# ───────────────────────────────────────────
st.set_page_config(page_title="Contract Status", page_icon="📋", layout="wide")
apply_style()

st.title("Contract Status Management")
st.caption("กรอก User Status และ Purchaser Status สำหรับแต่ละ Purchasing Doc")

st.markdown("<br>", unsafe_allow_html=True)

col_form, col_gap, col_table = st.columns([1, 0.08, 2])

with col_form:
    st.subheader("เพิ่ม / แก้ไข Status")

    df_docs = load_all_docs()

    doc_no = st.selectbox(
        "Purchasing Doc No",
        df_docs["purchasing_doc_no"].tolist(),
        placeholder="ค้นหา Doc No...",
    )

    st.markdown("<div style='height:0.5rem'></div>", unsafe_allow_html=True)

    user_status = st.radio(
        "User Status",
        ["confirmed", ""],
        format_func=lambda x: "✅  confirmed" if x == "confirmed" else "⬜  (ว่าง)",
        horizontal=True,
    )

    st.markdown("<div style='height:0.5rem'></div>", unsafe_allow_html=True)

    purchaser_status = st.radio(
        "Purchaser Status",
        ["confirmed", ""],
        format_func=lambda x: "✅  confirmed" if x == "confirmed" else "⬜  (ว่าง)",
        horizontal=True,
    )

    st.markdown("<div style='height:1rem'></div>", unsafe_allow_html=True)

    if st.button("Save", type="primary", use_container_width=True):
        with st.spinner("กำลังบันทึก..."):
            try:
                save_status(doc_no, user_status, purchaser_status)
                new_entry = pd.DataFrame([{
                    "purchasing_doc_no": doc_no,
                    "user_status":       user_status,
                    "purchaser_status":  purchaser_status,
                    "updated_timestamp": datetime.now(pytz.timezone("Asia/Bangkok")).replace(tzinfo=None),
                }])
                df = st.session_state.saved_data
                df = df[df["purchasing_doc_no"] != doc_no]
                df = pd.concat([new_entry, df], ignore_index=True)
                st.session_state.saved_data = df
                st.toast(f"Saved — {doc_no}", icon="✅")
                st.rerun()
            except Exception as e:
                st.error(f"Error: {e}")

with col_table:
    st.subheader("รายการที่บันทึกแล้ว")

    df_saved = st.session_state.saved_data

    if df_saved.empty:
        st.warning("ยังไม่มีข้อมูล")
    else:
        st.dataframe(
            df_saved,
            use_container_width=True,
            hide_index=True,
            column_config={
                "purchasing_doc_no":  st.column_config.TextColumn("Doc No"),
                "user_status":        st.column_config.TextColumn("User Status"),
                "purchaser_status":   st.column_config.TextColumn("Purchaser Status"),
                "updated_timestamp":  st.column_config.DatetimeColumn("Updated At", format="YYYY-MM-DD HH:mm:ss"),
            },
        )
        st.caption(f"{len(df_saved)} รายการ")

    st.markdown("<div style='height:0.5rem'></div>", unsafe_allow_html=True)

    if st.button("↺  Refresh", use_container_width=True):
        load_all_docs.clear()
        st.session_state.saved_data = None
        st.rerun()

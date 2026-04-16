import streamlit as st
import pandas as pd
import requests
import pytz
import threading
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

PURCHASER_STATUS_OPTIONS = [
    "Not Start",
    "RFQ - Request for Quotation",
    "Compared",
    "RL - Recommend Letter",
    "OA Created",
    "Cancel OA",
]

# ───────────────────────────────────────────
# STYLE — Clean White / Modern Tech
# ───────────────────────────────────────────
def apply_style():
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=DM+Mono:wght@400;500&display=swap');

    html, body, [class*="css"] {
        font-family: 'Inter', sans-serif;
    }

    /* Background */
    .stApp {
        background-color: #ffffff;
        color: #0f0f0f;
    }

    /* Hide default UI */
    #MainMenu, footer, header { visibility: hidden; }

    /* Title */
    h1 {
        font-family: 'Inter', sans-serif !important;
        font-weight: 700 !important;
        font-size: 1.5rem !important;
        color: #0f0f0f !important;
        letter-spacing: -0.03em !important;
        margin-bottom: 0.1rem !important;
    }

    /* Caption */
    .element-container p small,
    [data-testid="stCaptionContainer"] p {
        color: #999 !important;
        font-size: 0.78rem !important;
    }

    /* Subheader / section label */
    h3 {
        font-family: 'Inter', sans-serif !important;
        font-weight: 600 !important;
        font-size: 0.68rem !important;
        color: #aaa !important;
        text-transform: uppercase !important;
        letter-spacing: 0.12em !important;
        margin-bottom: 1rem !important;
    }

    /* Label */
    label, [data-testid="stWidgetLabel"] p {
        color: #888 !important;
        font-size: 0.88rem !important;
        font-weight: 600 !important;
        text-transform: uppercase !important;
        letter-spacing: 0.1em !important;
    }

    /* Selectbox */
    .stSelectbox > div > div {
        background-color: #f7f7f8 !important;
        border: 1.5px solid #e8e8e8 !important;
        border-radius: 10px !important;
        color: #0f0f0f !important;
        font-family: 'Inter', sans-serif !important;
        font-size: 0.88rem !important;
    }
    .stSelectbox > div > div:hover {
        border-color: #c8c8c8 !important;
    }
    .stSelectbox > div > div:focus-within {
        border-color: #0f0f0f !important;
        box-shadow: 0 0 0 2px rgba(15,15,15,0.08) !important;
    }
    .stSelectbox [data-baseweb="select"] input {
        color: #0f0f0f !important;
    }
    [data-baseweb="popover"] {
        background-color: #ffffff !important;
        border: 1px solid #e8e8e8 !important;
        border-radius: 10px !important;
        box-shadow: 0 8px 24px rgba(0,0,0,0.08) !important;
    }
    [data-baseweb="menu"] {
        background-color: #ffffff !important;
    }
    [data-baseweb="menu"] li {
        color: #0f0f0f !important;
        font-family: 'Inter', sans-serif !important;
        font-size: 0.85rem !important;
    }
    [data-baseweb="menu"] li:hover {
        background-color: #f7f7f8 !important;
    }

    /* Text area */
    .stTextArea textarea {
        background-color: #f7f7f8 !important;
        border: 1.5px solid #e8e8e8 !important;
        border-radius: 10px !important;
        color: #0f0f0f !important;
        font-family: 'Inter', sans-serif !important;
        font-size: 0.88rem !important;
    }
    .stTextArea textarea:focus {
        border-color: #0f0f0f !important;
        box-shadow: 0 0 0 2px rgba(15,15,15,0.08) !important;
    }
    .stTextArea textarea::placeholder {
        color: #bbb !important;
    }

    /* Text input */
    .stTextInput input {
        background-color: #f7f7f8 !important;
        border: 1.5px solid #e8e8e8 !important;
        border-radius: 10px !important;
        color: #0f0f0f !important;
        font-family: 'DM Mono', monospace !important;
        font-size: 0.88rem !important;
    }
    .stTextInput input:focus {
        border-color: #0f0f0f !important;
        box-shadow: 0 0 0 2px rgba(15,15,15,0.08) !important;
    }
    .stTextInput input::placeholder {
        color: #bbb !important;
    }

    /* Button Save (primary) */
    .stButton > button[kind="primary"] {
        background-color: #0f0f0f !important;
        color: #ffffff !important;
        border: none !important;
        border-radius: 10px !important;
        font-family: 'Inter', sans-serif !important;
        font-weight: 600 !important;
        font-size: 0.88rem !important;
        letter-spacing: 0.01em !important;
        padding: 0.55rem 1rem !important;
        transition: all 0.2s !important;
    }
    .stButton > button[kind="primary"]:hover {
        background-color: #333 !important;
        transform: translateY(-1px) !important;
        box-shadow: 0 4px 12px rgba(0,0,0,0.15) !important;
    }

    /* Button Refresh (secondary) */
    .stButton > button[kind="secondary"] {
        background-color: #ffffff !important;
        color: #666 !important;
        border: 1.5px solid #e8e8e8 !important;
        border-radius: 10px !important;
        font-family: 'Inter', sans-serif !important;
        font-size: 0.85rem !important;
        transition: all 0.15s !important;
    }
    .stButton > button[kind="secondary"]:hover {
        border-color: #0f0f0f !important;
        color: #0f0f0f !important;
    }

    /* Dataframe */
    .stDataFrame {
        border: 1.5px solid #e8e8e8 !important;
        border-radius: 12px !important;
        overflow: hidden !important;
    }

    /* Alert */
    .stAlert {
        background-color: #f7f7f8 !important;
        border: 1px solid #e8e8e8 !important;
        border-radius: 10px !important;
        color: #666 !important;
    }

    /* Spinner */
    .stSpinner > div {
        border-top-color: #0f0f0f !important;
    }

    /* Scrollbar */
    ::-webkit-scrollbar { width: 5px; height: 5px; }
    ::-webkit-scrollbar-track { background: #ffffff; }
    ::-webkit-scrollbar-thumb { background: #e0e0e0; border-radius: 10px; }
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
        .to_pandas()
        .drop_duplicates(subset=["purchasing_doc_no"])
        .sort_values("purchasing_doc_no")
    )
    return df

@st.cache_data(ttl=600)
def load_saved():
    opts = storage_options()
    try:
        df = DeltaTable(f"{ONELAKE_BASE}/gold_manual_contract_status", storage_options=opts).to_pandas()
        # Handle column name migration: updated_timestamp → update_at
        if "updated_timestamp" in df.columns and "update_at" not in df.columns:
            df = df.rename(columns={"updated_timestamp": "update_at"})
        if "update_at" in df.columns:
            df["update_at"] = (
                pd.to_datetime(df["update_at"], utc=True)
                .dt.tz_convert("Asia/Bangkok")
                .dt.tz_localize(None)
            )
            df = df.sort_values("update_at", ascending=False)
        # Ensure all expected columns exist
        for col in ["comment", "new_purchasing_doc_no"]:
            if col not in df.columns:
                df[col] = ""
        return df
    except Exception:
        return pd.DataFrame(columns=[
            "purchasing_doc_no", "user_status", "purchaser_status",
            "comment", "new_purchasing_doc_no", "update_at",
        ])

def _save_to_delta(new_row: pd.DataFrame, opts: dict):
    """Write to Delta Lake — runs in background thread."""
    table_path = f"{ONELAKE_BASE}/gold_manual_contract_status"
    try:
        (
            DeltaTable(table_path, storage_options=opts)
            .merge(
                source=new_row,
                predicate="s.purchasing_doc_no = t.purchasing_doc_no",
                source_alias="s",
                target_alias="t",
            )
            .when_matched_update({
                "purchaser_status":      "s.purchaser_status",
                "comment":               "s.comment",
                "new_purchasing_doc_no": "s.new_purchasing_doc_no",
                "update_at":             "s.update_at",
                # user_status is intentionally excluded — managed by email alert
            })
            .when_not_matched_insert_all()
            .execute()
        )
    except Exception:
        write_deltalake(table_path, new_row, mode="append", storage_options=opts)

def save_status_async(doc_no: str, purchaser_status: str, comment: str, new_doc_no: str):
    """Build row, fetch token in main thread, then fire background write."""
    opts = storage_options()  # token must be fetched in main thread
    new_row = pd.DataFrame([{
        "purchasing_doc_no":     doc_no,
        "user_status":           "",
        "purchaser_status":      purchaser_status,
        "comment":               comment,
        "new_purchasing_doc_no": new_doc_no,
        "update_at":             datetime.now(pytz.timezone("Asia/Bangkok")),
    }])
    threading.Thread(target=_save_to_delta, args=(new_row, opts), daemon=True).start()
    return new_row

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

st.title("Contract Status")
st.caption("กรอก Purchaser Status สำหรับแต่ละ Purchasing Doc")

st.markdown("<br>", unsafe_allow_html=True)

col_form, col_gap, col_table = st.columns([1, 0.08, 2.2])

# ── LEFT: FORM ──
with col_form:
    st.subheader("เพิ่ม / แก้ไข")

    df_docs = load_all_docs()

    doc_no = st.selectbox(
        "Purchasing Doc No",
        df_docs["purchasing_doc_no"].tolist(),
        placeholder="ค้นหา Doc No...",
    )

    # Pre-fill form with previously saved values for the selected doc
    existing = st.session_state.saved_data
    existing_row = existing[existing["purchasing_doc_no"] == doc_no] if doc_no else pd.DataFrame()
    if not existing_row.empty:
        saved_status  = existing_row.iloc[0].get("purchaser_status", PURCHASER_STATUS_OPTIONS[0])
        saved_comment = existing_row.iloc[0].get("comment", "")
        saved_new_doc = existing_row.iloc[0].get("new_purchasing_doc_no", "")
        status_index  = PURCHASER_STATUS_OPTIONS.index(saved_status) if saved_status in PURCHASER_STATUS_OPTIONS else 0
    else:
        saved_status, saved_comment, saved_new_doc, status_index = PURCHASER_STATUS_OPTIONS[0], "", "", 0

    st.markdown("<div style='height:0.4rem'></div>", unsafe_allow_html=True)

    purchaser_status = st.selectbox(
        "Purchaser Status",
        PURCHASER_STATUS_OPTIONS,
        index=status_index,
    )

    st.markdown("<div style='height:0.4rem'></div>", unsafe_allow_html=True)

    comment = st.text_area(
        "Comment",
        value=saved_comment or "",
        placeholder="หมายเหตุ / รายละเอียดเพิ่มเติม ...",
        height=100,
    )

    st.markdown("<div style='height:0.4rem'></div>", unsafe_allow_html=True)

    new_doc_no = st.text_input(
        "เลขสัญญาใหม่ (Purchasing Doc No ใหม่)",
        value=saved_new_doc or "",
        placeholder="ตัวเลขเท่านั้น เช่น 4500012345",
    )

    st.markdown("<div style='height:0.8rem'></div>", unsafe_allow_html=True)

    if st.button("Save", type="primary", use_container_width=True):
        if new_doc_no and not new_doc_no.isdigit():
            st.error("เลขสัญญาใหม่ต้องเป็นตัวเลขเท่านั้น")
        else:
            try:
                new_entry = save_status_async(doc_no, purchaser_status, comment, new_doc_no)
                new_entry["update_at"] = new_entry["update_at"].dt.tz_localize(None)
                df = st.session_state.saved_data
                # Preserve existing user_status from email alert
                existing_row = df[df["purchasing_doc_no"] == doc_no]
                if not existing_row.empty and "user_status" in existing_row.columns:
                    new_entry["user_status"] = existing_row.iloc[0]["user_status"]
                df = df[df["purchasing_doc_no"] != doc_no]
                df = pd.concat([new_entry, df], ignore_index=True)
                st.session_state.saved_data = df
                st.toast(f"Saved — {doc_no}", icon="✅")
                st.rerun()
            except Exception as e:
                st.error(f"Error: {e}")

# ── RIGHT: TABLE ──
with col_table:
    df_saved = st.session_state.saved_data
    count = len(df_saved)

    # Header row with badge
    st.markdown(
        f"""
        <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:1rem;">
            <span style="font-size:0.88rem; color:#aaa; text-transform:uppercase; letter-spacing:0.12em; font-weight:600; font-family:'Inter',sans-serif;">
                รายการที่บันทึกแล้ว
            </span>
            <span style="background:#0f0f0f; color:#fff; font-size:0.72rem; font-weight:600;
                         padding:0.2rem 0.75rem; border-radius:20px; font-family:'Inter',sans-serif;">
                {count} รายการ
            </span>
        </div>
        """,
        unsafe_allow_html=True,
    )

    if df_saved.empty:
        st.warning("ยังไม่มีข้อมูล")
    else:
        # Join with source to get contract name column (if available)
        docs_df = load_all_docs()
        name_col = next(
            (c for c in docs_df.columns
             if c != "purchasing_doc_no"
             and any(k in c.lower() for k in ["name", "contract", "desc", "short", "text"])),
            None,
        )

        display_df = df_saved.copy()
        if name_col:
            display_df = display_df.merge(
                docs_df[["purchasing_doc_no", name_col]],
                on="purchasing_doc_no",
                how="left",
            )

        ordered_cols = ["purchasing_doc_no"]
        if name_col:
            ordered_cols.append(name_col)
        ordered_cols += ["user_status", "purchaser_status", "comment", "new_purchasing_doc_no", "update_at"]
        display_df = display_df[[c for c in ordered_cols if c in display_df.columns]]

        col_cfg = {
            "purchasing_doc_no":     st.column_config.TextColumn("Doc No"),
            "user_status":           st.column_config.TextColumn("User Status"),
            "purchaser_status":      st.column_config.TextColumn("Purchaser Status"),
            "comment":               st.column_config.TextColumn("Comment"),
            "new_purchasing_doc_no": st.column_config.TextColumn("เลขสัญญาใหม่"),
            "update_at":             st.column_config.DatetimeColumn("Updated At", format="YYYY-MM-DD HH:mm"),
        }
        if name_col:
            col_cfg[name_col] = st.column_config.TextColumn("Contract Name")

        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True,
            column_config=col_cfg,
        )

    st.markdown("<div style='height:0.5rem'></div>", unsafe_allow_html=True)

    if st.button("↺  Refresh data", use_container_width=True):
        load_all_docs.clear()
        load_saved.clear()
        st.session_state.saved_data = None
        st.rerun()

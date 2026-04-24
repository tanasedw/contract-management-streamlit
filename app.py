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
    "Completed",
]

# ───────────────────────────────────────────
# STYLE — Clean White / Modern Tech
# ───────────────────────────────────────────
def apply_style():
    st.markdown("""
    <style>
    @import url('https://fonts.googleapis.com/css2?family=Sora:wght@300;400;500;600;700&display=swap');

    /* Aurora animated background */
    .stApp {
        font-family: 'Sora', sans-serif !important;
        color: #ffffff !important;
        background-color: #0f0f1a !important;
    }
    .stApp::before {
        content: '';
        position: fixed;
        inset: -50%;
        z-index: 0;
        background:
            radial-gradient(ellipse at 20% 30%, rgba(6,182,212,0.35) 0%, transparent 50%),
            radial-gradient(ellipse at 80% 20%, rgba(168,85,247,0.35) 0%, transparent 50%),
            radial-gradient(ellipse at 40% 70%, rgba(236,72,153,0.3) 0%, transparent 50%),
            radial-gradient(ellipse at 70% 80%, rgba(59,130,246,0.25) 0%, transparent 50%);
        animation: aurora 15s ease-in-out infinite alternate;
        pointer-events: none;
    }
    @keyframes aurora {
        0%   { transform: translate(0,0) rotate(0deg) scale(1); }
        33%  { transform: translate(5%,-5%) rotate(3deg) scale(1.05); }
        66%  { transform: translate(-3%,3%) rotate(-2deg) scale(0.98); }
        100% { transform: translate(2%,-2%) rotate(1deg) scale(1.02); }
    }

    html, body, [class*="css"] {
        font-family: 'Sora', sans-serif !important;
        color: #ffffff !important;
    }

    /* Hide default UI */
    #MainMenu, footer, header { visibility: hidden; }

    /* Title */
    h1 {
        font-family: 'Sora', sans-serif !important;
        font-weight: 700 !important;
        font-size: 1.5rem !important;
        background: linear-gradient(135deg, #ffffff, #06b6d4, #a855f7) !important;
        -webkit-background-clip: text !important;
        -webkit-text-fill-color: transparent !important;
        background-clip: text !important;
        letter-spacing: -0.03em !important;
        margin-bottom: 0.1rem !important;
    }

    /* Caption */
    .element-container p small,
    [data-testid="stCaptionContainer"] p {
        color: rgba(255,255,255,0.5) !important;
        font-size: 0.78rem !important;
    }

    /* Subheader / section label */
    h3 {
        font-family: 'Sora', sans-serif !important;
        font-weight: 600 !important;
        font-size: 0.68rem !important;
        color: rgba(255,255,255,0.5) !important;
        text-transform: uppercase !important;
        letter-spacing: 0.12em !important;
        margin-bottom: 1rem !important;
    }

    /* Label */
    label, [data-testid="stWidgetLabel"] p {
        color: rgba(255,255,255,0.6) !important;
        font-size: 0.88rem !important;
        font-weight: 600 !important;
        text-transform: uppercase !important;
        letter-spacing: 0.1em !important;
    }

    /* Selectbox */
    .stSelectbox > div > div {
        background-color: rgba(255,255,255,0.05) !important;
        border: 1px solid rgba(255,255,255,0.15) !important;
        border-radius: 12px !important;
        color: #ffffff !important;
        font-family: 'Sora', sans-serif !important;
        font-size: 0.88rem !important;
        backdrop-filter: blur(10px) !important;
    }
    .stSelectbox > div > div:focus-within {
        border-color: #06b6d4 !important;
        box-shadow: 0 0 0 2px rgba(6,182,212,0.3) !important;
    }
    .stSelectbox [data-baseweb="select"] input { color: #ffffff !important; }
    [data-baseweb="popover"] {
        background-color: rgba(15,15,26,0.95) !important;
        border: 1px solid rgba(255,255,255,0.15) !important;
        border-radius: 12px !important;
        backdrop-filter: blur(20px) !important;
        box-shadow: 0 8px 32px rgba(0,0,0,0.4) !important;
    }
    [data-baseweb="menu"] { background-color: transparent !important; }
    [data-baseweb="menu"] li {
        color: rgba(255,255,255,0.8) !important;
        font-family: 'Sora', sans-serif !important;
        font-size: 0.85rem !important;
    }
    [data-baseweb="menu"] li:hover {
        background-color: rgba(6,182,212,0.15) !important;
        color: #ffffff !important;
    }

    /* Text area */
    .stTextArea textarea {
        background-color: rgba(255,255,255,0.05) !important;
        border: 1px solid rgba(255,255,255,0.15) !important;
        border-radius: 12px !important;
        color: #ffffff !important;
        font-family: 'Sora', sans-serif !important;
        font-size: 0.88rem !important;
        backdrop-filter: blur(10px) !important;
    }
    .stTextArea textarea:focus {
        border-color: #06b6d4 !important;
        box-shadow: 0 0 0 2px rgba(6,182,212,0.3) !important;
    }
    .stTextArea textarea::placeholder { color: rgba(255,255,255,0.3) !important; }

    /* Text input */
    .stTextInput input {
        background-color: rgba(255,255,255,0.05) !important;
        border: 1px solid rgba(255,255,255,0.15) !important;
        border-radius: 12px !important;
        color: #ffffff !important;
        font-family: 'Sora', sans-serif !important;
        font-size: 0.88rem !important;
        backdrop-filter: blur(10px) !important;
    }
    .stTextInput input:focus {
        border-color: #06b6d4 !important;
        box-shadow: 0 0 0 2px rgba(6,182,212,0.3) !important;
    }
    .stTextInput input::placeholder { color: rgba(255,255,255,0.3) !important; }

    /* Button Save (primary) */
    .stButton > button[kind="primary"] {
        background: linear-gradient(135deg, #06b6d4, #a855f7) !important;
        color: #ffffff !important;
        border: none !important;
        border-radius: 12px !important;
        font-family: 'Sora', sans-serif !important;
        font-weight: 600 !important;
        font-size: 0.88rem !important;
        transition: all 0.3s ease !important;
    }
    .stButton > button[kind="primary"]:hover {
        transform: translateY(-1px) !important;
        box-shadow: 0 0 24px rgba(6,182,212,0.5), 0 0 48px rgba(168,85,247,0.3) !important;
    }

    /* Button Refresh (secondary) */
    .stButton > button[kind="secondary"] {
        background-color: rgba(255,255,255,0.05) !important;
        color: rgba(255,255,255,0.8) !important;
        border: 1px solid rgba(255,255,255,0.2) !important;
        border-radius: 12px !important;
        font-family: 'Sora', sans-serif !important;
        font-size: 0.85rem !important;
        backdrop-filter: blur(10px) !important;
        transition: all 0.2s ease !important;
    }
    .stButton > button[kind="secondary"]:hover {
        background-color: rgba(255,255,255,0.1) !important;
        color: #ffffff !important;
    }

    /* Alert */
    .stAlert {
        background-color: rgba(255,255,255,0.05) !important;
        border: 1px solid rgba(255,255,255,0.1) !important;
        border-radius: 12px !important;
        color: rgba(255,255,255,0.8) !important;
    }

    /* Spinner */
    .stSpinner > div { border-top-color: #06b6d4 !important; }

    /* Scrollbar */
    ::-webkit-scrollbar { width: 5px; height: 5px; }
    ::-webkit-scrollbar-track { background: transparent; }
    ::-webkit-scrollbar-thumb { background: rgba(255,255,255,0.2); border-radius: 10px; }
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
@st.cache_data(ttl=600, show_spinner=False)
def load_all_docs():
    opts = storage_options()
    df = (
        DeltaTable(f"{ONELAKE_BASE}/gold_contract_management", storage_options=opts)
        .to_pandas()
        .drop_duplicates(subset=["purchasing_doc_no"])
        .sort_values("purchasing_doc_no")
    )
    return df

@st.cache_data(ttl=600, show_spinner=False)
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
        # Dedup — keep latest per doc (guards against background-merge window)
        df = df.drop_duplicates(subset=["purchasing_doc_no"], keep="first")
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

def _overwrite_to_delta(df: pd.DataFrame, opts: dict):
    """Overwrite full table in background — no duplicates, no read needed."""
    table_path = f"{ONELAKE_BASE}/gold_manual_contract_status"
    try:
        write_deltalake(table_path, df, mode="overwrite",
                        schema_mode="overwrite", storage_options=opts)
    except Exception:
        pass

# ───────────────────────────────────────────
# UI CONFIG (must be first Streamlit call)
# ───────────────────────────────────────────
st.set_page_config(page_title="Contract Status", page_icon="📋", layout="wide")
apply_style()

# ───────────────────────────────────────────
# SESSION STATE + CUSTOM LOADING SCREEN
# ───────────────────────────────────────────
if "saved_data" not in st.session_state:
    st.session_state.saved_data = None
if st.session_state.saved_data is None:
    _loading = st.empty()
    _loading.markdown(
        """
        <style>
        @keyframes flower-spin {
            from { transform: rotate(0deg); }
            to   { transform: rotate(360deg); }
        }
        .fl-overlay {
            position: fixed; inset: 0;
            background: #0f0f1a;
            z-index: 9999;
            display: flex;
            flex-direction: column;
            align-items: center;
            justify-content: center;
        }
        .fl-flower {
            font-size: 4rem;
            display: inline-block;
            animation: flower-spin 1.2s linear infinite;
        }
        .fl-text {
            color: #06b6d4;
            font-size: 1rem;
            margin-top: 1.4rem;
            font-family: 'Inter', sans-serif;
            font-weight: 600;
            letter-spacing: 0.1em;
            text-transform: uppercase;
        }
        </style>
        <div class="fl-overlay">
            <span class="fl-flower">🌸</span>
            <div class="fl-text">Wait a moment...</div>
        </div>
        """,
        unsafe_allow_html=True,
    )
    # Run both fetches in parallel to halve load time
    results = {}
    def _fetch_saved():
        results["saved"] = load_saved()
    def _fetch_docs():
        results["docs"] = load_all_docs()
    t1 = threading.Thread(target=_fetch_saved)
    t2 = threading.Thread(target=_fetch_docs)
    t1.start(); t2.start()
    t1.join();  t2.join()
    st.session_state.saved_data = results["saved"]
    _loading.empty()

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
            _save_msg = st.empty()
            _save_msg.markdown(
                """
                <style>
                @keyframes flower-spin-save {
                    from { transform: rotate(0deg); }
                    to   { transform: rotate(360deg); }
                }
                .sv-inline {
                    display: flex;
                    align-items: center;
                    gap: 0.5rem;
                    padding: 0.4rem 0;
                }
                .sv-flower {
                    display: inline-block;
                    animation: flower-spin-save 1.2s linear infinite;
                    font-size: 1rem;
                    line-height: 1;
                }
                .sv-text {
                    color: #888;
                    font-size: 1rem;
                    font-family: 'Inter', sans-serif;
                    font-weight: 500;
                }
                </style>
                <div class="sv-inline">
                    <span class="sv-flower">🌸</span>
                    <span class="sv-text">Wait a moment...</span>
                </div>
                """,
                unsafe_allow_html=True,
            )
            try:
                opts = storage_options()
                # Build updated row
                new_entry = pd.DataFrame([{
                    "purchasing_doc_no":     doc_no,
                    "user_status":           "",
                    "purchaser_status":      purchaser_status,
                    "comment":               comment,
                    "new_purchasing_doc_no": new_doc_no,
                    "update_at":             datetime.now(pytz.timezone("Asia/Bangkok")).replace(tzinfo=None),
                }])
                # Preserve existing user_status from email alert
                df = st.session_state.saved_data
                existing_row = df[df["purchasing_doc_no"] == doc_no]
                if not existing_row.empty and "user_status" in existing_row.columns:
                    new_entry["user_status"] = existing_row.iloc[0]["user_status"]
                # Build full updated df (no duplicates)
                updated_df = pd.concat(
                    [new_entry, df[df["purchasing_doc_no"] != doc_no]],
                    ignore_index=True,
                )
                # Fire background overwrite — write-only, no read, fast
                threading.Thread(
                    target=_overwrite_to_delta,
                    args=(updated_df.copy(), opts),
                    daemon=False,
                ).start()
                # Update UI immediately
                st.session_state.saved_data = updated_df
                st.session_state.flash_doc_no = doc_no
                st.session_state.flash_key = int(datetime.now().timestamp() * 1000)
                _save_msg.empty()
                st.toast(f"Saved — {doc_no}", icon="✅")
                st.rerun()
            except Exception as e:
                _save_msg.empty()
                st.error(f"Save failed: {e}")

# ── RIGHT: TABLE ──
with col_table:
    df_saved = st.session_state.saved_data
    count = len(df_saved)
    df_saved = df_saved.head(20)

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

        col_labels = {
            "purchasing_doc_no":     "Doc No",
            "user_status":           "User Status",
            "purchaser_status":      "Purchaser Status",
            "comment":               "Comment",
            "new_purchasing_doc_no": "เลขสัญญาใหม่",
            "update_at":             "Updated At",
        }
        if name_col:
            col_labels[name_col] = "Contract Name"

        flash_doc = st.session_state.get("flash_doc_no", None)
        flash_key = st.session_state.get("flash_key", 0)
        anim_name = f"row-flash-{flash_key}"

        header_html = "".join(
            f'<th style="padding:0.5rem 0.75rem; text-align:left; font-size:0.72rem; '
            f'font-weight:700; text-transform:uppercase; letter-spacing:0.08em; '
            f'color:rgba(255,255,255,0.5); border-bottom:1px solid rgba(255,255,255,0.1); white-space:nowrap;">'
            f'{col_labels.get(c, c)}</th>'
            for c in display_df.columns
        )

        rows_html = ""
        for _, row in display_df.iterrows():
            is_flash = (flash_doc and row.get("purchasing_doc_no") == flash_doc)
            flash_style = (
                f'animation:{anim_name} 6s ease forwards;'
                if is_flash else ''
            )
            cells = ""
            for c in display_df.columns:
                val = row[c]
                if pd.isna(val) if not isinstance(val, str) else (val == ""):
                    val = ""
                elif c == "update_at" and not isinstance(val, str):
                    try:
                        val = pd.Timestamp(val).strftime("%Y-%m-%d %H:%M")
                    except Exception:
                        val = str(val)
                else:
                    val = str(val)
                cells += (
                    f'<td style="padding:0.45rem 0.75rem; font-size:0.82rem; '
                    f'color:rgba(255,255,255,0.85); border-bottom:1px solid rgba(255,255,255,0.08); '
                    f'white-space:nowrap; max-width:200px; overflow:hidden; '
                    f'text-overflow:ellipsis;">{val}</td>'
                )
            rows_html += f'<tr style="{flash_style}">{cells}</tr>'

        st.markdown(
            f"""
            <style>
            @keyframes {anim_name} {{
                0%   {{ background-color: #FFE3E1; }}
                100% {{ background-color: transparent; }}
            }}
            .saved-table {{ width:100%; border-collapse:collapse; font-family:'Sora',sans-serif; }}
            .saved-table tr:hover td {{ background-color:rgba(6,182,212,0.08); }}
            </style>
            <div style="overflow-x:auto;">
            <table class="saved-table">
              <thead><tr>{header_html}</tr></thead>
              <tbody>{rows_html}</tbody>
            </table>
            </div>
            """,
            unsafe_allow_html=True,
        )
        # Clear flash after rendering so it doesn't re-trigger on next interaction
        st.session_state.flash_doc_no = None

    st.markdown("<div style='height:0.5rem'></div>", unsafe_allow_html=True)

    if st.button("↺  Refresh data", use_container_width=True):
        load_saved.clear()          # reload status data only
        st.session_state.saved_data = None
        st.rerun()

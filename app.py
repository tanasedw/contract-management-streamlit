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
    @import url('https://fonts.googleapis.com/css2?family=Plus+Jakarta+Sans:wght@300;400;500;600;700&display=swap');

    :root {
        --neu-bg:     #e0e5ec;
        --neu-text:   #2d3748;
        --neu-muted:  #718096;
        --neu-accent: #6366f1;
        --neu-light:  #ffffff;
        --neu-dark:   #a3b1c6;
        --neu-flat:   6px 6px 12px #a3b1c6, -6px -6px 12px #ffffff;
        --neu-pressed: inset 4px 4px 8px #a3b1c6, inset -4px -4px 8px #ffffff;
        --neu-convex:  6px 6px 12px #a3b1c6, -6px -6px 12px #ffffff,
                       inset 2px 2px 4px #ffffff, inset -2px -2px 4px rgba(163,177,198,0.3);
    }

    html, body, [class*="css"] {
        font-family: 'Plus Jakarta Sans', sans-serif !important;
        background-color: var(--neu-bg) !important;
        color: var(--neu-text) !important;
    }

    /* Background */
    .stApp {
        background-color: var(--neu-bg) !important;
        color: var(--neu-text) !important;
    }

    /* Hide default UI */
    #MainMenu, footer, header { visibility: hidden; }

    /* Title */
    h1 {
        font-family: 'Plus Jakarta Sans', sans-serif !important;
        font-weight: 700 !important;
        font-size: 1.5rem !important;
        color: var(--neu-text) !important;
        letter-spacing: -0.03em !important;
        margin-bottom: 0.1rem !important;
    }

    /* Caption */
    .element-container p small,
    [data-testid="stCaptionContainer"] p {
        color: var(--neu-muted) !important;
        font-size: 0.78rem !important;
    }

    /* Subheader */
    h3 {
        font-family: 'Plus Jakarta Sans', sans-serif !important;
        font-weight: 600 !important;
        font-size: 0.68rem !important;
        color: var(--neu-muted) !important;
        text-transform: uppercase !important;
        letter-spacing: 0.12em !important;
        margin-bottom: 1rem !important;
    }

    /* Label */
    label, [data-testid="stWidgetLabel"] p {
        color: var(--neu-muted) !important;
        font-size: 0.88rem !important;
        font-weight: 600 !important;
        text-transform: uppercase !important;
        letter-spacing: 0.1em !important;
    }

    /* Selectbox */
    .stSelectbox > div > div {
        background-color: var(--neu-bg) !important;
        border: none !important;
        border-radius: 12px !important;
        box-shadow: var(--neu-pressed) !important;
        color: var(--neu-text) !important;
        font-family: 'Plus Jakarta Sans', sans-serif !important;
        font-size: 0.88rem !important;
    }
    .stSelectbox > div > div:focus-within {
        box-shadow: var(--neu-pressed), 0 0 0 2px var(--neu-accent) !important;
    }
    .stSelectbox [data-baseweb="select"] input {
        color: var(--neu-text) !important;
    }
    [data-baseweb="popover"] {
        background-color: var(--neu-bg) !important;
        border: none !important;
        border-radius: 14px !important;
        box-shadow: var(--neu-flat) !important;
    }
    [data-baseweb="menu"] {
        background-color: var(--neu-bg) !important;
    }
    [data-baseweb="menu"] li {
        color: var(--neu-text) !important;
        font-family: 'Plus Jakarta Sans', sans-serif !important;
        font-size: 0.85rem !important;
    }
    [data-baseweb="menu"] li:hover {
        background-color: rgba(99,102,241,0.08) !important;
    }

    /* Text area */
    .stTextArea textarea {
        background-color: var(--neu-bg) !important;
        border: none !important;
        border-radius: 12px !important;
        box-shadow: var(--neu-pressed) !important;
        color: var(--neu-text) !important;
        font-family: 'Plus Jakarta Sans', sans-serif !important;
        font-size: 0.88rem !important;
    }
    .stTextArea textarea:focus {
        box-shadow: var(--neu-pressed), 0 0 0 2px var(--neu-accent) !important;
        outline: none !important;
    }
    .stTextArea textarea::placeholder { color: var(--neu-muted) !important; }

    /* Text input */
    .stTextInput input {
        background-color: var(--neu-bg) !important;
        border: none !important;
        border-radius: 12px !important;
        box-shadow: var(--neu-pressed) !important;
        color: var(--neu-text) !important;
        font-family: 'Plus Jakarta Sans', sans-serif !important;
        font-size: 0.88rem !important;
    }
    .stTextInput input:focus {
        box-shadow: var(--neu-pressed), 0 0 0 2px var(--neu-accent) !important;
        outline: none !important;
    }
    .stTextInput input::placeholder { color: var(--neu-muted) !important; }

    /* Button Save (primary) */
    .stButton > button[kind="primary"] {
        background-color: var(--neu-bg) !important;
        color: var(--neu-accent) !important;
        border: none !important;
        border-radius: 12px !important;
        box-shadow: var(--neu-convex) !important;
        font-family: 'Plus Jakarta Sans', sans-serif !important;
        font-weight: 600 !important;
        font-size: 0.88rem !important;
        transition: all 0.2s ease !important;
    }
    .stButton > button[kind="primary"]:hover {
        box-shadow: 8px 8px 16px #a3b1c6, -8px -8px 16px #ffffff,
                    inset 2px 2px 4px #ffffff, inset -2px -2px 4px rgba(163,177,198,0.3) !important;
    }
    .stButton > button[kind="primary"]:active {
        box-shadow: var(--neu-pressed) !important;
    }

    /* Button Refresh (secondary) */
    .stButton > button[kind="secondary"] {
        background-color: var(--neu-bg) !important;
        color: var(--neu-muted) !important;
        border: none !important;
        border-radius: 12px !important;
        box-shadow: var(--neu-flat) !important;
        font-family: 'Plus Jakarta Sans', sans-serif !important;
        font-size: 0.85rem !important;
        transition: all 0.2s ease !important;
    }
    .stButton > button[kind="secondary"]:hover {
        color: var(--neu-accent) !important;
    }
    .stButton > button[kind="secondary"]:active {
        box-shadow: var(--neu-pressed) !important;
    }

    /* Alert */
    .stAlert {
        background-color: var(--neu-bg) !important;
        border: none !important;
        border-radius: 12px !important;
        box-shadow: var(--neu-flat) !important;
        color: var(--neu-muted) !important;
    }

    /* Spinner */
    .stSpinner > div {
        border-top-color: var(--neu-accent) !important;
    }

    /* Scrollbar */
    ::-webkit-scrollbar { width: 5px; height: 5px; }
    ::-webkit-scrollbar-track { background: var(--neu-bg); }
    ::-webkit-scrollbar-thumb { background: var(--neu-dark); border-radius: 10px; }
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
            background: #e0e5ec;
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
            color: #6366f1;
            font-size: 1rem;
            margin-top: 1.4rem;
            font-family: 'Plus Jakarta Sans', sans-serif;
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
            f'color:#888; border-bottom:1.5px solid #e8e8e8; white-space:nowrap;">'
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
                    f'color:#0f0f0f; border-bottom:1px solid #f0f0f0; '
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
            .saved-table {{ width:100%; border-collapse:collapse; font-family:'Plus Jakarta Sans',sans-serif; }}
            .saved-table tr:hover td {{ background-color:rgba(99,102,241,0.05); }}
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

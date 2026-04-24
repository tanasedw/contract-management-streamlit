# Contract Management Streamlit — CLAUDE.md

## Current stable version
Commit: `b1a6bd2` — "Revert UX: table shows 11 rows with sticky header and scrollbar for older entries"

## Stack
- **Frontend**: Streamlit (Python), deployed on Streamlit Community Cloud
- **Storage**: Microsoft Fabric OneLake via Delta Lake (`deltalake` Python library)
- **Auth**: Azure AD client credentials → bearer token, cached 3000s
- **Repo**: https://github.com/tanasedw/contract-management-streamlit

## Key decisions & patterns

### Save flow
- Build updated df in memory from `st.session_state.saved_data` (no OneLake read needed)
- Fire `write_deltalake(mode="overwrite")` in a **background thread** (`daemon=False`)
- Update session state immediately → instant UI response
- 🌸 spinning flower + "Wait a moment..." shown inline below Save button during write

### Load flow
- `load_saved()` and `load_all_docs()` both use `@st.cache_data(ttl=600, show_spinner=False)`
- On first load, both run in **parallel threads** behind a full-page 🌸 flower overlay
- Refresh button clears only `load_saved` cache (skips master docs table — rarely changes)

### Table display
- Custom HTML table (not `st.dataframe`) — enables row-level CSS animation
- Saved row flashes **pink #FFE3E1** for 6s after save using unique CSS animation name per save (forces browser replay)
- `load_saved()` deduplicates by `purchasing_doc_no` keeping latest `update_at`

### Loading overlay
- Background: Benjamin Moore Cloud White `#F4F0E8`
- Spinner: 🌸 emoji with CSS `animation: rotate` 1.2s linear infinite
- Text: "WAIT A MOMENT..." in gold `#e8a347`

## Purchaser Status options
```
Not Start
RFQ - Request for Quotation
Compared
RL - Recommend Letter
OA Created
Cancel OA
Completed
```

## OneLake tables
- `gold_contract_management` — master contract list (read only)
- `gold_manual_contract_status` — purchaser status entries (read/write)

## Form auto-fill
When a Purchasing Doc No is selected, the form pre-fills Purchaser Status, Comment, and เลขสัญญาใหม่ from the last saved values for that doc.

# Contract Management Streamlit — CLAUDE.md

## Current stable version
Commit: `590ecd3` — "Style: increase loading icon size to 150x150px"

## Stack
- **Frontend**: Streamlit (Python), deployed on Streamlit Community Cloud
- **Storage**: Microsoft Fabric OneLake via Delta Lake (`deltalake` Python library)
- **Auth**: Azure AD client credentials → bearer token, cached 3000s
- **Repo**: https://github.com/tanasedw/contract-management-streamlit

## Theme — Aurora Mesh Gradient
- Background: `#0f0f1a` dark navy + animated aurora (cyan/purple/pink/blue radial gradients)
- Font: **Sora**
- Inputs/Select: glass morphism `rgba(255,255,255,0.05)` + blur
- Input text (Comment & เลขสัญญาใหม่): `#a855f7` purple
- Button Save: gradient cyan `#06b6d4` → purple `#a855f7` with glow on hover
- Title: shimmer gradient text (white → cyan → purple)

## Key decisions & patterns

### Save flow
- Build updated df in memory from `st.session_state.saved_data` (no OneLake read needed)
- Fire `write_deltalake(mode="overwrite")` in a **background thread** (`daemon=False`)
- Update session state immediately → instant UI response
- Snowman wizard icon (150×150px) spinning + "Wait a moment..." shown inline below Save button

### Load flow
- `load_saved()` and `load_all_docs()` both use `@st.cache_data(ttl=600, show_spinner=False)`
- On first load, both run in **parallel threads** behind a full-page overlay (snowman wizard 150×150px spinning)
- Refresh button clears only `load_saved` cache (skips master docs table — rarely changes)

### Loading overlay
- Background: `#0f0f1a` (matches aurora theme)
- Spinner: `icon/png1.png` snowman wizard encoded as base64, 150×150px, CSS rotate animation
- Text: "WAIT A MOMENT..." in cyan `#06b6d4`

### Table display
- Custom HTML table (not `st.dataframe`) — enables row-level CSS animation
- Saved row flashes **pink #FFE3E1** for 6s after save using unique CSS animation name per save (forces browser replay)
- `load_saved()` deduplicates by `purchasing_doc_no` keeping latest `update_at`

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

# Contract Status Management

Streamlit web app สำหรับจัดการสถานะสัญญา (Contract Status) เชื่อมต่อกับ Microsoft Fabric / OneLake ผ่าน Delta Lake

## Features

- เลือก Purchasing Doc No และกำหนด **User Status** / **Purchaser Status** (confirmed หรือว่าง)
- บันทึกข้อมูลแบบ upsert กลับไปยัง Delta Table บน OneLake
- แสดงรายการสถานะที่บันทึกแล้วทั้งหมดพร้อม timestamp (Asia/Bangkok)
- Refresh โหลดข้อมูลใหม่จาก Delta Lake

## Delta Tables

| Table | Description |
|---|---|
| `gold_contract_management` | ข้อมูล Purchasing Doc ทั้งหมด (read only) |
| `gold_manual_contract_status` | สถานะที่กรอกด้วยมือ (read/write) |

## Setup

### 1. Install dependencies

```bash
pip install -r requirements.txt
```

### 2. Configure secrets

สร้างไฟล์ `.streamlit/secrets.toml` และกรอกข้อมูล Azure AD:

```toml
TENANT_ID     = "your-tenant-id"
CLIENT_ID     = "your-client-id"
CLIENT_SECRET = "your-client-secret"
```

### 3. Run

```bash
streamlit run app.py
```

## Tech Stack

- [Streamlit](https://streamlit.io/) — Web UI
- [Delta Lake (deltalake)](https://delta-io.github.io/delta-rs/) — อ่าน/เขียน Delta Table
- [Microsoft Fabric OneLake](https://learn.microsoft.com/en-us/fabric/onelake/onelake-overview) — Data storage
- Azure AD Client Credentials — Authentication

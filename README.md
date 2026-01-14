# Annual Pass Reconciliation UI

A Streamlit-based web interface for the Toll Plaza FASTag Annual Pass Reconciliation Pipeline.

## 🚀 Quick Start

```bash
# Create virtual environment
python3 -m venv venv

# Activate virtual environment
source venv/bin/activate  # Linux/Mac
# OR
venv\Scripts\activate  # Windows

# Install dependencies
pip install -r requirements.txt

# Run the application
streamlit run app.py

# Open in browser: http://localhost:8501
```

## ✨ Features

- **📤 File Upload**: Drag-and-drop Excel/CSV files
- **🔪 Slicer**: Filters ANNUALPASS transactions from raw files
- **🔗 Merger**: Combines monthly files by project/plaza
- **📊 Reconciler**: Calculates TripCount and ATP/NAP summaries
- **📥 Download**: Export all results as ZIP

## 📁 Supported File Formats

- Excel (`.xlsx`, `.xls`, `.xlsb`)
- CSV (`.csv`)

## 🏢 Supported Banks

| Bank | Plazas |
|------|--------|
| ICICI | 17 |
| IDFC | 13 |

## 📊 Output Files

- `{project}_transactions_with_tripcount.csv` - All transactions with TripCount
- `{project}_daily_ATP_NAP_plaza.csv` - Daily summary by plaza

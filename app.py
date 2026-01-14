"""
Annual Pass Reconciliation Pipeline UI
A Streamlit-based interface for processing toll plaza FASTag ANNUAL PASS transactions.
"""

import streamlit as st
import pandas as pd
import os
import sys
import shutil
import tempfile
import zipfile
from datetime import datetime
from io import BytesIO
import time

# Add parent directory to path for importing pipeline modules
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PARENT_DIR = os.path.dirname(SCRIPT_DIR)
sys.path.insert(0, PARENT_DIR)

# Page configuration
st.set_page_config(
    page_title="Annual Pass Reconciler",
    page_icon="🚗",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for premium styling - Light soothing theme
st.markdown("""
<style>
    /* Main background and theme - Light soothing colors */
    .stApp {
        background: linear-gradient(135deg, #f8fafc 0%, #e8f4f8 50%, #f0f5ff 100%);
    }
    
    /* Header styling */
    .main-header {
        background: linear-gradient(90deg, #4a90a4 0%, #6b8cce 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
        font-size: 3rem;
        font-weight: 800;
        text-align: center;
        margin-bottom: 0.5rem;
        font-family: 'Inter', sans-serif;
    }
    
    .sub-header {
        color: #5a6a7a;
        text-align: center;
        font-size: 1.1rem;
        margin-bottom: 2rem;
    }
    
    /* Card styling */
    .pipeline-card {
        background: rgba(255, 255, 255, 0.85);
        backdrop-filter: blur(10px);
        border-radius: 16px;
        padding: 1.5rem;
        border: 1px solid rgba(74, 144, 164, 0.2);
        margin-bottom: 1rem;
        transition: all 0.3s ease;
        box-shadow: 0 2px 8px rgba(74, 144, 164, 0.08);
    }
    
    .pipeline-card:hover {
        transform: translateY(-2px);
        border-color: rgba(74, 144, 164, 0.4);
        box-shadow: 0 8px 32px rgba(74, 144, 164, 0.12);
    }
    
    .step-title {
        color: #2d3748;
        font-size: 1.3rem;
        font-weight: 600;
        margin-bottom: 0.5rem;
    }
    
    .step-description {
        color: #4a5568;
        font-size: 0.95rem;
        line-height: 1.6;
    }
    
    .step-number {
        background: linear-gradient(135deg, #6b9eb8 0%, #8ba4c9 100%);
        color: white;
        width: 36px;
        height: 36px;
        border-radius: 50%;
        display: inline-flex;
        align-items: center;
        justify-content: center;
        font-weight: 700;
        margin-right: 12px;
    }
    
    /* Status indicators */
    .status-success {
        background: linear-gradient(135deg, #68d391 0%, #48bb78 100%);
        color: white;
        padding: 0.5rem 1rem;
        border-radius: 8px;
        font-weight: 600;
        display: inline-block;
    }
    
    .status-pending {
        background: linear-gradient(135deg, #f6c87a 0%, #eaab5c 100%);
        color: #744210;
        padding: 0.5rem 1rem;
        border-radius: 8px;
        font-weight: 600;
        display: inline-block;
    }
    
    .status-error {
        background: linear-gradient(135deg, #fc8d8d 0%, #f56565 100%);
        color: white;
        padding: 0.5rem 1rem;
        border-radius: 8px;
        font-weight: 600;
        display: inline-block;
    }
    
    /* Upload area */
    .uploadfile {
        border: 2px dashed rgba(74, 144, 164, 0.4);
        border-radius: 16px;
        background: rgba(74, 144, 164, 0.05);
    }
    
    /* Progress bar */
    .stProgress > div > div {
        background: linear-gradient(90deg, #6b9eb8 0%, #8ba4c9 100%);
    }
    
    /* Buttons */
    .stButton > button {
        background: linear-gradient(135deg, #6b9eb8 0%, #8ba4c9 100%);
        color: white;
        border: none;
        border-radius: 12px;
        padding: 0.75rem 2rem;
        font-weight: 600;
        transition: all 0.3s ease;
        width: 100%;
    }
    
    .stButton > button:hover {
        transform: translateY(-2px);
        box-shadow: 0 8px 25px rgba(74, 144, 164, 0.3);
    }
    
    /* Sidebar */
    .css-1d391kg, [data-testid="stSidebar"] {
        background: linear-gradient(180deg, #f0f7fa 0%, #e8f0f5 100%);
    }
    
    [data-testid="stSidebar"] .stMarkdown {
        color: #2d3748;
    }
    
    /* Metrics */
    .metric-card {
        background: linear-gradient(135deg, rgba(107, 158, 184, 0.1) 0%, rgba(139, 164, 201, 0.1) 100%);
        border-radius: 12px;
        padding: 1.25rem;
        text-align: center;
        border: 1px solid rgba(74, 144, 164, 0.2);
    }
    
    .metric-value {
        font-size: 2rem;
        font-weight: 700;
        background: linear-gradient(90deg, #4a90a4 0%, #6b8cce 100%);
        -webkit-background-clip: text;
        -webkit-text-fill-color: transparent;
    }
    
    .metric-label {
        color: #4a5568;
        font-size: 0.9rem;
        margin-top: 0.25rem;
    }
    
    /* Table styling */
    .dataframe {
        background: rgba(255, 255, 255, 0.8) !important;
        border-radius: 12px !important;
    }
    
    /* Hide Streamlit branding */
    #MainMenu {visibility: hidden;}
    footer {visibility: hidden;}
    
    /* Animation */
    @keyframes pulse {
        0%, 100% { opacity: 1; }
        50% { opacity: 0.5; }
    }
    
    .processing {
        animation: pulse 2s infinite;
    }
    
    /* Override Streamlit default text colors for light theme */
    .stMarkdown, .stText, p, span, label {
        color: #2d3748 !important;
    }
    
    h1, h2, h3, h4, h5, h6 {
        color: #1a365d !important;
    }
    
    /* File uploader styling */
    [data-testid="stFileUploader"] {
        background: rgba(255, 255, 255, 0.6);
        border-radius: 12px;
        padding: 1rem;
    }
    
    /* Expander styling */
    .streamlit-expanderHeader {
        background: rgba(255, 255, 255, 0.8);
        border-radius: 8px;
        color: #2d3748 !important;
    }
</style>
""", unsafe_allow_html=True)


def initialize_session_state():
    """Initialize session state variables."""
    if 'uploaded_files' not in st.session_state:
        st.session_state.uploaded_files = []
    if 'processing_complete' not in st.session_state:
        st.session_state.processing_complete = False
    if 'current_step' not in st.session_state:
        st.session_state.current_step = 0
    if 'results' not in st.session_state:
        st.session_state.results = {}
    if 'temp_dir' not in st.session_state:
        st.session_state.temp_dir = None
    if 'processing_log' not in st.session_state:
        st.session_state.processing_log = []


def create_temp_directory():
    """Create a temporary directory for processing."""
    if st.session_state.temp_dir is None or not os.path.exists(st.session_state.temp_dir):
        st.session_state.temp_dir = tempfile.mkdtemp(prefix="reconciliation_")
    return st.session_state.temp_dir


def cleanup_temp_directory():
    """Clean up temporary directory."""
    if st.session_state.temp_dir and os.path.exists(st.session_state.temp_dir):
        shutil.rmtree(st.session_state.temp_dir, ignore_errors=True)
        st.session_state.temp_dir = None


def add_log(message, level="info"):
    """Add a log message."""
    timestamp = datetime.now().strftime("%H:%M:%S")
    icons = {"info": "ℹ️", "success": "✅", "warning": "⚠️", "error": "❌"}
    st.session_state.processing_log.append({
        "time": timestamp,
        "level": level,
        "message": message,
        "icon": icons.get(level, "ℹ️")
    })


def render_header():
    """Render the main header."""
    st.markdown('<h1 class="main-header">🚗 Annual Pass Reconciler</h1>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Process toll plaza FASTag ANNUAL PASS transactions with ease</p>', unsafe_allow_html=True)


def render_sidebar():
    """Render the sidebar with information."""
    with st.sidebar:
        st.markdown("### 📊 Pipeline Overview")
        st.markdown("""
        This tool processes toll transaction files through three stages:
        
        **1️⃣ Slicer**  
        Extracts ANNUALPASS transactions from raw files
        
        **2️⃣ Merger**  
        Combines monthly files by project/plaza
        
        **3️⃣ Reconciler**  
        Calculates TripCount and generates ATP/NAP summaries
        """)
        
        st.divider()
        
        st.markdown("### 📁 Supported Formats")
        st.markdown("""
        - Excel (`.xlsx`, `.xls`, `.xlsb`)
        - CSV (`.csv`)
        """)
        
        st.divider()
        
        st.markdown("### 🏢 Supported Banks")
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**ICICI Bank**")
            st.caption("17 Plazas")
        with col2:
            st.markdown("**IDFC Bank**")
            st.caption("13 Plazas")
        
        st.divider()
        
        if st.button("🗑️ Clear Session", use_container_width=True):
            cleanup_temp_directory()
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.rerun()


def render_upload_section():
    """Render the file upload section."""
    st.markdown("### 📤 Upload Transaction Files")
    
    with st.container():
        uploaded_files = st.file_uploader(
            "Drop your Excel or CSV files here",
            type=['xlsx', 'xls', 'xlsb', 'csv'],
            accept_multiple_files=True,
            help="Upload toll transaction files in Excel or CSV format"
        )
        
        if uploaded_files:
            st.session_state.uploaded_files = uploaded_files
            
            # Show uploaded files
            st.markdown(f"**📎 {len(uploaded_files)} file(s) uploaded:**")
            
            cols = st.columns(3)
            for idx, file in enumerate(uploaded_files):
                with cols[idx % 3]:
                    file_icon = "📊" if file.name.endswith(('.xlsx', '.xls', '.xlsb')) else "📄"
                    st.markdown(f"""
                    <div style="background: rgba(102, 126, 234, 0.1); padding: 0.75rem; border-radius: 8px; margin-bottom: 0.5rem;">
                        {file_icon} <strong>{file.name}</strong><br>
                        <small style="color: #94a3b8;">{file.size / 1024:.1f} KB</small>
                    </div>
                    """, unsafe_allow_html=True)


def save_uploaded_files(temp_dir, uploaded_files):
    """Save uploaded files to temporary directory."""
    input_dir = os.path.join(temp_dir, "input")
    os.makedirs(input_dir, exist_ok=True)
    
    for file in uploaded_files:
        file_path = os.path.join(input_dir, file.name)
        with open(file_path, 'wb') as f:
            f.write(file.getbuffer())
    
    return input_dir


def run_slicer(input_dir, temp_dir, progress_callback=None):
    """Run the slicer step."""
    import polars as pl
    from fastexcel import read_excel as fastexcel_read
    
    # Import configuration from slicer module
    PLAZA_ID_HEADERS = {" Plaza ID", "Entry Plaza Code", "Entry Plaza Id", " Plaza Code", " Entry Plaza Code"}
    ANNUAL_PASS_VALUES = {"ANNUALPASS", "ANNUAL PASS"}
    
    BANK_PLAZA_MAP = {
        "IDFC": {
            "142001": ("Ghoti", "IHPL"), "142002": ("Arjunali", "IHPL"),
            "220001": ("Raipur", "BPPTPL"), "220002": ("Indranagar", "BPPTPL"),
            "220003": ("Birami", "BPPTPL"), "220004": ("Uthman", "BPPTPL"),
            "235001": ("Mandawada", "SUTPL"), "235002": ("Negadiya", "SUTPL"),
            "243000": ("Rupakheda", "BRTPL"), "243001": ("Mujras", "BRTPL"),
            "073001": ("Bollapalli", "SEL"), "073002": ("Tangutur", "SEL"),
            "073003": ("Musunur", "SEL")
        },
        "ICICI": {
            "540030": ("Ladgaon", "CSJTPL"), "540032": ("Nagewadi", "CSJTPL"),
            "120001": ("Shanthigrama", "DHTPL"), "120002": ("Kadabahalli", "DHTPL"),
            "139001": ("Shirpur", "DPTL"), "139002": ("Songir", "DPTL"),
            "167001": ("Vaniyambadi", "KWTPL"), "167002": ("Pallikonda", "KWTPL"),
            "169001": ("Palayam", "KTTRL"), "234002": ("Chagalamarri", "REPL"),
            "352001": ("Nannur", "REPL"), "352013": ("Chapirevula", "REPL"),
            "352065": ("Patimeedapalli", "REPL"), "045001": ("Gudur", "HYTPL"),
            "046001": ("Kasaba", "BHTPL"), "046002": ("Nagarhalla", "BHTPL"),
            "079001": ("Shakapur", "WATL")
        }
    }
    
    BANK_COLUMN_MAP = {
        "ICICI": {"FastagReasonCode": ("Reason", "Reason Code")},
        "IDFC": {"FastagReasonCode": " Trc Vrc Reason Code"}
    }
    
    OUTPUT_COLUMNS = {
        "ICICI": {
            "TransactionDateTime": ("Transaction Date", "Entry Txn Date"),
            "VRN": ("Licence Plate No.", "License Plate No."),
            "TagID": ("Tag Id", "Hex Tag No"),
            "TripType": ("Trip Type", "TRIPTYPEDISCRIPTION")
        },
        "IDFC": {
            "TransactionDateTime": " Reader Read Time",
            "VRN": " Vehicle Reg. No.",
            "TagID": " Tag ID",
            "TripType": " Journey Type"
        }
    }
    
    sliced_dir = os.path.join(temp_dir, "SLICED")
    os.makedirs(sliced_dir, exist_ok=True)
    
    results = {"files_processed": 0, "files_written": 0, "rows_extracted": 0}
    
    def resolve_plaza(plaza_id):
        plaza_id = str(plaza_id).strip().strip("'\"")
        if '.' in plaza_id:
            try:
                plaza_id = str(int(float(plaza_id)))
            except ValueError:
                pass
        plaza_id = plaza_id.zfill(6)
        for bank, plazas in BANK_PLAZA_MAP.items():
            if plaza_id in plazas:
                plaza_name, project_name = plazas[plaza_id]
                return bank, plaza_name, project_name
        return None, None, None
    
    def resolve_column_name(column_config, df_columns, col_name_map=None):
        if col_name_map is None:
            col_name_map = {}
        if isinstance(column_config, tuple):
            columns_to_check = list(column_config)
        else:
            columns_to_check = [column_config]
        for col_name in columns_to_check:
            if col_name in df_columns:
                return col_name
            if col_name in col_name_map:
                return col_name_map[col_name]
        return None
    
    def standardize_output(df):
        import re
        text_cols_to_clean = ['VRN', 'TagID', 'PlazaID']
        for col in text_cols_to_clean:
            if col in df.columns:
                df = df.with_columns(
                    pl.col(col).cast(pl.Utf8)
                      .str.replace_all(r'[\n\t\r]+', ' ')
                      .str.replace_all(r'\s+', ' ')
                      .str.strip_chars()
                      .alias(col)
                )
        
        if 'TransactionDateTime' in df.columns:
            col_dtype = df.schema.get('TransactionDateTime')
            if col_dtype == pl.Datetime or str(col_dtype).startswith('Datetime'):
                df = df.with_columns(
                    pl.col('TransactionDateTime').dt.strftime('%Y-%m-%d %H:%M:%S').alias('TransactionDateTime')
                )
            elif col_dtype in (pl.Float64, pl.Float32, pl.Int64):
                from datetime import datetime as dt, timedelta
                EXCEL_EPOCH = dt(1899, 12, 30)
                df = df.with_columns(
                    (pl.lit(EXCEL_EPOCH) + pl.duration(days=pl.col('TransactionDateTime').cast(pl.Int64)) +
                     pl.duration(seconds=((pl.col('TransactionDateTime') % 1) * 86400).cast(pl.Int64)))
                    .dt.strftime('%Y-%m-%d %H:%M:%S')
                    .alias('TransactionDateTime')
                )
            else:
                df = df.with_columns(
                    pl.coalesce([
                        pl.col('TransactionDateTime').str.to_datetime('%Y-%m-%d %H:%M:%S', strict=False),
                        pl.col('TransactionDateTime').str.to_datetime('%d-%m-%Y %H:%M:%S', strict=False),
                        pl.col('TransactionDateTime').str.to_datetime('%d/%m/%Y %H:%M:%S', strict=False),
                    ]).dt.strftime('%Y-%m-%d %H:%M:%S').alias('TransactionDateTime')
                )
        return df
    
    def extract_month_year(df, time_col):
        col_dtype = df.schema.get(time_col)
        if col_dtype == pl.Datetime or str(col_dtype).startswith('Datetime'):
            df = df.with_columns(pl.col(time_col).dt.strftime('%b-%y').alias('MonthYear'))
        elif col_dtype in (pl.Float64, pl.Float32, pl.Int64):
            from datetime import datetime as dt
            EXCEL_EPOCH = dt(1899, 12, 30)
            df = df.with_columns(
                (pl.lit(EXCEL_EPOCH) + pl.duration(days=pl.col(time_col).cast(pl.Int64)))
                .dt.strftime('%b-%y')
                .alias('MonthYear')
            )
        else:
            df = df.with_columns(
                pl.coalesce([
                    pl.col(time_col).str.to_datetime('%Y-%m-%d %H:%M:%S', strict=False).dt.strftime('%b-%y'),
                    pl.col(time_col).str.to_datetime('%d-%m-%Y %H:%M:%S', strict=False).dt.strftime('%b-%y'),
                    pl.col(time_col).str.to_datetime('%d/%m/%Y %H:%M:%S', strict=False).dt.strftime('%b-%y'),
                ]).alias('MonthYear')
            )
        return df
    
    def write_grouped_by_month(df, project_name, plaza_name, time_col='TransactionDateTime'):
        nonlocal results
        if time_col not in df.columns:
            return
        
        df = extract_month_year(df, time_col)
        months = df.select('MonthYear').drop_nulls().unique().to_series().to_list()
        
        for month in months:
            if month is None:
                continue
            df_month = df.filter(pl.col('MonthYear') == month).drop('MonthYear')
            if df_month.height == 0:
                continue
            
            out_dir = os.path.join(sliced_dir, month, project_name)
            os.makedirs(out_dir, exist_ok=True)
            out_filename = f"{plaza_name}_ANNUALPASS.csv"
            out_path = os.path.join(out_dir, out_filename)
            
            df_month = standardize_output(df_month)
            df_month = df_month.with_columns([pl.col(c).cast(pl.Utf8) for c in df_month.columns])
            
            if os.path.exists(out_path):
                existing = pl.read_csv(out_path)
                existing = existing.with_columns([pl.col(c).cast(pl.Utf8) for c in existing.columns])
                common_cols = [c for c in existing.columns if c in df_month.columns]
                if common_cols:
                    existing = existing.select(common_cols)
                    df_month = df_month.select(common_cols)
                df_month = pl.concat([existing, df_month])
            
            df_month.write_csv(out_path)
            results["rows_extracted"] += df_month.height
            results["files_written"] += 1
    
    # Process files
    supported_ext = {".csv", ".xlsx", ".xls", ".xlsb"}
    files = [f for f in os.listdir(input_dir) if os.path.splitext(f)[1].lower() in supported_ext]
    
    for idx, filename in enumerate(files):
        if progress_callback:
            progress_callback(idx / len(files), f"Processing {filename}...")
        
        file_path = os.path.join(input_dir, filename)
        ext = os.path.splitext(filename)[1].lower()
        results["files_processed"] += 1
        
        try:
            if ext == ".csv":
                # Process CSV
                lf = pl.scan_csv(file_path, infer_schema_length=2000, truncate_ragged_lines=True, ignore_errors=True)
                schema_cols = set(lf.collect_schema().names())
                plaza_col = next((c for c in PLAZA_ID_HEADERS if c in schema_cols), None)
                if not plaza_col:
                    add_log(f"Skipping {filename}: No Plaza ID column", "warning")
                    continue
                
                plaza_df = lf.select(pl.col(plaza_col)).drop_nulls().limit(1).collect()
                if plaza_df.height == 0:
                    continue
                
                plaza_id = str(plaza_df[0, 0]).strip()
                bank, plaza_name, project_name = resolve_plaza(plaza_id)
                if not bank:
                    add_log(f"Unknown Plaza ID {plaza_id} in {filename}", "warning")
                    continue
                
                reason_col_config = BANK_COLUMN_MAP.get(bank, {}).get("FastagReasonCode")
                reason_col = resolve_column_name(reason_col_config, schema_cols) if reason_col_config else None
                
                if reason_col:
                    lf_filtered = lf.filter(
                        pl.col(reason_col).cast(pl.Utf8).str.strip_chars().str.to_uppercase().is_in(ANNUAL_PASS_VALUES)
                    )
                    df = lf_filtered.collect()
                else:
                    df = lf.collect()
                
                if df.height == 0:
                    continue
                
                # Select and rename columns
                bank_cols = OUTPUT_COLUMNS.get(bank, {})
                import re
                col_name_map = {}
                for col in df.columns:
                    normalized = re.sub(r'\s+', ' ', col.replace('\n', ' ').replace('\t', ' ')).strip()
                    col_name_map[normalized] = col
                
                cols_to_keep = []
                rename_map = {}
                for std_name, col_config in bank_cols.items():
                    actual_col = resolve_column_name(col_config, df.columns, col_name_map)
                    if actual_col:
                        cols_to_keep.append(actual_col)
                        rename_map[actual_col] = std_name
                
                if plaza_col in df.columns:
                    cols_to_keep.append(plaza_col)
                    rename_map[plaza_col] = "PlazaID"
                
                if cols_to_keep:
                    df = df.select(cols_to_keep).rename(rename_map)
                
                write_grouped_by_month(df, project_name, plaza_name)
                add_log(f"Processed {filename}: {df.height} rows", "success")
                
            else:
                # Process Excel
                xl = fastexcel_read(file_path)
                for sheet_name in xl.sheet_names:
                    try:
                        for header_row in [0, 1, 2]:
                            sheet = xl.load_sheet(sheet_name, header_row=header_row)
                            sheet_cols = {c.name for c in sheet.available_columns()}
                            plaza_col = next((c for c in PLAZA_ID_HEADERS if c in sheet_cols), None)
                            if plaza_col:
                                break
                        
                        if not plaza_col:
                            continue
                        
                        df = pl.from_arrow(xl.load_sheet(sheet_name, header_row=header_row, eager=True))
                        if df.height == 0:
                            continue
                        
                        unique_plazas = df.select(pl.col(plaza_col)).drop_nulls().unique().to_series().to_list()
                        
                        for raw_pid in unique_plazas:
                            pid_str = str(raw_pid).strip().strip("'\"")
                            if '.' in pid_str:
                                try:
                                    pid_str = str(int(float(pid_str)))
                                except:
                                    pass
                            normalized_pid = pid_str.zfill(6)
                            
                            bank, plaza_name, project_name = resolve_plaza(normalized_pid)
                            if not bank:
                                continue
                            
                            df_plaza = df.filter(pl.col(plaza_col) == raw_pid)
                            
                            reason_col_config = BANK_COLUMN_MAP.get(bank, {}).get("FastagReasonCode")
                            reason_col = resolve_column_name(reason_col_config, df_plaza.columns) if reason_col_config else None
                            
                            if reason_col:
                                df_filtered = df_plaza.filter(
                                    pl.col(reason_col).cast(pl.Utf8).str.strip_chars().str.to_uppercase().is_in(ANNUAL_PASS_VALUES)
                                )
                            else:
                                df_filtered = df_plaza
                            
                            if df_filtered.height == 0:
                                continue
                            
                            bank_cols = OUTPUT_COLUMNS.get(bank, {})
                            import re
                            col_name_map = {}
                            for col in df_filtered.columns:
                                normalized = re.sub(r'\s+', ' ', col.replace('\n', ' ').replace('\t', ' ')).strip()
                                col_name_map[normalized] = col
                            
                            cols_to_keep = []
                            rename_map = {}
                            for std_name, col_config in bank_cols.items():
                                actual_col = resolve_column_name(col_config, df_filtered.columns, col_name_map)
                                if actual_col:
                                    cols_to_keep.append(actual_col)
                                    rename_map[actual_col] = std_name
                            
                            if plaza_col in df_filtered.columns:
                                cols_to_keep.append(plaza_col)
                                rename_map[plaza_col] = "PlazaID"
                            
                            if cols_to_keep:
                                df_filtered = df_filtered.select(cols_to_keep).rename(rename_map)
                            
                            write_grouped_by_month(df_filtered, project_name, plaza_name)
                        
                        add_log(f"Processed {filename} - {sheet_name}", "success")
                    except Exception as e:
                        add_log(f"Error in sheet {sheet_name}: {str(e)}", "warning")
                        
        except Exception as e:
            add_log(f"Error processing {filename}: {str(e)}", "error")
    
    return results, sliced_dir


def run_merger(sliced_dir, temp_dir, progress_callback=None):
    """Run the merger step."""
    from collections import defaultdict
    
    merged_dir = os.path.join(temp_dir, "MERGED")
    os.makedirs(merged_dir, exist_ok=True)
    
    results = {"files_merged": 0, "total_rows": 0}
    
    # Build file map
    file_map = defaultdict(lambda: defaultdict(list))
    
    monthly_folders = [d for d in os.listdir(sliced_dir) if os.path.isdir(os.path.join(sliced_dir, d))]
    monthly_folders.sort()
    
    for month_folder in monthly_folders:
        month_path = os.path.join(sliced_dir, month_folder)
        project_folders = [d for d in os.listdir(month_path) if os.path.isdir(os.path.join(month_path, d))]
        
        for project in project_folders:
            project_path = os.path.join(month_path, project)
            for filename in os.listdir(project_path):
                if filename.lower().endswith('.csv'):
                    file_path = os.path.join(project_path, filename)
                    file_map[project][filename].append({
                        'path': file_path,
                        'month': month_folder
                    })
    
    total_projects = len(file_map)
    
    for proj_idx, (project, plaza_files) in enumerate(sorted(file_map.items())):
        if progress_callback:
            progress_callback(proj_idx / total_projects, f"Merging project {project}...")
        
        project_output_dir = os.path.join(merged_dir, project)
        os.makedirs(project_output_dir, exist_ok=True)
        
        for plaza_file, month_data in plaza_files.items():
            try:
                dfs = []
                for file_info in sorted(month_data, key=lambda x: x['month']):
                    df = pd.read_csv(file_info['path'])
                    df['SourceMonth'] = file_info['month']
                    dfs.append(df)
                
                if not dfs:
                    continue
                
                merged_df = pd.concat(dfs, ignore_index=True)
                if 'TransactionDateTime' in merged_df.columns:
                    merged_df = merged_df.sort_values('TransactionDateTime').reset_index(drop=True)
                
                output_path = os.path.join(project_output_dir, plaza_file)
                merged_df.to_csv(output_path, index=False)
                
                results["files_merged"] += 1
                results["total_rows"] += len(merged_df)
                
            except Exception as e:
                add_log(f"Error merging {project}/{plaza_file}: {str(e)}", "error")
    
    add_log(f"Merged {results['files_merged']} files with {results['total_rows']} total rows", "success")
    return results, merged_dir


def run_reconciler(merged_dir, temp_dir, progress_callback=None):
    """Run the reconciler step."""
    import polars as pl
    from collections import defaultdict
    
    output_dir = os.path.join(temp_dir, "RECONCILIATION_OUTPUT")
    os.makedirs(output_dir, exist_ok=True)
    
    BANK_PLAZA_MAP = {
        "IDFC": {
            "142001": ("Ghoti", "IHPL"), "142002": ("Arjunali", "IHPL"),
            "220001": ("Raipur", "BPPTPL"), "220002": ("Indranagar", "BPPTPL"),
            "220003": ("Birami", "BPPTPL"), "220004": ("Uthman", "BPPTPL"),
            "235001": ("Mandawada", "SUTPL"), "235002": ("Negadiya", "SUTPL"),
            "243000": ("Rupakheda", "BRTPL"), "243001": ("Mujras", "BRTPL"),
            "073001": ("Bollapalli", "SEL"), "073002": ("Tangutur", "SEL"),
            "073003": ("Musunur", "SEL")
        },
        "ICICI": {
            "540030": ("Ladgaon", "CSJTPL"), "540032": ("Nagewadi", "CSJTPL"),
            "120001": ("Shanthigrama", "DHTPL"), "120002": ("Kadabahalli", "DHTPL"),
            "139001": ("Shirpur", "DPTL"), "139002": ("Songir", "DPTL"),
            "167001": ("Vaniyambadi", "KWTPL"), "167002": ("Pallikonda", "KWTPL"),
            "169001": ("Palayam", "KTTRL"), "234002": ("Chagalamarri", "REPL"),
            "352001": ("Nannur", "REPL"), "352013": ("Chapirevula", "REPL"),
            "352065": ("Patimeedapalli", "REPL"), "045001": ("Gudur", "HYTPL"),
            "046001": ("Kasaba", "BHTPL"), "046002": ("Nagarhalla", "BHTPL"),
            "079001": ("Shakapur", "WATL")
        }
    }
    
    def resolve_plaza(plaza_id_raw):
        try:
            plaza_id = str(int(float(plaza_id_raw))).zfill(6)
        except (ValueError, TypeError):
            return None, None, None
        for bank, plazas in BANK_PLAZA_MAP.items():
            if plaza_id in plazas:
                plaza_name, project_name = plazas[plaza_id]
                return bank, plaza_name, project_name
        return None, None, None
    
    results = {"projects_processed": 0, "total_transactions": 0, "summary_rows": 0}
    
    # Discover files
    all_files = []
    for root, _, files in os.walk(merged_dir):
        for f in files:
            if f.lower().endswith('.csv') and not f.startswith('~$'):
                all_files.append(os.path.join(root, f))
    
    grouped_files = defaultdict(list)
    for file_path in all_files:
        project = os.path.basename(os.path.dirname(file_path))
        grouped_files[project].append(file_path)
    
    total_projects = len(grouped_files)
    
    for proj_idx, (project, files) in enumerate(grouped_files.items()):
        if progress_callback:
            progress_callback(proj_idx / total_projects, f"Reconciling project {project}...")
        
        try:
            project_dfs = []
            for file in files:
                try:
                    df = pl.read_csv(file, infer_schema_length=10000, schema_overrides={"TripType": pl.Utf8})
                    
                    if "TagID" not in df.columns and "VRN" in df.columns:
                        df = df.with_columns(pl.col("VRN").alias("TagID"))
                    
                    required = {"TransactionDateTime", "VRN", "TagID", "PlazaID"}
                    if required - set(df.columns):
                        continue
                    
                    cols = list(required)
                    if "TripType" in df.columns:
                        cols.append("TripType")
                    
                    df = df.select(cols).with_columns([
                        pl.col("PlazaID").cast(pl.Utf8).str.strip_chars().str.strip_chars("'").str.zfill(6),
                        pl.col("TransactionDateTime").str.to_datetime(strict=False)
                    ])
                    project_dfs.append(df)
                except:
                    continue
            
            if not project_dfs:
                continue
            
            df_all = pl.concat(project_dfs, how="vertical")
            pdf = df_all.to_pandas()
            
            pdf = pdf.rename(columns={
                "TransactionDateTime": "Reader Read Time",
                "TagID": "Tag ID",
                "VRN": "Vehicle Reg. No."
            })
            
            pdf["Reader Read Time"] = pd.to_datetime(pdf["Reader Read Time"])
            
            # Resolve plaza metadata
            plaza_meta = pdf["PlazaID"].apply(resolve_plaza)
            pdf["Bank"] = plaza_meta.apply(lambda x: x[0])
            pdf["PlazaName"] = plaza_meta.apply(lambda x: x[1])
            pdf["ProjectName"] = plaza_meta.apply(lambda x: x[2])
            
            pdf = pdf.sort_values(["PlazaID", "Vehicle Reg. No.", "Reader Read Time"]).reset_index(drop=True)
            
            # Calculate TripCount
            def calc_tripcount(group):
                group = group.sort_values("Reader Read Time").copy()
                times = group["Reader Read Time"].values
                n = len(times)
                trip_counts = []
                window_start = None
                window_end = None
                trip_count = 0
                
                for i in range(n):
                    t = pd.Timestamp(times[i])
                    if window_start is None or t > window_end:
                        window_start = t
                        window_end = window_start + pd.Timedelta(hours=24)
                        trip_count = 1
                    else:
                        trip_count += 1
                    trip_counts.append(trip_count)
                
                group["TripCount"] = trip_counts
                return group
            
            import warnings
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=FutureWarning)
                pdf = pdf.groupby(["PlazaID", "Vehicle Reg. No."], sort=False, group_keys=False).apply(calc_tripcount)
            pdf = pdf.reset_index(drop=True)
            
            # Calculate ReportDate
            def report_date(ts):
                if pd.isna(ts):
                    return None
                return (ts - pd.Timedelta(days=1)).date() if ts.time() < pd.Timestamp("08:00").time() else ts.date()
            
            pdf["ReportDate"] = pdf["Reader Read Time"].apply(report_date)
            pdf["IsQualifiedNAP"] = pdf["TripCount"] <= 2
            
            # Generate summary
            daily_summary = (
                pdf.groupby(["ProjectName", "PlazaID", "PlazaName", "ReportDate"])
                .agg(ATP=("Reader Read Time", "count"), NAP=("IsQualifiedNAP", "sum"))
                .reset_index()
                .sort_values(["ProjectName", "PlazaID", "ReportDate"])
            )
            
            # Save outputs
            project_out = os.path.join(output_dir, project)
            os.makedirs(project_out, exist_ok=True)
            
            transactions_file = os.path.join(project_out, f"{project}_transactions_with_tripcount.csv")
            pdf.to_csv(transactions_file, index=False)
            
            summary_file = os.path.join(project_out, f"{project}_daily_ATP_NAP_plaza.csv")
            daily_summary.to_csv(summary_file, index=False)
            
            results["projects_processed"] += 1
            results["total_transactions"] += len(pdf)
            results["summary_rows"] += len(daily_summary)
            
            add_log(f"Reconciled {project}: {len(pdf)} transactions, {len(daily_summary)} summary rows", "success")
            
        except Exception as e:
            add_log(f"Error reconciling {project}: {str(e)}", "error")
    
    return results, output_dir


def create_download_zip(output_dir):
    """Create a ZIP file of all outputs for download."""
    zip_buffer = BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        for root, _, files in os.walk(output_dir):
            for file in files:
                file_path = os.path.join(root, file)
                arcname = os.path.relpath(file_path, output_dir)
                zip_file.write(file_path, arcname)
    zip_buffer.seek(0)
    return zip_buffer


def render_processing_section():
    """Render the processing section."""
    if not st.session_state.uploaded_files:
        st.info("👆 Please upload files to begin processing")
        return
    
    st.markdown("### 🚀 Process Pipeline")
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        if st.button("▶️ Run Full Pipeline", type="primary", use_container_width=True):
            temp_dir = create_temp_directory()
            st.session_state.processing_log = []
            
            with st.spinner("Processing..."):
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                # Step 1: Save files
                status_text.markdown("**📥 Saving uploaded files...**")
                input_dir = save_uploaded_files(temp_dir, st.session_state.uploaded_files)
                add_log(f"Saved {len(st.session_state.uploaded_files)} files", "success")
                progress_bar.progress(10)
                
                # Step 2: Slicer
                status_text.markdown("**🔪 Step 1: Slicing transactions...**")
                def slicer_progress(pct, msg):
                    progress_bar.progress(int(10 + pct * 30))
                    status_text.markdown(f"**🔪 Slicing:** {msg}")
                
                slicer_results, sliced_dir = run_slicer(input_dir, temp_dir, slicer_progress)
                st.session_state.results['slicer'] = slicer_results
                progress_bar.progress(40)
                
                # Step 3: Merger
                status_text.markdown("**🔗 Step 2: Merging files...**")
                def merger_progress(pct, msg):
                    progress_bar.progress(int(40 + pct * 30))
                    status_text.markdown(f"**🔗 Merging:** {msg}")
                
                merger_results, merged_dir = run_merger(sliced_dir, temp_dir, merger_progress)
                st.session_state.results['merger'] = merger_results
                progress_bar.progress(70)
                
                # Step 4: Reconciler
                status_text.markdown("**📊 Step 3: Reconciling data...**")
                def reconciler_progress(pct, msg):
                    progress_bar.progress(int(70 + pct * 30))
                    status_text.markdown(f"**📊 Reconciling:** {msg}")
                
                reconciler_results, output_dir = run_reconciler(merged_dir, temp_dir, reconciler_progress)
                st.session_state.results['reconciler'] = reconciler_results
                st.session_state.results['output_dir'] = output_dir
                progress_bar.progress(100)
                
                status_text.markdown("**✅ Processing complete!**")
                st.session_state.processing_complete = True
                add_log("Pipeline completed successfully!", "success")
                time.sleep(1)
                st.rerun()
    
    with col2:
        if st.button("🗑️ Clear Results", use_container_width=True):
            cleanup_temp_directory()
            st.session_state.processing_complete = False
            st.session_state.results = {}
            st.session_state.processing_log = []
            st.rerun()


def render_results_section():
    """Render the results section."""
    if not st.session_state.processing_complete:
        return
    
    st.markdown("---")
    st.markdown("### 📈 Results")
    
    results = st.session_state.results
    
    # Metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        slicer = results.get('slicer', {})
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{slicer.get('files_processed', 0)}</div>
            <div class="metric-label">Files Processed</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{slicer.get('rows_extracted', 0):,}</div>
            <div class="metric-label">Rows Extracted</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col3:
        reconciler = results.get('reconciler', {})
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{reconciler.get('total_transactions', 0):,}</div>
            <div class="metric-label">Total Transactions</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col4:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{reconciler.get('projects_processed', 0)}</div>
            <div class="metric-label">Projects</div>
        </div>
        """, unsafe_allow_html=True)
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Download button
    output_dir = results.get('output_dir')
    if output_dir and os.path.exists(output_dir):
        zip_buffer = create_download_zip(output_dir)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        st.download_button(
            label="📥 Download All Results (ZIP)",
            data=zip_buffer,
            file_name=f"reconciliation_results_{timestamp}.zip",
            mime="application/zip",
            use_container_width=True
        )
        
        # Show output files
        st.markdown("#### 📂 Output Files")
        for root, dirs, files in os.walk(output_dir):
            for file in files:
                if file.endswith('.csv'):
                    rel_path = os.path.relpath(os.path.join(root, file), output_dir)
                    st.markdown(f"- 📄 `{rel_path}`")


def render_log_section():
    """Render the processing log."""
    if st.session_state.processing_log:
        with st.expander("📋 Processing Log", expanded=False):
            for entry in st.session_state.processing_log:
                color = {"success": "#10b981", "warning": "#f59e0b", "error": "#ef4444", "info": "#3b82f6"}
                st.markdown(
                    f"<span style='color: {color.get(entry['level'], '#94a3b8')}'>"
                    f"{entry['icon']} [{entry['time']}] {entry['message']}</span>",
                    unsafe_allow_html=True
                )


def main():
    """Main application entry point."""
    initialize_session_state()
    render_header()
    render_sidebar()
    
    # Main content
    render_upload_section()
    st.markdown("---")
    render_processing_section()
    render_results_section()
    render_log_section()


if __name__ == "__main__":
    main()

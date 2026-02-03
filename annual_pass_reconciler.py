"""
Annual Pass Reconciliation Pipeline UI - Database Version
A Streamlit-based interface for processing toll plaza FASTag ANNUAL PASS transactions
directly from Amazon Redshift database.
"""

import streamlit as st
import pandas as pd
import polars as pl
import os
import sys
import tempfile
import zipfile
from datetime import datetime, timedelta
from io import BytesIO
import time
import warnings

# Import database configuration
from db_config import (
    get_connection,
    test_connection,
    BANK_PLAZA_MAP,
    get_plazas_by_bank,
    get_projects_by_bank,
    get_plazas_by_project,
    resolve_plaza,
    build_query,
    get_column_map,
    REDSHIFT_CONFIG,
)

# Page configuration
st.set_page_config(
    page_title="Annual Pass Reconciler (DB)",
    page_icon="🗄️",
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
    
    /* Expander styling */
    .streamlit-expanderHeader {
        background: rgba(255, 255, 255, 0.8);
        border-radius: 8px;
        color: #2d3748 !important;
    }
    
    /* Connection status */
    .connection-ok {
        color: #48bb78;
        font-weight: 600;
    }
    
    .connection-fail {
        color: #f56565;
        font-weight: 600;
    }
</style>
""", unsafe_allow_html=True)


def initialize_session_state():
    """Initialize session state variables."""
    if 'processing_complete' not in st.session_state:
        st.session_state.processing_complete = False
    if 'results' not in st.session_state:
        st.session_state.results = {}
    if 'temp_dir' not in st.session_state:
        st.session_state.temp_dir = None
    if 'processing_log' not in st.session_state:
        st.session_state.processing_log = []
    if 'db_connected' not in st.session_state:
        st.session_state.db_connected = False
    if 'fetched_data' not in st.session_state:
        st.session_state.fetched_data = None


def create_temp_directory():
    """Create a temporary directory for processing."""
    if st.session_state.temp_dir is None or not os.path.exists(st.session_state.temp_dir):
        st.session_state.temp_dir = tempfile.mkdtemp(prefix="reconciliation_db_")
    return st.session_state.temp_dir


def cleanup_temp_directory():
    """Clean up temporary directory."""
    import shutil
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
    st.markdown('<h1 class="main-header">🗄️ Annual Pass Reconciler (DB)</h1>', unsafe_allow_html=True)
    st.markdown('<p class="sub-header">Process FASTag ANNUAL PASS transactions directly from Redshift database</p>', unsafe_allow_html=True)


def render_sidebar():
    """Render the sidebar with information."""
    with st.sidebar:
        st.markdown("### 📊 Pipeline Overview")
        st.markdown("""
        This tool processes toll transactions through three stages:
        
        **1️⃣ Data Fetcher**  
        Queries ANNUALPASS transactions from Redshift
        
        **2️⃣ Data Consolidator**  
        Groups data by project/plaza
        
        **3️⃣ Reconciler**  
        Calculates TripCount and generates ATP/NAP summaries
        """)
        
        st.divider()
        
        # Database connection status
        st.markdown("### 🔌 Database Status")
        if REDSHIFT_CONFIG["host"]:
            connected, msg = test_connection()
            if connected:
                st.markdown('<span class="connection-ok">✅ Connected</span>', unsafe_allow_html=True)
                st.session_state.db_connected = True
            else:
                st.markdown('<span class="connection-fail">❌ Connection Failed</span>', unsafe_allow_html=True)
                st.caption(f"Error: {msg[:50]}...")
                st.session_state.db_connected = False
        else:
            st.markdown('<span class="connection-fail">⚠️ Not Configured</span>', unsafe_allow_html=True)
            st.caption("Set credentials in .env file")
            st.session_state.db_connected = False
        
        st.divider()
        
        st.markdown("### 🏢 Supported Banks")
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("**ICICI Bank**")
            st.caption(f"{len(BANK_PLAZA_MAP.get('ICICI', {}))} Plazas")
        with col2:
            st.markdown("**IDFC Bank**")
            st.caption(f"{len(BANK_PLAZA_MAP.get('IDFC', {}))} Plazas")
        
        st.divider()
        
        if st.button("🗑️ Clear Session", width='stretch'):
            cleanup_temp_directory()
            for key in list(st.session_state.keys()):
                del st.session_state[key]
            st.rerun()


def render_query_section():
    """Render the query configuration section."""
    st.markdown("### 🔍 Query Configuration")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Bank selection
        bank = st.selectbox(
            "Select Bank",
            options=["IDFC", "ICICI"],
            help="Choose the bank to query transactions from"
        )
        
        # Project selection (filtered by bank)
        projects = get_projects_by_bank(bank)
        selected_project = st.selectbox(
            "Select Project",
            options=["All Projects"] + projects,
            help="Filter by project or select all"
        )
    
    with col2:
        # Date range selection
        today = datetime.now().date()
        default_start = today - timedelta(days=7)
        
        start_date = st.date_input(
            "Start Date",
            value=default_start,
            help="Query transactions from this date"
        )
        
        end_date = st.date_input(
            "End Date",
            value=today,
            help="Query transactions up to this date"
        )
    
    # Plaza selection (filtered by project if selected)
    st.markdown("#### 📍 Select Plazas")
    
    available_plazas = get_plazas_by_bank(bank)
    if selected_project != "All Projects":
        available_plazas = {
            pid: info for pid, info in available_plazas.items()
            if info[1] == selected_project
        }
    
    plaza_options = {f"{pid} - {info[0]} ({info[1]})": pid for pid, info in available_plazas.items()}
    
    selected_plaza_labels = st.multiselect(
        "Select Plaza(s)",
        options=list(plaza_options.keys()),
        default=list(plaza_options.keys())[:1] if plaza_options else [],
        help="Select one or more plazas to query"
    )
    
    selected_plaza_ids = [plaza_options[label] for label in selected_plaza_labels]
    
    # Show query preview
    if selected_plaza_ids:
        with st.expander("🔎 Query Preview", expanded=False):
            query = build_query(
                bank,
                selected_plaza_ids,
                start_date.strftime("%Y-%m-%d 00:00:00"),
                end_date.strftime("%Y-%m-%d 23:59:59"),
                limit=50  # Preview limit
            )
            st.code(query, language="sql")
    
    return {
        "bank": bank,
        "project": selected_project,
        "plaza_ids": selected_plaza_ids,
        "start_date": start_date,
        "end_date": end_date
    }


def fetch_data_from_db(bank, plaza_ids, start_date, end_date, progress_callback=None):
    """Fetch ANNUALPASS transactions from database."""
    if progress_callback:
        progress_callback(0.1, "Connecting to database...")
    
    results = {"rows_fetched": 0, "plazas_queried": len(plaza_ids)}
    
    try:
        conn = get_connection()
        cursor = conn.cursor()
        
        if progress_callback:
            progress_callback(0.3, "Executing query...")
        
        query = build_query(
            bank,
            plaza_ids,
            start_date.strftime("%Y-%m-%d 00:00:00"),
            end_date.strftime("%Y-%m-%d 23:59:59")
        )
        
        cursor.execute(query)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        
        if progress_callback:
            progress_callback(0.7, "Processing results...")
        
        df = pd.DataFrame(rows, columns=columns)
        
        # Rename columns to standard names
        column_map = get_column_map(bank)
        df = df.rename(columns=column_map)
        
        cursor.close()
        conn.close()
        
        results["rows_fetched"] = len(df)
        
        if progress_callback:
            progress_callback(1.0, "Data fetched successfully!")
        
        add_log(f"Fetched {len(df)} ANNUALPASS transactions from {len(plaza_ids)} plaza(s)", "success")
        
        return df, results
        
    except Exception as e:
        add_log(f"Database error: {str(e)}", "error")
        raise


def consolidate_data(df, progress_callback=None):
    """Consolidate and group data by project/plaza."""
    if progress_callback:
        progress_callback(0.2, "Grouping by project/plaza...")
    
    results = {"projects": 0, "plazas": 0}
    
    # Add metadata columns
    if "PlazaID" in df.columns:
        df["PlazaID"] = df["PlazaID"].astype(str).str.strip().str.zfill(6)
        
        # Resolve plaza metadata
        plaza_meta = df["PlazaID"].apply(resolve_plaza)
        df["Bank"] = plaza_meta.apply(lambda x: x[0])
        df["PlazaName"] = plaza_meta.apply(lambda x: x[1])
        df["ProjectName"] = plaza_meta.apply(lambda x: x[2])
        
        results["projects"] = df["ProjectName"].nunique()
        results["plazas"] = df["PlazaID"].nunique()
    
    # Parse datetime
    if "Reader Read Time" in df.columns:
        df["Reader Read Time"] = pd.to_datetime(df["Reader Read Time"], errors='coerce')
        
        # Add SourceMonth column
        df["SourceMonth"] = df["Reader Read Time"].dt.strftime("%b-%y")
    
    if progress_callback:
        progress_callback(1.0, "Data consolidated!")
    
    add_log(f"Consolidated data: {results['projects']} projects, {results['plazas']} plazas", "success")
    
    return df, results


def run_reconciler(df, temp_dir, progress_callback=None):
    """Run the reconciler step - calculate TripCount and ATP/NAP."""
    output_dir = os.path.join(temp_dir, "RECONCILIATION_OUTPUT")
    os.makedirs(output_dir, exist_ok=True)
    
    results = {"projects_processed": 0, "total_transactions": 0, "summary_rows": 0}
    
    if df.empty:
        add_log("No data to reconcile", "warning")
        return results, output_dir
    
    # Group by project
    projects = df["ProjectName"].unique()
    total_projects = len(projects)
    
    for proj_idx, project in enumerate(projects):
        if progress_callback:
            progress_callback(proj_idx / total_projects, f"Reconciling project {project}...")
        
        if project is None:
            continue
        
        try:
            pdf = df[df["ProjectName"] == project].copy()
            
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


def render_processing_section(query_config):
    """Render the processing section."""
    if not query_config["plaza_ids"]:
        st.info("👆 Please select at least one plaza to begin processing")
        return
    
    st.markdown("### 🚀 Process Pipeline")
    
    col1, col2 = st.columns([1, 1])
    
    with col1:
        run_disabled = not st.session_state.db_connected
        if st.button("▶️ Run Full Pipeline", type="primary", width='stretch', disabled=run_disabled):
            temp_dir = create_temp_directory()
            st.session_state.processing_log = []
            
            with st.spinner("Processing..."):
                progress_bar = st.progress(0)
                status_text = st.empty()
                
                try:
                    # Step 1: Fetch Data
                    status_text.markdown("**🔍 Step 1: Fetching data from database...**")
                    def fetch_progress(pct, msg):
                        progress_bar.progress(int(pct * 30))
                        status_text.markdown(f"**🔍 Fetching:** {msg}")
                    
                    df, fetch_results = fetch_data_from_db(
                        query_config["bank"],
                        query_config["plaza_ids"],
                        query_config["start_date"],
                        query_config["end_date"],
                        fetch_progress
                    )
                    st.session_state.results['fetcher'] = fetch_results
                    st.session_state.fetched_data = df
                    progress_bar.progress(30)
                    
                    # Step 2: Consolidate
                    status_text.markdown("**🔗 Step 2: Consolidating data...**")
                    def consolidate_progress(pct, msg):
                        progress_bar.progress(int(30 + pct * 20))
                        status_text.markdown(f"**🔗 Consolidating:** {msg}")
                    
                    df, consolidate_results = consolidate_data(df, consolidate_progress)
                    st.session_state.results['consolidator'] = consolidate_results
                    progress_bar.progress(50)
                    
                    # Step 3: Reconcile
                    status_text.markdown("**📊 Step 3: Reconciling data...**")
                    def reconciler_progress(pct, msg):
                        progress_bar.progress(int(50 + pct * 50))
                        status_text.markdown(f"**📊 Reconciling:** {msg}")
                    
                    reconciler_results, output_dir = run_reconciler(df, temp_dir, reconciler_progress)
                    st.session_state.results['reconciler'] = reconciler_results
                    st.session_state.results['output_dir'] = output_dir
                    progress_bar.progress(100)
                    
                    status_text.markdown("**✅ Processing complete!**")
                    st.session_state.processing_complete = True
                    add_log("Pipeline completed successfully!", "success")
                    time.sleep(1)
                    st.rerun()
                    
                except Exception as e:
                    status_text.markdown(f"**❌ Error:** {str(e)}")
                    add_log(f"Pipeline failed: {str(e)}", "error")
        
        if run_disabled:
            st.caption("⚠️ Database connection required")
    
    with col2:
        if st.button("🗑️ Clear Results", width='stretch'):
            cleanup_temp_directory()
            st.session_state.processing_complete = False
            st.session_state.results = {}
            st.session_state.processing_log = []
            st.session_state.fetched_data = None
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
        fetcher = results.get('fetcher', {})
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{fetcher.get('plazas_queried', 0)}</div>
            <div class="metric-label">Plazas Queried</div>
        </div>
        """, unsafe_allow_html=True)
    
    with col2:
        st.markdown(f"""
        <div class="metric-card">
            <div class="metric-value">{fetcher.get('rows_fetched', 0):,}</div>
            <div class="metric-label">Rows Fetched</div>
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
    
    # Data Preview
    if st.session_state.fetched_data is not None and not st.session_state.fetched_data.empty:
        with st.expander("📋 Data Preview (First 100 rows)", expanded=False):
            st.dataframe(st.session_state.fetched_data.head(100), width='stretch')
    
    # Download button
    output_dir = results.get('output_dir')
    if output_dir and os.path.exists(output_dir):
        zip_buffer = create_download_zip(output_dir)
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        st.download_button(
            label="📥 Download All Results (ZIP)",
            data=zip_buffer,
            file_name=f"reconciliation_db_results_{timestamp}.zip",
            mime="application/zip",
            width='stretch'
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
    query_config = render_query_section()
    st.markdown("---")
    render_processing_section(query_config)
    render_results_section()
    render_log_section()


if __name__ == "__main__":
    main()

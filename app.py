"""
Annual Pass Reconciler - Main Application Entry Point
Unified MVC application supporting both Database and File Upload modes.
"""

import streamlit as st
import pandas as pd
from datetime import datetime
import os
import sys

# Page config must be first
st.set_page_config(
    page_title="Annual Pass Reconciler",
    page_icon="🚗",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Apply custom styles
from views.styles import apply_custom_css
apply_custom_css()

# MVC Components
from views.ui_components import UIComponents
from config.plaza_config import get_all_projects, get_plazas_by_project, get_plaza_info
from controllers.data_fetcher import DataFetcher
from controllers.data_consolidator import consolidate_data
from controllers.reconciler_controller import ReconcilerController


def init_session_state():
    """Initialize session state variables."""
    if 'processing_log' not in st.session_state:
        st.session_state.processing_log = []
    if 'results' not in st.session_state:
        st.session_state.results = None
    if 'reconciliation_complete' not in st.session_state:
        st.session_state.reconciliation_complete = False
    if 'data_source' not in st.session_state:
        st.session_state.data_source = 'database'
    if 'transactions_df' not in st.session_state:
        st.session_state.transactions_df = None
    if 'summary_df' not in st.session_state:
        st.session_state.summary_df = None


def main():
    """Main application loop."""
    init_session_state()
    
    # 1. Header
    UIComponents.render_header(
        "Annual Pass Reconciler",
        "Automated reconciliation pipeline for IDFC & ICICI FASTag transactions"
    )
    
    # 2. Sidebar Navigation
    with st.sidebar:
        st.image("https://img.icons8.com/color/96/000000/toll-booth.png", width=80)
        
        # Tabs for main modes
        mode = st.radio(
            "Mode", 
            ["🚀 Run Pipeline", "📊 View History"],
            label_visibility="collapsed"
        )
        
        # Separation
        st.markdown("---")
    
    # Mode 1: Run Pipeline
    if mode == "🚀 Run Pipeline":
        run_pipeline_view()
    
    # Mode 2: View History
    else:
        view_history_view()
    
    # Sidebar Footer
    with st.sidebar:
        UIComponents.render_sidebar_info()


def run_pipeline_view():
    """Render the main pipeline execution view."""
    
    # 1. Data Source Selection
    source_type = UIComponents.render_data_source_toggle()
    st.session_state.data_source = source_type
    
    params = {}
    ready_to_run = False
    
    # 2. Configure Source
    if source_type == 'database':
        # Database Mode
        projects = get_all_projects()
        
        # Build plaza dict for multiselect
        all_plazas = {}
        # This is simplified; in production we'd filter based on bank/project selection
        # For now, let's just get all plazas
        # Note: UIComponents.render_database_selectors expects a dict of id:name
        # We need to construct this from our config
        from config.plaza_config import BANK_PLAZA_MAP
        for bank_data in BANK_PLAZA_MAP.values():
            for pid, info in bank_data.items():
                all_plazas[pid] = info['plaza']
        
        db_params = UIComponents.render_database_selectors(projects, all_plazas)
        
        params = db_params
        ready_to_run = bool(db_params['plaza_ids'] and db_params['start_date'] and db_params['end_date'])
        
    else:
        # File Upload Mode
        uploaded_files = UIComponents.render_file_uploader()
        
        # Optional bank override for files
        st.sidebar.markdown("**Options**")
        bank_override = st.sidebar.selectbox("Force Bank Format", ["Auto-detect", "IDFC", "ICICI"])
        
        params['uploaded_files'] = uploaded_files
        params['bank'] = None if bank_override == "Auto-detect" else bank_override
        ready_to_run = bool(uploaded_files)
    
    # 3. Main Action Area
    col1, col2 = st.columns([2, 1])
    
    with col1:
        st.markdown("### ⚙️ Pipeline Status")
        
        # Run Button
        if UIComponents.render_run_button("▶️ Start Reconciliation", disabled=not ready_to_run):
            execute_reconciliation(source_type, params)
        
        # Display logs
        if st.session_state.processing_log:
            UIComponents.render_log_viewer(st.session_state.processing_log)
    
    with col2:
        st.markdown("### 📈 Live Metrics")
        
        if st.session_state.results:
            metrics = {
                "Total Transactions": st.session_state.results.get('total_transactions', 0),
                "Total NAP": st.session_state.results.get('total_nap', 0),
                "ATP Count": st.session_state.results.get('total_atp', 0)
            }
            UIComponents.render_metrics(metrics)
        else:
            st.info("Run pipeline to see metrics")
    
    # 4. Results Display
    if st.session_state.reconciliation_complete:
        st.markdown("---")
        st.markdown("### 🏁 Reconciliation Results")
        
        # Summary Table
        if st.session_state.summary_df is not None:
            UIComponents.render_results_table(st.session_state.summary_df, "Daily Summary (ATP/NAP)")
        
        # Transactions Preview
        if st.session_state.transactions_df is not None:
            UIComponents.render_results_table(st.session_state.transactions_df.head(100), "Transaction Details (Preview)")
        
        # Download Button
        if st.session_state.transactions_df is not None:
            # Create ZIP in memory
            import io
            import zipfile
            
            zip_buffer = io.BytesIO()
            with zipfile.ZipFile(zip_buffer, "w") as zf:
                # Add transactions CSV
                zf.writestr(
                    "transactions_with_tripcount.csv",
                    st.session_state.transactions_df.to_csv(index=False)
                )
                # Add summary CSV
                if st.session_state.summary_df is not None:
                    zf.writestr(
                        "daily_summary_atp_nap.csv",
                        st.session_state.summary_df.to_csv(index=False)
                    )
            
            UIComponents.render_download_button(
                zip_buffer.getvalue(),
                f"reconciliation_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
            )


def execute_reconciliation(source_type: str, params: dict):
    """Execute the full reconciliation process."""
    st.session_state.processing_log = []
    st.session_state.reconciliation_complete = False
    st.session_state.results = None
    
    progress_bar = st.progress(0, text="Initializing...")
    
    def update_progress(p, text):
        progress_bar.progress(p, text=text)
        st.session_state.processing_log.append({'message': text, 'type': 'info'})
    
    try:
        # 1. Fetch Data
        update_progress(0.1, "Fetching data...")
        with DataFetcher(source_type) as fetcher:
            df = fetcher.fetch(params)
        
        if df.empty:
            update_progress(1.0, "No data found matching criteria.")
            st.error("No data found!")
            return
            
        update_progress(0.3, f"Fetched {len(df)} transactions. Consolidating...")
        
        # 2. Consolidate Data
        # Ensure we have metadata for consolidation
        meta = {
            'bank': params.get('bank'),
            'project': params.get('project')
        }
        df_consolidated = consolidate_data(df, meta)
        
        update_progress(0.5, "Running reconciliation logic...")
        
        # 3. Reconcile & Persist
        # Create metadata for the run
        run_meta = {
            'data_source': source_type,
            'bank': params.get('bank'),
            'project': params.get('project', 'Multiple'),
            'plaza_ids': params.get('plaza_ids', []),
            'start_date': str(params.get('start_date')),
            'end_date': str(params.get('end_date')),
            'created_by': 'Streamlit User'
        }
        
        controller = ReconcilerController(save_to_db=True)
        try:
            transactions_df, summary_df, results = controller.run_pipeline(
                df_consolidated,
                run_meta,
                progress_callback=update_progress
            )
            
            # Store results in session
            st.session_state.transactions_df = transactions_df
            st.session_state.summary_df = summary_df
            st.session_state.results = results
            st.session_state.reconciliation_complete = True
            
            st.success("Reconciliation completed successfully!")
            
        except Exception as e:
            st.error(f"Reconciliation failed: {str(e)}")
            st.session_state.processing_log.append({'message': f"Error: {str(e)}", 'type': 'error'})
        finally:
            controller.cleanup()
            
    except Exception as e:
        st.error(f"Pipeline error: {str(e)}")
        st.session_state.processing_log.append({'message': f"Pipeline error: {str(e)}", 'type': 'error'})


def view_history_view():
    """Render history viewing interface."""
    st.markdown("### 📊 Historical Reports")
    
    # Initialize controller for DB access
    controller = ReconcilerController(save_to_db=True)
    
    try:
        # filters = UIComponents.render_history_filters()
        # For MVP, simpler filters
        col1, col2 = st.columns(2)
        with col1:
             bank = st.selectbox("Filter Bank", ["All", "IDFC", "ICICI"])
        
        filters = {}
        if bank != "All":
            filters['bank'] = bank
            
        # Get history
        history_df = controller.get_run_history(filters)
        
        # Render viewer using component
        run_id = UIComponents.render_history_viewer(history_df)
        
        if run_id:
            # Load details
            with st.spinner("Loading run details..."):
                details = controller.get_run_details(run_id)
                
                # Show Metadata
                meta = details['metadata'].iloc[0] if not details['metadata'].empty else {}
                st.markdown(f"**Run Date:** {meta.get('run_date')} | **User:** {meta.get('created_by')}")
                
                # Show Summary
                if not details['summary'].empty:
                    st.markdown("#### 📅 Daily Summary")
                    st.dataframe(details['summary'], use_container_width=True)
                
                # Show Transactions
                if not details['transactions'].empty:
                    st.markdown("#### 🚗 Transactions")
                    st.dataframe(details['transactions'].head(1000), use_container_width=True)
                    st.caption("Showing first 1000 rows")
                    
                    # Download CSV
                    csv = details['transactions'].to_csv(index=False)
                    st.download_button(
                        "📥 Download Full CSV",
                        data=csv,
                        file_name=f"transactions_{run_id[:8]}.csv",
                        mime="text/csv"
                    )
                    
    except Exception as e:
        st.error(f"Failed to load history: {str(e)}")
    finally:
        controller.cleanup()


if __name__ == "__main__":
    main()

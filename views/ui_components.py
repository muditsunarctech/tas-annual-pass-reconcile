"""
UI Components Module - Reusable Streamlit UI Components
Contains all UI rendering logic for the Annual Pass Reconciler application.
"""

import streamlit as st
import pandas as pd
from datetime import datetime, date
from typing import List, Optional, Dict, Tuple


class UIComponents:
    """Collection of reusable UI components for the application."""
    
    @staticmethod
    def render_header(title: str, subtitle: str = None):
        """
        Render application header.
        
        Args:
            title: Main title
            subtitle: Optional subtitle
        """
        st.markdown(f'<div class="main-header">{title}</div>', unsafe_allow_html=True)
        if subtitle:
            st.markdown(f'<div class="sub-header">{subtitle}</div>', unsafe_allow_html=True)
    
    @staticmethod
    def render_data_source_toggle() -> str:
        """
        Render data source selection toggle.
        
        Returns:
            str: Selected data source ('database' or 'file_upload')
        """
        st.sidebar.markdown("### 📊 Data Source")
        
        source = st.sidebar.radio(
            "Select data source",
            options=["Database (Redshift)", "File Upload (Excel/CSV)"],
            label_visibility="collapsed",
            horizontal=False
        )
        
        if source == "Database (Redshift)":
            return "database"
        else:
            return "file_upload"
    
    @staticmethod
    def render_database_selectors(projects: List[str], plazas: Dict[str, str]) -> Dict:
        """
        Render database query selectors (bank, project, plaza, dates).
        
        Args:
            projects: List of available projects
            plazas: Dictionary of plaza_id: plaza_name
        
        Returns:
            Dict with selected values
        """
        st.sidebar.markdown("---")
        st.sidebar.markdown("### 🏦 Database Query Settings")
        
        # Bank selector
        bank = st.sidebar.selectbox(
            "Bank",
            options=["IDFC", "ICICI"]
        )
        
        # Project selector
        project = st.sidebar.selectbox(
            "Project",
            options=["All"] + projects
        )
        
        # Plaza selector
        plaza_ids = st.sidebar.multiselect(
            "Plaza(s)",
            options=list(plazas.keys()),
            format_func=lambda x: f"{x} - {plazas[x]}"
        )
        
        # Date range
        st.sidebar.markdown("**Date Range**")
        col1, col2 = st.sidebar.columns(2)
        with col1:
            start_date = st.date_input(
                "Start",
                value=date.today().replace(day=1)
            )
        with col2:
            end_date = st.date_input(
                "End",
                value=date.today()
            )
        
        return {
            "bank": bank,
            "project": project,
            "plaza_ids": plaza_ids,
            "start_date": start_date,
            "end_date": end_date
        }
    
    @staticmethod
    def render_file_uploader() -> List:
        """
        Render file upload widget.
        
        Returns:
            List of uploaded files
        """
        st.sidebar.markdown("---")
        st.sidebar.markdown("### 📁 File Upload")
        
        uploaded_files = st.sidebar.file_uploader(
            "Upload CSV or Excel files",
            type=["csv", "xlsx", "xls", "xlsb"],
            accept_multiple_files=True
        )
        
        return uploaded_files or []
    
    @staticmethod
    def render_run_button(text: str = "▶️ Run Reconciliation", disabled: bool = False) -> bool:
        """
        Render main run button.
        
        Args:
            text: Button text
            disabled: Whether button is disabled
        
        Returns:
            bool: True if button clicked
        """
        return st.button(text, type="primary", disabled=disabled)
    
    @staticmethod
    def render_progress(progress: float, text: str):
        """
        Render progress bar with text.
        
        Args:
            progress: Progress value 0.0-1.0
            text: Progress text
        """
        st.progress(progress, text=text)
    
    @staticmethod
    def render_metrics(metrics: Dict[str, int]):
        """
        Render metric cards.
        
        Args:
            metrics: Dictionary of label: value
        """
        cols = st.columns(len(metrics))
        for idx, (label, value) in enumerate(metrics.items()):
            with cols[idx]:
                st.metric(label=label, value=f"{value:,}")
    
    @staticmethod
    def render_results_table(df: pd.DataFrame, title: str = "Results"):
        """
        Render results table in expander.
        
        Args:
            df: DataFrame to display
            title: Expander title
        """
        with st.expander(f"📋 {title}", expanded=False):
            st.dataframe(df, use_container_width=True)
    
    @staticmethod
    def render_download_button(data: bytes, filename: str, label: str = "📥 Download Results"):
        """
        Render download button.
        
        Args:
            data: File data as bytes
            filename: Download filename
            label: Button label
        """
        st.download_button(
            label=label,
            data=data,
            file_name=filename,
            mime="application/zip"
        )
    
    @staticmethod
    def render_history_viewer(history_df: pd.DataFrame) -> Optional[str]:
        """
        Render history table with run selector.
        
        Args:
            history_df: DataFrame of past runs
        
        Returns:
            Selected run_id or None
        """
        st.markdown("### 📊 Past Reconciliation Runs")
        
        if history_df.empty:
            st.info("No reconciliation history found.")
            return None
        
        # Display table
        st.dataframe(
            history_df[[
                'run_id', 'run_date', 'bank', 'project', 
                'total_transactions', 'total_nap', 'status'
            ]],
            use_container_width=True
        )
        
        # Run selector
        run_id = st.selectbox(
            "Select a run to view details",
            options=history_df['run_id'].tolist(),
            format_func=lambda x: f"{x[:8]}... - {history_df[history_df['run_id']==x]['project'].iloc[0]}"
        )
        
        return run_id
    
    @staticmethod
    def render_info_box(message: str, box_type: str = "info"):
        """
        Render styled info box.
        
        Args:
            message: Message text
            box_type: 'info', 'warning', or 'error'
        """
        box_class = f"{box_type}-box"
        st.markdown(
            f'<div class="{box_class}">{message}</div>',
            unsafe_allow_html=True
        )
    
    @staticmethod
    def render_status_badge(status: str, text: str):
        """
        Render status badge.
        
        Args:
            status: 'success', 'pending', or 'error'
            text: Badge text
        """
        status_class = f"status-{status}"
        st.markdown(
            f'<span class="{status_class}">{text}</span>',
            unsafe_allow_html=True
        )
    
    @staticmethod
    def render_sidebar_info():
        """Render sidebar information section."""
        st.sidebar.markdown("---")
        st.sidebar.markdown("### ℹ️ About")
        st.sidebar.info(
            """
            **Annual Pass Reconciler**
            
            Automates reconciliation of toll plaza 
            ANNUALPASS transactions.
            
            - Calculate TripCount
            - Determine ATP/NAP
            - Generate reports
            """
        )
    
    @staticmethod
    def render_connection_status(db_type: str, is_connected: bool):
        """
        Render database connection status.
        
        Args:
            db_type: Database type (e.g., 'Redshift', 'MySQL')
            is_connected: Connection status
        """
        status = "🟢 Connected" if is_connected else "🔴 Disconnected"
        color = "success" if is_connected else "error"
        
        st.sidebar.markdown(f"**{db_type}:** {status}")
    
    @staticmethod
    def render_log_viewer(logs: List[Dict]):
        """
        Render processing log viewer.
        
        Args:
            logs: List of log entries with 'message' and 'type' keys
        """
        with st.expander("📝 Processing Log", expanded=False):
            for log in logs:
                emoji = {
                    'info': 'ℹ️',
                    'success': '✅',
                    'warning': '⚠️',
                    'error': '❌'
                }.get(log.get('type', 'info'), 'ℹ️')
                
                st.write(f"{emoji} {log['message']}")
    
    @staticmethod
    def render_history_filters() -> Dict:
        """
        Render history filter controls.
        
        Returns:
            Dict with filter values
        """
        col1, col2, col3 = st.columns(3)
        
        with col1:
            bank_filter = st.selectbox("Bank", ["All", "IDFC", "ICICI"])
        
        with col2:
            project_filter = st.selectbox("Project", ["All"])  # Populated dynamically
        
        with col3:
            date_range = st.date_input("Date Range", value=[])
        
        return {
            "bank": bank_filter if bank_filter != "All" else None,
            "project": project_filter if project_filter != "All" else None,
            "date_range": date_range
        }


# Convenience functions
def render_header(title: str, subtitle: str = None):
    """Render header."""
    UIComponents.render_header(title, subtitle)


def render_data_source_toggle() -> str:
    """Render data source toggle."""
    return UIComponents.render_data_source_toggle()


def render_metrics(metrics: Dict[str, int]):
    """Render metrics."""
    UIComponents.render_metrics(metrics)

"""
Database Configuration for Annual Pass Reconciliation
Supports both IDFC and ICICI bank data from Amazon Redshift
"""

import os
from typing import Optional, Dict, Any, Tuple
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# ============================================================================
# REDSHIFT CONNECTION CONFIGURATION
# ============================================================================

REDSHIFT_CONFIG = {
    "host": os.getenv("REDSHIFT_HOST", "").strip().rstrip(",").strip('"'),
    "port": int(os.getenv("REDSHIFT_PORT", "5439").strip().rstrip(",").strip('"')),
    "database": os.getenv("REDSHIFT_DATABASE", "").strip().rstrip(",").strip('"'),
    "user": os.getenv("REDSHIFT_USER", "").strip().rstrip(",").strip('"'),
    "password": os.getenv("REDSHIFT_PASSWORD", "").strip().rstrip(",").strip('"'),
}



def get_connection():
    """
    Create and return a Redshift database connection.
    Uses redshift_connector for native Redshift support.
    """
    try:
        import redshift_connector
        conn = redshift_connector.connect(
            host=REDSHIFT_CONFIG["host"],
            port=REDSHIFT_CONFIG["port"],
            database=REDSHIFT_CONFIG["database"],
            user=REDSHIFT_CONFIG["user"],
            password=REDSHIFT_CONFIG["password"],
        )
        return conn
    except ImportError:
        # Fallback to psycopg2 if redshift_connector not available
        import psycopg2
        conn = psycopg2.connect(
            host=REDSHIFT_CONFIG["host"],
            port=REDSHIFT_CONFIG["port"],
            dbname=REDSHIFT_CONFIG["database"],
            user=REDSHIFT_CONFIG["user"],
            password=REDSHIFT_CONFIG["password"],
        )
        return conn


def test_connection() -> Tuple[bool, str]:
    """Test database connection and return status."""
    try:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        conn.close()
        return True, "Connection successful"
    except Exception as e:
        return False, str(e)


# ============================================================================
# BANK CONFIGURATION
# ============================================================================

# Bank to table mapping
BANK_TABLE_MAP = {
    "IDFC": {
        "schema": "ods_fastag",
        "table": "idfc_transaction_api",
        "type": "single"  # Single combined table
    },
    "ICICI": {
        "schema": "ods_fastag",
        "acquirer_table": "acquirer_transaction_information",
        "concessionaire_table": "concessionaire_transaction_information",
        "type": "dual"  # Two separate tables to join
    }
}

# ============================================================================
# PLAZA CONFIGURATION
# ============================================================================

BANK_PLAZA_MAP = {
    "IDFC": {
        "142001": ("Ghoti", "IHPL"),
        "142002": ("Arjunali", "IHPL"),
        "220001": ("Raipur", "BPPTPL"),
        "220002": ("Indranagar", "BPPTPL"),
        "220003": ("Birami", "BPPTPL"),
        "220004": ("Uthman", "BPPTPL"),
        "235001": ("Mandawada", "SUTPL"),
        "235002": ("Negadiya", "SUTPL"),
        "243000": ("Rupakheda", "BRTPL"),
        "243001": ("Mujras", "BRTPL"),
        "073001": ("Bollapalli", "SEL"),
        "073002": ("Tangutur", "SEL"),
        "073003": ("Musunur", "SEL")
    },
    "ICICI": {
        "540030": ("Ladgaon", "CSJTPL"),
        "540032": ("Nagewadi", "CSJTPL"),
        "120001": ("Shanthigrama", "DHTPL"),
        "120002": ("Kadabahalli", "DHTPL"),
        "139001": ("Shirpur", "DPTL"),
        "139002": ("Songir", "DPTL"),
        "167001": ("Vaniyambadi", "KWTPL"),
        "167002": ("Pallikonda", "KWTPL"),
        "169001": ("Palayam", "KTTRL"),
        "234002": ("Chagalamarri", "REPL"),
        "352001": ("Nannur", "REPL"),
        "352013": ("Chapirevula", "REPL"),
        "352065": ("Patimeedapalli", "REPL"),
        "045001": ("Gudur", "HYTPL"),
        "046001": ("Kasaba", "BHTPL"),
        "046002": ("Nagarhalla", "BHTPL"),
        "079001": ("Shakapur", "WATL")
    }
}


def get_plazas_by_bank(bank: str) -> Dict[str, Tuple[str, str]]:
    """Get all plazas for a given bank."""
    return BANK_PLAZA_MAP.get(bank, {})


def get_plazas_by_project(bank: str, project: str) -> Dict[str, str]:
    """Get plaza IDs and names for a given bank and project."""
    plazas = BANK_PLAZA_MAP.get(bank, {})
    return {
        plaza_id: plaza_name
        for plaza_id, (plaza_name, proj) in plazas.items()
        if proj == project
    }


def get_projects_by_bank(bank: str) -> list:
    """Get unique project names for a given bank."""
    plazas = BANK_PLAZA_MAP.get(bank, {})
    return sorted(list(set(proj for _, (_, proj) in plazas.items())))


def resolve_plaza(plaza_id: str) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """
    Resolve plaza ID to bank, plaza name, and project name.
    Returns (bank, plaza_name, project_name) or (None, None, None) if not found.
    """
    plaza_id = str(plaza_id).strip().zfill(6)
    for bank, plazas in BANK_PLAZA_MAP.items():
        if plaza_id in plazas:
            plaza_name, project_name = plazas[plaza_id]
            return bank, plaza_name, project_name
    return None, None, None


# ============================================================================
# COLUMN MAPPING
# ============================================================================

# IDFC column mapping (single table)
IDFC_COLUMN_MAP = {
    "conc_plaza_id": "PlazaID",
    "conc_vrn_no": "Vehicle Reg. No.",
    "conc_tag_id": "Tag ID",
    "conc_txn_dt_processed": "Reader Read Time",
    "acq_txn_desc": "TripType",
    "acq_txn_reason": "ReasonCode",
}

# ICICI column mapping (from acquirer table)
ICICI_ACQ_COLUMN_MAP = {
    "ihmclplazacode": "PlazaID",
    "vrn": "Vehicle Reg. No.",
    "tagid": "Tag ID",
    "acqtxndateprocessed": "Reader Read Time",
    "triptype": "TripType",
    "acqtxnreason": "ReasonCode",
}

# ICICI column mapping (from concessionaire table - used if needed)
ICICI_CONC_COLUMN_MAP = {
    "ihmclplazacode": "PlazaID",
    "concvrn": "Vehicle Reg. No.",
    "conctagid": "Tag ID",
    "conctxndateprocessed": "Reader Read Time",
}


# ============================================================================
# SQL QUERY BUILDERS
# ============================================================================

def build_idfc_query(plaza_ids: list, start_date: str, end_date: str, limit: Optional[int] = None) -> str:
    """
    Build SQL query for IDFC bank transactions.
    Filters for ANNUALPASS transactions directly in SQL.
    Uses ROW_NUMBER() to deduplicate by conc_txn_id, keeping only the latest batch.
    """
    plaza_list = "', '".join(plaza_ids)
    query = f"""
    WITH ranked AS (
        SELECT 
            conc_plaza_id,
            conc_vrn_no,
            conc_tag_id,
            conc_txn_dt_processed,
            acq_txn_desc,
            acq_txn_reason,
            conc_txn_id,
            ROW_NUMBER() OVER (PARTITION BY conc_txn_id ORDER BY batch DESC, id DESC) as rn
        FROM ods_fastag.idfc_transaction_api
        WHERE conc_plaza_id IN ('{plaza_list}')
          AND acq_txn_reason = 'ANNUALPASS'
          AND conc_txn_dt_processed BETWEEN '{start_date}' AND '{end_date}'
    )
    SELECT 
        conc_plaza_id,
        conc_vrn_no,
        conc_tag_id,
        conc_txn_dt_processed,
        acq_txn_desc,
        acq_txn_reason
    FROM ranked
    WHERE rn = 1
    ORDER BY conc_txn_dt_processed DESC
    """
    if limit:
        query += f" LIMIT {limit}"
    return query


def build_icici_query(plaza_ids: list, start_date: str, end_date: str, limit: Optional[int] = None) -> str:
    """
    Build SQL query for ICICI bank transactions.
    Uses the acquirer_transaction_information table.
    Filters for ANNUALPASS transactions directly in SQL.
    Uses ROW_NUMBER() to deduplicate by conctxnid, keeping only the latest batch.
    """
    plaza_list = "', '".join(plaza_ids)
    query = f"""
    WITH ranked AS (
        SELECT 
            ihmclplazacode,
            vrn,
            tagid,
            acqtxndateprocessed,
            triptype,
            acqtxnreason,
            conctxnid,
            ROW_NUMBER() OVER (PARTITION BY conctxnid ORDER BY batch DESC) as rn
        FROM ods_fastag.acquirer_transaction_information
        WHERE ihmclplazacode IN ('{plaza_list}')
          AND acqtxnreason = 'ANNUALPASS'
          AND acqtxndateprocessed BETWEEN '{start_date}' AND '{end_date}'
    )
    SELECT 
        ihmclplazacode,
        vrn,
        tagid,
        acqtxndateprocessed,
        triptype,
        acqtxnreason
    FROM ranked
    WHERE rn = 1
    ORDER BY acqtxndateprocessed DESC
    """
    if limit:
        query += f" LIMIT {limit}"
    return query


def build_query(bank: str, plaza_ids: list, start_date: str, end_date: str, limit: Optional[int] = None) -> str:
    """
    Build the appropriate SQL query based on bank type.
    """
    if bank == "IDFC":
        return build_idfc_query(plaza_ids, start_date, end_date, limit)
    elif bank == "ICICI":
        return build_icici_query(plaza_ids, start_date, end_date, limit)
    else:
        raise ValueError(f"Unknown bank: {bank}")


def get_column_map(bank: str) -> Dict[str, str]:
    """Get the column mapping for a given bank."""
    if bank == "IDFC":
        return IDFC_COLUMN_MAP
    elif bank == "ICICI":
        return ICICI_ACQ_COLUMN_MAP
    else:
        raise ValueError(f"Unknown bank: {bank}")

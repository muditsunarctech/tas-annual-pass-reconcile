"""
Plaza Configuration Mappings
Maps plaza IDs to bank, plaza name, and project for both IDFC and ICICI banks.
"""

# Bank to Plaza Mapping
BANK_PLAZA_MAP = {
    "IDFC": {
        # BPPTPL Project
        "220001": {"bank": "IDFC", "plaza": "Raipur", "project": "BPPTPL"},
        "220002": {"bank": "IDFC", "plaza": "Indranagar", "project": "BPPTPL"},
        "220003": {"bank": "IDFC", "plaza": "Birami", "project": "BPPTPL"},
        "220004": {"bank": "IDFC", "plaza": "Uthman", "project": "BPPTPL"},
        
        # GHOTI Project
        "142001": {"bank": "IDFC", "plaza": "Ghoti", "project": "GHOTI"},
        
        # Add more IDFC plazas as needed
    },
    "ICICI": {
        # Example ICICI plazas (update with actual data)
        "142002": {"bank": "ICICI", "plaza": "Plaza Name", "project": "ProjectName"},
        
        # Add more ICICI plazas as needed
    }
}

# Project to Plazas mapping (for UI filters)
PROJECT_PLAZA_MAP = {
    "BPPTPL": ["220001", "220002", "220003", "220004"],
    "GHOTI": ["142001"],
    # Add more projects
}

def get_plaza_info(plaza_id: str, bank: str = None):
    """
    Get plaza information by ID.
    
    Args:
        plaza_id: Plaza ID to lookup
        bank: Optional bank filter (IDFC or ICICI)
    
    Returns:
        dict: Plaza information or None if not found
    """
    if bank:
        return BANK_PLAZA_MAP.get(bank, {}).get(plaza_id)
    
    # Search across all banks
    for bank_name, plazas in BANK_PLAZA_MAP.items():
        if plaza_id in plazas:
            return plazas[plaza_id]
    
    return None

def get_plazas_by_project(project_name: str, bank: str = None):
    """
    Get all plaza IDs for a project.
    
    Args:
        project_name: Project name
        bank: Optional bank filter
    
    Returns:
        list: Plaza IDs
    """
    plaza_ids = PROJECT_PLAZA_MAP.get(project_name, [])
    
    if not bank:
        return plaza_ids
    
    # Filter by bank
    return [pid for pid in plaza_ids if get_plaza_info(pid, bank) is not None]

def get_all_projects(bank: str = None):
    """
    Get list of all projects.
    
    Args:
        bank: Optional bank filter
    
    Returns:
        list: Project names
    """
    if not bank:
        return list(PROJECT_PLAZA_MAP.keys())
    
    projects = set()
    for plaza_id, info in BANK_PLAZA_MAP.get(bank, {}).items():
        projects.add(info["project"])
    
    return sorted(list(projects))

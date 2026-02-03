"""
Styles Module - CSS Styling for Streamlit UI
Contains all custom CSS for the Annual Pass Reconciler application.
"""


def get_custom_css():
    """
    Get custom CSS for the Streamlit application.
    
    Returns:
        str: CSS styling string
    """
    return """
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
        @keyframes fadeIn {
            from {
                opacity: 0;
                transform: translateY(20px);
            }
            to {
                opacity: 1;
                transform: translateY(0);
            }
        }
        
        .animate-fade-in {
            animation: fadeIn 0.5s ease-out;
        }
        
        /* Info boxes */
        .info-box {
            background: rgba(74, 144, 164, 0.1);
            border-left: 4px solid #6b9eb8;
            padding: 1rem;
            border-radius: 8px;
            margin: 1rem 0;
        }
        
        .warning-box {
            background: rgba(246, 200, 122, 0.2);
            border-left: 4px solid #f6c87a;
            padding: 1rem;
            border-radius: 8px;
            margin: 1rem 0;
        }
        
        .error-box {
            background: rgba(252, 141, 141, 0.2);
            border-left: 4px solid #fc8d8d;
            padding: 1rem;
            border-radius: 8px;
            margin: 1rem 0;
        }
    </style>
    """


def apply_custom_css():
    """Apply custom CSS to Streamlit app."""
    import streamlit as st
    st.markdown(get_custom_css(), unsafe_allow_html=True)

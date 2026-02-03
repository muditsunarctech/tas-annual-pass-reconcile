# Annual Pass Reconciler

A Streamlit-based application for reconciling FASTag Annual Pass transactions for IDFC and ICICI banks.

## 🚀 Features

- **Dual-Mode Operation**:
  - **Database Mode**: Fetch transactions directly from Redshift warehouse.
  - **File Upload Mode**: Process Excel/CSV files manually.
- **Automated Reconciliation**:
  - Calculates `TripCount` within 24-hour rolling windows.
  - Determines Report Date (logical day cutoff at 8:00 AM).
  - Identifies ATP (Annual Pass) vs. NAP (Non-Annual Pass) transactions.
- **Result Persistence (New!)**:
  - Stores reconciliation runs and results in MySQL.
  - View historical reports and trends.
  - Download past results as CSV/ZIP.
- **Premium UI**:
  - Modern, responsive interface.
  - Real-time progress tracking and logs.

## 🛠️ Architecture

The application follows a modular **MVC (Model-View-Controller)** pattern:

- **`models/`**: Data access and business logic (Database, File Processing, Reconciliation Engine).
- **`views/`**: UI components and styling.
- **`controllers/`**: Application orchestration and logic flow.
- **`config/`**: Configuration for databases and plazas.

## 📋 Prerequisites

- Python 3.8+
- MySQL Server (for result storage)
- Access to Redshift (for database mode)

## 🔧 Installation

1. **Clone the repository**:
   ```bash
   git clone <repository-url>
   cd AnnualPassReconcile
   ```

2. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

3. **Configure Environment**:
   Create a `.env` file in the root directory:
   ```ini
   # Redshift Credentials
   DB_HOST=redshift-cluster.example.com
   DB_PORT=5439
   DB_USER=your_user
   DB_PASSWORD=your_password
   DB_NAME=dev
   
   # MySQL Credentials (Result Storage)
   MYSQL_HOST=localhost
   MYSQL_PORT=3306
   MYSQL_DATABASE=annual_pass_reconciler
   MYSQL_USER=your_mysql_user
   MYSQL_PASSWORD=your_mysql_password
   ```

4. **Initialize MySQL Database**:
   Run the schema script to create the necessary tables:
   ```bash
   mysql -u root -p < config/mysql_schema.sql
   ```

## ▶️ Usage

Start the application:
```bash
streamlit run app.py
```

### Running a Reconciliation
1. Select **"🚀 Run Pipeline"** from the sidebar.
2. Choose your **Data Source**:
   - **Database**: Select Bank, Project, Plaza(s), and Date Range.
   - **File Upload**: Upload your transaction files (Excel/CSV).
3. Click **▶️ Start Reconciliation**.
4. Monitor the live logs and progress.
5. Once complete, view the summary or download the full results.

### Viewing History
1. Select **"📊 View History"** from the sidebar.
2. Filter past runs by Bank or Project.
3. Select a run to view detailed stats and download archived results.

## 📁 Project Structure

```
AnnualPassReconcile/
├── app.py                  # Main entry point
├── models/                 # Data & Logic
│   ├── database.py         # Redshift interactions
│   ├── result_storage.py   # MySQL interactions
│   ├── reconciliation.py   # Core logic
│   └── file_processor.py   # File handling
├── views/                  # UI
│   ├── ui_components.py    # Reusable widgets
│   └── styles.py           # CSS styling
├── controllers/            # orchestration
│   ├── reconciler_controller.py
│   ├── data_fetcher.py
│   └── data_consolidator.py
├── config/                 # Configuration
│   ├── db_config.py        # SQL queries
│   ├── plaza_config.py     # Mappings
│   └── mysql_schema.sql    # DDL
└── archive/                # Legacy files
```

## 🤝 Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add some amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

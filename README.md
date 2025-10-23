# Verra Relo Web Scraper

A Python web scraper using Playwright and Pandas to extract tabular data from web pages and save it to Excel files.

## Features

- **Web Scraping**: Uses Playwright to navigate web pages and extract table data
- **Data Processing**: Converts HTML tables to Pandas DataFrames
- **Excel Export**: Saves data to Excel files with distinct naming
- **CSV Backup System**: Maintains CSV backups as source of truth for data integrity
- **Data Restoration**: Automatically restores Excel files from CSV backups when manipulation is detected
- **Configuration**: URL management through JSON configuration file
- **Modular Design**: Separate modules for web client, data handling, and utilities

## Project Structure

```
verra-relo/
├── config.json          # Configuration file with URLs to scrape
├── requirements.txt      # Python dependencies
├── setup.sh             # Setup script for installation
├── data/                # Directory for output Excel files and metadata
│   └── csv_backups/     # CSV backup files (source of truth)
└── src/                 # Source code
    ├── __init__.py      # Package initialization
    ├── main.py          # Main script
    ├── web_client.py    # Playwright web client class
    ├── data_handler.py  # Data file handling with CSV backup system
    ├── utils.py         # Utility functions
    └── test_scraper.py  # Test script
```

## Installation

1. **Clone the repository** (if needed):

   ```bash
   git clone <repository-url>
   cd verra-relo
   ```

2. **Run the setup script**:

   ```bash
   chmod +x setup.sh
   ./setup.sh
   ```

   This will:

   - Create a virtual environment
   - Install Python dependencies (pandas, playwright, openpyxl, lxml)
   - Install Playwright browser (Chromium)

3. **Activate the virtual environment**:
   ```bash
   source .venv/bin/activate
   ```

## Configuration

Edit `config.json` to specify the URLs you want to scrape:

```json
{
  "fetch_urls": ["https://example.com/page1", "https://example.com/page2"]
}
```

## Usage

### Running the Main Scraper

```bash
cd src
python main.py
```

This will:

1. Read URLs from `config.json`
2. Visit each URL with Playwright
3. Extract all tables from each page
4. Save the data to Excel files in the `data/` directory

### Testing the Setup

```bash
cd src
python test_scraper.py
```

This will run tests to verify that all components are working correctly.

## Modules

### `web_client.py` - PlaywrightWebClient

A class for web scraping using Playwright:

- `start()` / `close()`: Manage browser session
- `go_to_page(url)`: Navigate to a URL
- `extract_tables()`: Extract all tables from current page
- `get_page_tables(url)`: Navigate and extract tables in one call

**Usage Example:**

```python
from web_client import PlaywrightWebClient

with PlaywrightWebClient() as client:
    tables = client.get_page_tables("https://example.com")
    for table in tables:
        print(f"Table shape: {table.shape}")
```

### `data_handler.py` - DataHandler

A class for reading and writing Excel files with CSV backup system:

**Core Features:**

- CSV files serve as the source of truth for data integrity
- Automatic detection of Excel file manipulation via checksums
- Automatic restoration from CSV backup when manipulation is detected
- Incremental data updates with backup synchronization

**Key Methods:**

- `read_excel(filename)`: Read Excel file, restore from CSV backup if manipulated
- `write_excel(dataframe, filename)`: Write DataFrame to Excel with CSV backup
- `write_excel_incremental()`: Incrementally update Excel with new data
- `write_multiple_sheets()`: Handle multiple sheets with individual CSV backups
- `verify_file_integrity()`: Check if Excel file matches stored checksum
- `force_restore_from_backup()`: Manually restore Excel from CSV backup

**Usage Example:**

```python
from data_handler import DataHandler
import pandas as pd

handler = DataHandler()
df = pd.DataFrame({'A': [1, 2], 'B': [3, 4]})
filename = handler.generate_filename("my_data")

# Write Excel with automatic CSV backup creation
handler.write_excel(df, filename)

# Read Excel with automatic integrity check and restoration
df_read = handler.read_excel(filename)

# Check file integrity
is_valid = handler.verify_file_integrity(filename)
```

### `utils.py` - Configuration Utilities

Functions for reading configuration:

- `read_config()`: Read JSON configuration file
- `get_fetch_urls()`: Get list of URLs from config

**Usage Example:**

```python
from utils import get_fetch_urls

urls = get_fetch_urls()
print(f"Found {len(urls)} URLs to process")
```

## Output

The scraper creates files in the `data/` directory:

**Excel Files:**

- `scraped_data_1_domain.xlsx`
- `scraped_data_2_domain.xlsx`

**CSV Backup Files (Source of Truth):**

- `data/csv_backups/scraped_data_1_domain_Data.csv`
- `data/csv_backups/scraped_data_2_domain_Table_1.csv`

**Metadata File:**

- `data/data_metadata.json` - Contains file checksums and sheet information

Each Excel file contains:

- Single sheet named "Data" if one table was found
- Multiple sheets named "Table_1", "Table_2", etc. if multiple tables were found

**Data Integrity:**

- CSV backups are automatically created for each sheet
- File checksums are stored to detect manipulation
- Excel files are automatically restored from CSV backups if manipulation is detected

## Dependencies

- **pandas**: Data manipulation and analysis
- **playwright**: Web browser automation
- **openpyxl**: Excel file writing
- **lxml**: HTML/XML parsing (used by pandas for HTML tables)

## Troubleshooting

1. **Import errors**: Make sure you've activated the virtual environment and installed dependencies
2. **Playwright browser issues**: Run `playwright install chromium` to install browsers
3. **Permission errors**: Make sure the `data/` directory is writable
4. **No tables found**: Check if the target website has `<table>` elements or adjust the table selector

## Customization

### Custom Table Selectors

If the target website uses custom table classes or IDs:

```python
# In web_client.py, modify the extract_tables call:
tables = client.extract_tables("table.data-table")  # Custom CSS selector
```

### Custom File Naming

Modify the filename generation in `main.py`:

```python
# Custom naming pattern
base_name = f"custom_data_{datetime.now().strftime('%Y%m%d')}"
filename = excel_handler.generate_filename(base_name, url, timestamp=False)
```

### Backup and Recovery

The system now includes comprehensive backup and recovery features:

```python
# Manually verify file integrity
handler = DataHandler()
is_valid = handler.verify_file_integrity("my_file.xlsx")

# Force restore from CSV backup
handler.force_restore_from_backup("my_file.xlsx")

# List all CSV backup files
backups = handler.list_csv_backups()

# Get file metadata (checksum, last updated, sheets)
metadata = handler.get_file_metadata("my_file.xlsx")
```

**How the Backup System Works:**

1. Every Excel write operation creates a corresponding CSV backup
2. File checksums are calculated and stored in metadata
3. When reading Excel files, checksums are verified
4. If manipulation is detected, the file is automatically restored from CSV backup
5. CSV files serve as the authoritative source of truth

## License

[Add your license information here]

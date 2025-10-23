# Verra Relo Web Scraper

A Python web scraper using Playwright and Pandas to extract tabular data from web pages and save it to Excel files with CSV backup integrity.

## Installation

1. **Clone the repository**:

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
  "fetch_urls": [
    {
      "name": "data_source_name",
      "url": "https://example.com/page"
    }
  ]
}
```

## Usage

### Running the Main Script

```bash
python src/main.py
```

This will:

1. Read URLs from `config.json`
2. Extract tables from each configured website
3. Save data to Excel files in the `data/` directory
4. Create CSV backups for data integrity

### Using the Backup Utility

**Check file status:**

```bash
python src/backup_utility.py list
```

**Verify file integrity:**

```bash
python src/backup_utility.py check --file "filename.xlsx"
```

**Restore from backup:**

```bash
python src/backup_utility.py restore --file "filename.xlsx"
```

## Output Files

- **Excel files**: `data/filename.xlsx` (for business use)
- **CSV backups**: `data/backups/filename_data.csv` (source of truth)
- **Metadata**: `data/backups/metadata.json` (integrity tracking)

## Troubleshooting

1. **Import errors**: Ensure virtual environment is activated
2. **Browser issues**: Run `playwright install chromium`
3. **Permission errors**: Check `data/` directory is writable
4. **No tables found**: Verify target website has `<table>` elements

For detailed functional design and architecture, see [FUNCTIONAL_DESIGN.md](FUNCTIONAL_DESIGN.md).

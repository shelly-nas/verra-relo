"""
Main script to fetch data from configured URLs and save to Excel files.
"""
import logging

# Set up logging BEFORE importing modules that use logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s: %(message)s'
)

from typing import List
import pandas as pd
from utils import get_url_objects
from web_client import PlaywrightWebClient
from data_handler import DataHandler

logger = logging.getLogger(__name__)


def main():
    """
    Main function to orchestrate the data fetching and saving process.
    """
    try:
        # Initialize components
        logger.info("Starting data fetching process...")
        
        # Get URL objects from config
        url_objects = get_url_objects()
        logger.info(f"Found {len(url_objects)} URLs to process")
        
        # Initialize Data handler
        data_handler = DataHandler()
        
        # Process each URL
        with PlaywrightWebClient(headless=True) as web_client:
            for i, url_obj in enumerate(url_objects, 1):
                name = url_obj['name']
                url = url_obj['url']
                
                logger.info(f"Processing URL {i}/{len(url_objects)}: {name} ({url})")
                
                try:
                    # Fetch tables from the URL
                    dataframes = web_client.get_page_tables(url)
                    
                    if not dataframes:
                        logger.warning(f"No tables found on page: {name} ({url})")
                        continue
                    
                    logger.info(f"Extracted {len(dataframes)} table(s) from {name}")
                    
                    # Use the name from config for the filename (no timestamp)
                    filename = data_handler.generate_filename(name, timestamp=False)
                    
                    # Save data to Excel incrementally with CSV backup
                    if len(dataframes) == 1:
                        # Single table - save to single sheet incrementally
                        filepath, total_rows, new_rows = data_handler.write_excel_incremental(
                            dataframes[0], 
                            filename, 
                            sheet_name="data"
                        )
                        logger.info(f"Successfully updated data from {name} to {filepath}")
                        logger.info(f"Total rows: {total_rows}, New rows added: {new_rows}")
                    else:
                        # Multiple tables - save to multiple sheets incrementally
                        sheet_names = [f"Table_{j+1}" for j in range(len(dataframes))]
                        filepath, results = data_handler.write_multiple_sheets_incremental(
                            dataframes, 
                            filename, 
                            sheet_names
                        )
                        logger.info(f"Successfully updated data from {name} to {filepath}")
                        
                        # Log summary information for each sheet
                        total_new_rows = 0
                        total_all_rows = 0
                        for i, (total_rows, new_rows) in enumerate(results):
                            logger.info(f"Sheet {sheet_names[i]}: {total_rows} total rows, {new_rows} new rows")
                            total_new_rows += new_rows
                            total_all_rows += total_rows
                        logger.info(f"Overall: {total_all_rows} total rows, {total_new_rows} new rows added")
                
                except Exception as e:
                    logger.error(f"Failed to process URL {name} ({url}): {e}")
                    continue
        
        logger.info("Data fetching process completed successfully!")
        
        # List all created files
        excel_files = data_handler.list_excel_files()
        if excel_files:
            logger.info("Created Excel files:")
            for file in excel_files:
                logger.info(f"  - {file}")
        
        # List CSV backup files
        csv_backups = data_handler.list_csv_backups()
        if csv_backups:
            logger.info("CSV backup files:")
            for file in csv_backups:
                logger.info(f"  - {file}")
    
    except Exception as e:
        logger.error(f"Fatal error in main process: {e}")
        raise


if __name__ == "__main__":
    main()
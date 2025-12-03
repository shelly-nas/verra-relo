"""
Main script to fetch data from configured URLs and save to Excel files.
"""
import logging
from datetime import datetime

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
from email_notifier import EmailNotifier

logger = logging.getLogger(__name__)

# Track last run info for web UI
_last_run_info = {
    'details': None,
    'email_sent': False,
    'email_sent_at': None,
    'email_subject': None,
    'email_summary': None,
    'email_recipients': 0
}


def get_last_run_info():
    """Get information about the last run for the web UI."""
    return _last_run_info.copy()


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
        
        # Initialize Data handler and email notifier
        data_handler = DataHandler()
        email_notifier = EmailNotifier()
        
        # Track changes for email notification
        all_changes = []
        
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
                        filepath, total_rows, new_rows, new_rows_df = data_handler.write_excel_incremental(
                            dataframes[0], 
                            filename, 
                            sheet_name="data"
                        )
                        logger.info(f"Successfully updated data from {name} to {filepath}")
                        logger.info(f"Total rows: {total_rows}, New rows added: {new_rows}")
                        
                        # Track changes for email notification
                        if new_rows > 0:
                            all_changes.append({
                                'name': name,
                                'total_rows': total_rows,
                                'new_rows': new_rows,
                                'new_rows_df': new_rows_df
                            })
                    else:
                        # Multiple tables - save to multiple sheets incrementally
                        sheet_names = [f"Table_{j+1}" for j in range(len(dataframes))]
                        filepath, results = data_handler.write_multiple_sheets_incremental(
                            dataframes, 
                            filename, 
                            sheet_names
                        )
                        logger.info(f"Successfully updated data from {name} to {filepath}")
                        
                        # Log summary information for each sheet and collect new rows
                        total_new_rows = 0
                        total_all_rows = 0
                        combined_new_rows_df = None
                        for i, (total_rows_sheet, new_rows_sheet, new_rows_df_sheet) in enumerate(results):
                            logger.info(f"Sheet {sheet_names[i]}: {total_rows_sheet} total rows, {new_rows_sheet} new rows")
                            total_new_rows += new_rows_sheet
                            total_all_rows += total_rows_sheet
                            # Combine new rows from all sheets
                            if new_rows_df_sheet is not None and not new_rows_df_sheet.empty:
                                if combined_new_rows_df is None:
                                    combined_new_rows_df = new_rows_df_sheet
                                else:
                                    combined_new_rows_df = pd.concat([combined_new_rows_df, new_rows_df_sheet], ignore_index=True)
                        logger.info(f"Overall: {total_all_rows} total rows, {total_new_rows} new rows added")
                        
                        # Track changes for email notification
                        if total_new_rows > 0:
                            all_changes.append({
                                'name': name,
                                'total_rows': total_all_rows,
                                'new_rows': total_new_rows,
                                'new_rows_df': combined_new_rows_df
                            })
                
                except Exception as e:
                    logger.error(f"Failed to process URL {name} ({url}): {e}")
                    continue
        
        logger.info("Data fetching process completed successfully!")
        
        # Build run details summary
        sources_processed = len(url_objects)
        total_new = sum(c['new_rows'] for c in all_changes) if all_changes else 0
        sources_with_changes = len(all_changes)
        
        _last_run_info['details'] = f"Processed {sources_processed} source(s), {total_new} new row(s) in {sources_with_changes} source(s)"
        
        # Always send email notification (even with no changes, to confirm script is running)
        # Build status info for sources
        all_sources_status = []
        for url_obj in url_objects:
            name = url_obj['name']
            # Find if this source had changes
            change_info = next((c for c in all_changes if c['name'] == name), None)
            if change_info:
                all_sources_status.append({
                    'name': name,
                    'total_rows': change_info['total_rows'],
                    'new_rows': change_info['new_rows'],
                    'new_rows_df': change_info.get('new_rows_df')
                })
            else:
                # No changes for this source - still include it in the email
                all_sources_status.append({
                    'name': name,
                    'total_rows': 0,  # We don't track this if no changes
                    'new_rows': 0,
                    'new_rows_df': None
                })
        
        # Determine email subject based on changes
        if total_new > 0:
            email_subject = f"IND Register Update: {total_new} new entries detected"
            logger.info(f"Changes detected: {total_new} new rows across {sources_with_changes} source(s)")
        else:
            email_subject = "IND Register Update: No new entries (status check)"
            logger.info("No changes detected, sending status confirmation email")
        
        # Send email
        if email_notifier.send_changes_notification(all_sources_status, subject=email_subject):
            logger.info("Email notification sent successfully")
            _last_run_info['email_sent'] = True
            _last_run_info['email_sent_at'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            _last_run_info['email_subject'] = email_subject
            _last_run_info['email_summary'] = f"{total_new} new entries across {sources_with_changes} source(s)"
            _last_run_info['email_recipients'] = len(email_notifier.mailing_list)
        else:
            logger.info("Email notification skipped or failed")
            _last_run_info['email_sent'] = False
        
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
"""
Web client using Playwright for scraping data from web pages.
"""
import pandas as pd
from playwright.sync_api import sync_playwright
from typing import List, Optional
import logging
from io import StringIO

logger = logging.getLogger(__name__)


class PlaywrightWebClient:
    """
    A web client class using Playwright to navigate pages and extract table data.
    """
    
    def __init__(self, headless: bool = True, timeout: int = 30000):
        """
        Initialize the Playwright web client.
        
        Args:
            headless (bool): Whether to run browser in headless mode
            timeout (int): Default timeout in milliseconds
        """
        self.headless = headless
        self.timeout = timeout
        self.playwright = None
        self.browser = None
        self.context = None
        self.page = None
    
    def __enter__(self):
        """Context manager entry."""
        self.start()
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        self.close()
    
    def start(self):
        """Start the browser session."""
        try:
            logger.info("Starting Playwright...")
            self.playwright = sync_playwright().start()
            logger.info("Launching Firefox browser...")
            # Use Firefox for better compatibility with emulation/Docker
            self.browser = self.playwright.firefox.launch(
                headless=self.headless,
            )
            logger.info("Creating browser context...")
            self.context = self.browser.new_context(
                viewport={'width': 1280, 'height': 720},
                user_agent='Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:120.0) Gecko/20100101 Firefox/120.0'
            )
            self.page = self.context.new_page()
            self.page.set_default_timeout(self.timeout)
            logger.info("Browser session started successfully")
        except Exception as e:
            logger.error(f"Failed to start browser session: {e}")
            self.close()
            raise
    
    def close(self):
        """Close the browser session and cleanup resources."""
        try:
            if self.page:
                self.page.close()
            if self.context:
                self.context.close()
            if self.browser:
                self.browser.close()
            if self.playwright:
                self.playwright.stop()
            logger.info("Browser session closed successfully")
        except Exception as e:
            logger.error(f"Error closing browser session: {e}")
    
    def go_to_page(self, url: str) -> bool:
        """
        Navigate to a specific URL.
        
        Args:
            url (str): The URL to navigate to
            
        Returns:
            bool: True if navigation was successful, False otherwise
        """
        if not self.page:
            logger.error("Browser session not started. Call start() first.")
            return False
        
        try:
            logger.info(f"Navigating to: {url}")
            response = self.page.goto(url)
            
            if response and response.status >= 400:
                logger.error(f"HTTP error {response.status} when accessing {url}")
                return False
            
            # Wait for page to load
            self.page.wait_for_load_state('networkidle')
            logger.info(f"Successfully navigated to: {url}")
            return True
        
        except Exception as e:
            logger.error(f"Failed to navigate to {url}: {e}")
            return False
    
    def extract_tables(self, table_selector: str = "table") -> List[pd.DataFrame]:
        """
        Extract all tables from the current page and convert them to DataFrames.
        
        Args:
            table_selector (str): CSS selector for tables (default: "table")
            
        Returns:
            List[pd.DataFrame]: List of DataFrames, one for each table found
        """
        if not self.page:
            logger.error("Browser session not started. Call start() first.")
            return []
        
        try:
            # Wait for tables to be present
            self.page.wait_for_selector(table_selector, timeout=10000)
            
            # Get all table elements
            tables = self.page.query_selector_all(table_selector)
            logger.info(f"Found {len(tables)} table(s) on the page")
            
            dataframes = []
            
            for i, table in enumerate(tables):
                try:
                    # Extract table HTML
                    table_html = table.inner_html()
                    
                    # Convert to DataFrame using pandas
                    # We wrap it in a complete table tag for pandas to parse correctly
                    full_table_html = f"<table>{table_html}</table>"
                    
                    # Use pandas to read HTML tables
                    # Use StringIO to avoid future warning about literal HTML
                    dfs = pd.read_html(StringIO(full_table_html), keep_default_na=False)
                    
                    if dfs:
                        df = dfs[0]  # Take the first (and should be only) table
                        
                        # Ensure all columns are strings to preserve any special formatting
                        for col in df.columns:
                            df[col] = df[col].astype(str)
                        
                        # Additional processing for first two columns to handle specific cases
                        if len(df.columns) >= 1:
                            # Clean first column - remove extra whitespace but preserve content
                            df.iloc[:, 0] = df.iloc[:, 0].str.strip()
                            
                        if len(df.columns) >= 2:
                            # Clean second column - preserve numeric strings including leading zeros
                            # Handle the case where pandas converts numbers to float and we want to preserve as string
                            def format_kvk_number(val):
                                if pd.isna(val) or val == '' or val == 'nan':
                                    return ''
                                # Convert to string, removing .0 from floats if present
                                str_val = str(val)
                                if str_val.endswith('.0'):
                                    str_val = str_val[:-2]
                                # Ensure leading zeros are preserved for numbers that should have them
                                # KvK numbers should be 8 digits, so pad with leading zeros if needed
                                if str_val.isdigit() and len(str_val) < 8:
                                    str_val = str_val.zfill(8)
                                return str_val.strip()
                            
                            df.iloc[:, 1] = df.iloc[:, 1].apply(format_kvk_number)
                        
                        logger.info(f"Table {i+1}: {df.shape[0]} rows, {df.shape[1]} columns")
                        logger.info(f"All columns converted to string type to preserve formatting")
                        dataframes.append(df)
                    
                except Exception as e:
                    logger.warning(f"Failed to extract table {i+1}: {e}")
                    continue
            
            return dataframes
        
        except Exception as e:
            logger.error(f"Failed to extract tables: {e}")
            return []
    
    def get_page_tables(self, url: str, table_selector: str = "table") -> List[pd.DataFrame]:
        """
        Navigate to a URL and extract all tables from the page.
        
        Args:
            url (str): The URL to scrape
            table_selector (str): CSS selector for tables
            
        Returns:
            List[pd.DataFrame]: List of DataFrames extracted from the page
        """
        if self.go_to_page(url):
            return self.extract_tables(table_selector)
        else:
            logger.error(f"Failed to load page: {url}")
            return []
"""
Email notification module for sending alerts when data changes are detected.
"""
import smtplib
import logging
import os
import json
import io
from email.message import EmailMessage
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from typing import List, Dict, Optional
from datetime import datetime

try:
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

logger = logging.getLogger(__name__)


class EmailNotifier:
    """
    Handles email notifications for data change alerts.
    Uses SMTP for sending emails.
    """
    
    def __init__(self):
        """Initialize the email notifier with configuration."""
        self.smtp_server = os.getenv('SMTP_SERVER', 'smtp.gmail.com')
        self.smtp_port = int(os.getenv('SMTP_PORT', '587'))
        self.smtp_username = os.getenv('SMTP_USERNAME', '')
        self.smtp_password = os.getenv('SMTP_PASSWORD', '')
        self.sender_email = os.getenv('SENDER_EMAIL', self.smtp_username)
        self.sender_name = self._load_sender_name()
        self.mailing_list = self._load_mailing_list()
        self.enabled = self._is_enabled()
        
        if self.enabled:
            logger.info(f"Email notifier initialized with {len(self.mailing_list)} recipients")
        else:
            logger.info("Email notifier is disabled (missing SMTP configuration)")
    
    def _is_enabled(self) -> bool:
        """Check if email notifications are enabled (SMTP configured)."""
        return bool(self.smtp_username and self.smtp_password and self.mailing_list)
    
    def _get_formatted_sender(self) -> str:
        """Get the formatted sender string with optional display name."""
        if self.sender_name:
            return f"{self.sender_name} <{self.sender_email}>"
        return self.sender_email
    
    def _load_sender_name(self) -> str:
        """
        Load sender name from config file.
        
        Reads from src/config.json 'sender_name' key.
        """
        try:
            config_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                'config.json'
            )
            if os.path.exists(config_path):
                with open(config_path, 'r') as f:
                    config = json.load(f)
                    return config.get('sender_name', 'IND Register Alerts')
        except Exception as e:
            logger.warning(f"Could not load sender name from config: {e}")
        
        return 'IND Register Alerts'
    
    def _load_mailing_list(self) -> List[str]:
        """
        Load mailing list from config file.
        
        Reads from src/config.json 'mailing_list' key.
        Mailing list is managed via the web UI.
        """
        try:
            config_path = os.path.join(
                os.path.dirname(os.path.abspath(__file__)),
                'config.json'
            )
            if os.path.exists(config_path):
                with open(config_path, 'r') as f:
                    config = json.load(f)
                    mailing_list = config.get('mailing_list', [])
                    if isinstance(mailing_list, list):
                        return mailing_list
        except Exception as e:
            logger.warning(f"Could not load mailing list from config: {e}")
        
        return []
    
    def send_changes_notification(
        self, 
        changes: List[Dict], 
        subject: Optional[str] = None
    ) -> bool:
        """
        Send notification email about detected changes with CSV attachment of new entries.
        
        Args:
            changes: List of change dictionaries with keys:
                     - name: Name of the data source
                     - total_rows: Total rows after update
                     - new_rows: Number of new rows added
                     - new_rows_df: Optional pandas DataFrame with new entries
            subject: Optional custom subject line
            
        Returns:
            bool: True if email sent successfully, False otherwise
        """
        if not self.enabled:
            logger.info("Email notifications disabled, skipping send")
            return False
        
        # Always send email, even if no changes (to confirm script is running)
        # Build email content
        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        if subject is None:
            total_new = sum(c.get('new_rows', 0) for c in changes)
            subject = f"IND Register Update: {total_new} new entries detected"
        
        # Create HTML email body
        html_body = self._create_html_body(changes, timestamp)
        text_body = self._create_text_body(changes, timestamp)
        
        # Create combined CSV attachment from all new rows
        csv_attachment = self._create_csv_attachment(changes, timestamp)
        
        return self._send_email(subject, html_body, text_body, csv_attachment)
    
    def _create_csv_attachment(self, changes: List[Dict], timestamp: str) -> Optional[Dict]:
        """
        Create a CSV attachment containing all new entries from all sources.
        
        Args:
            changes: List of change dictionaries with new_rows_df
            timestamp: Timestamp string for filename
            
        Returns:
            Dict with 'filename' and 'data' keys, or None if no data
        """
        if not PANDAS_AVAILABLE:
            logger.warning("Pandas not available, cannot create CSV attachment")
            return None
        
        # Collect all new rows DataFrames
        all_new_rows = []
        for change in changes:
            new_rows_df = change.get('new_rows_df')
            logger.info(f"Processing change: {change.get('name')}, new_rows_df is None: {new_rows_df is None}")
            if new_rows_df is not None:
                logger.info(f"new_rows_df.empty: {new_rows_df.empty}, shape: {new_rows_df.shape}")
            if new_rows_df is not None and not new_rows_df.empty:
                # Add source name column to identify which source the row came from
                df_copy = new_rows_df.copy()
                df_copy.insert(0, 'source', change.get('name', 'Unknown'))
                all_new_rows.append(df_copy)
                logger.info(f"Added {len(df_copy)} rows to attachment")
        
        if not all_new_rows:
            logger.info("No new rows data available for CSV attachment")
            return None
        
        try:
            # Combine all new rows into a single DataFrame
            combined_df = pd.concat(all_new_rows, ignore_index=True)
            
            # Convert to CSV
            csv_buffer = io.StringIO()
            combined_df.to_csv(csv_buffer, index=False)
            csv_data = csv_buffer.getvalue()
            
            # Create filename with timestamp
            date_str = datetime.now().strftime('%Y%m%d_%H%M%S')
            filename = f"new_entries_{date_str}.csv"
            
            logger.info(f"Created CSV attachment with {len(combined_df)} new entries")
            
            return {
                'filename': filename,
                'data': csv_data
            }
            
        except Exception as e:
            logger.error(f"Failed to create CSV attachment: {e}")
            return None
    
    def _create_html_body(self, changes: List[Dict], timestamp: str) -> str:
        """Create HTML email body."""
        total_new = sum(c.get('new_rows', 0) for c in changes) if changes else 0
        
        if total_new > 0:
            subtitle = f"Changes detected at {timestamp}"
            attachment_note = '<p style="color: #37352f; font-size: 14px; margin-top: 16px;">📎 <em>See attached CSV file for the complete list of new entries.</em></p>'
        else:
            subtitle = f"Status check at {timestamp} - No new entries found"
            attachment_note = '<p style="color: #787774; font-size: 14px; margin-top: 16px;">✓ <em>The scheduler is running correctly. No new entries were found in this check.</em></p>'
        
        html = f"""
        <html>
        <head>
            <style>
                body {{ font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif; color: #37352f; }}
                .container {{ max-width: 600px; margin: 0 auto; padding: 20px; }}
                h1 {{ font-size: 24px; color: #37352f; margin-bottom: 8px; }}
                .subtitle {{ color: #787774; font-size: 14px; margin-bottom: 24px; }}
                table {{ width: 100%; border-collapse: collapse; margin-bottom: 20px; }}
                th, td {{ padding: 12px; text-align: left; border-bottom: 1px solid #e3e2e0; }}
                th {{ background: #f7f6f5; font-weight: 600; font-size: 12px; text-transform: uppercase; letter-spacing: 0.03em; }}
                .new-rows {{ color: #0f7b0f; font-weight: 600; }}
                .footer {{ color: #787774; font-size: 12px; margin-top: 24px; padding-top: 16px; border-top: 1px solid #e3e2e0; }}
            </style>
        </head>
        <body>
            <div class="container">
                <h1>IND Register Update</h1>
                <p class="subtitle">{subtitle}</p>
                
                <table>
                    <tr>
                        <th>Data Source</th>
                        <th>New Entries</th>
                        <th>Total Entries</th>
                    </tr>
        """
        
        for change in changes:
            name = change.get('name', 'Unknown')
            new_rows = change.get('new_rows', 0)
            total_rows = change.get('total_rows', 0)
            
            new_class = ' class="new-rows"' if new_rows > 0 else ''
            total_display = total_rows if total_rows > 0 else '-'
            html += f"""
                    <tr>
                        <td>{name}</td>
                        <td{new_class}>{new_rows}</td>
                        <td>{total_display}</td>
                    </tr>
            """
        
        html += f"""
                </table>
                
                <p><strong>Total new entries:</strong> <span class="new-rows">{total_new}</span></p>
                
                {attachment_note}
                
                <div class="footer">
                    <p>This is an automated notification from the IND Register Scheduler.</p>
                </div>
            </div>
        </body>
        </html>
        """
        
        return html
    
    def _create_text_body(self, changes: List[Dict], timestamp: str) -> str:
        """Create plain text email body."""
        total_new = sum(c.get('new_rows', 0) for c in changes) if changes else 0
        
        text = f"IND Register Update\n"
        if total_new > 0:
            text += f"Changes detected at {timestamp}\n"
        else:
            text += f"Status check at {timestamp} - No new entries found\n"
        text += "=" * 50 + "\n\n"
        
        for change in changes:
            name = change.get('name', 'Unknown')
            new_rows = change.get('new_rows', 0)
            total_rows = change.get('total_rows', 0)
            
            text += f"• {name}\n"
            text += f"  New entries: {new_rows}\n"
            text += f"  Total entries: {total_rows if total_rows > 0 else '-'}\n\n"
        
        text += f"Total new entries: {total_new}\n\n"
        
        if total_new > 0:
            text += "See attached CSV file for the complete list of new entries.\n\n"
        else:
            text += "The scheduler is running correctly. No new entries were found in this check.\n\n"
        
        text += "-" * 50 + "\n"
        text += "This is an automated notification from the IND Register Scheduler.\n"
        
        return text
    
    def _send_email(self, subject: str, html_body: str, text_body: str, csv_attachment: Optional[Dict] = None) -> bool:
        """
        Send email via SMTP with optional CSV attachment.
        
        Args:
            subject: Email subject
            html_body: HTML version of the email
            text_body: Plain text version of the email
            csv_attachment: Optional dict with 'filename' and 'data' keys for CSV attachment
            
        Returns:
            bool: True if sent successfully
        """
        try:
            if csv_attachment:
                # Use MIMEMultipart for email with attachment
                msg = MIMEMultipart('mixed')
                msg['Subject'] = subject
                msg['From'] = self._get_formatted_sender()
                msg['Bcc'] = ', '.join(self.mailing_list)
                
                # Create alternative container for text/html
                msg_alternative = MIMEMultipart('alternative')
                msg_alternative.attach(MIMEText(text_body, 'plain'))
                msg_alternative.attach(MIMEText(html_body, 'html'))
                msg.attach(msg_alternative)
                
                # Add CSV attachment
                csv_part = MIMEBase('text', 'csv')
                csv_part.set_payload(csv_attachment['data'])
                encoders.encode_base64(csv_part)
                csv_part.add_header(
                    'Content-Disposition',
                    f"attachment; filename={csv_attachment['filename']}"
                )
                msg.attach(csv_part)
                logger.info(f"Attached CSV file: {csv_attachment['filename']}")
            else:
                # Use simple EmailMessage without attachment
                msg = EmailMessage()
                msg['Subject'] = subject
                msg['From'] = self._get_formatted_sender()
                msg['Bcc'] = ', '.join(self.mailing_list)
                
                # Set plain text content and add HTML alternative
                msg.set_content(text_body)
                msg.add_alternative(html_body, subtype='html')
            
            # Debug: Log SMTP configuration (mask password)
            logger.debug(f"SMTP Server: {self.smtp_server}:{self.smtp_port}")
            logger.debug(f"SMTP Username: {self.smtp_username}")
            logger.debug(f"SMTP Password: {'*' * len(self.smtp_password) if self.smtp_password else '(empty)'}")
            logger.debug(f"Sender Email: {self.sender_email}")
            
            # Connect and send using STARTTLS
            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.ehlo()
                server.starttls()
                server.ehlo()
                # Use smtp_username for login (the account credentials)
                logger.info(f"Logging in with username: {self.smtp_username}")
                server.login(self.smtp_username, self.smtp_password)
                server.send_message(msg)
            
            logger.info(f"Email notification sent to {len(self.mailing_list)} recipients")
            return True
            
        except Exception as e:
            logger.error(f"Failed to send email notification: {e}")
            return False
    
    def send_test_email(self) -> bool:
        """Send a test email to verify configuration."""
        if not self.enabled:
            logger.error("Cannot send test email: SMTP not configured")
            return False
        
        # Create sample DataFrame for CSV attachment if pandas is available
        test_df = None
        if PANDAS_AVAILABLE:
            test_df = pd.DataFrame({
                'Organisatie': ['Test Company A', 'Test Company B', 'Test Company C'],
                'KvK nummer': ['12345678', '23456789', '34567890'],
                'created_date': ['2025-12-01', '2025-12-02', '2025-12-03']
            })
        
        test_changes = [{
            'name': 'Test Data Source',
            'new_rows': 3,
            'total_rows': 100,
            'new_rows_df': test_df
        }]
        
        return self.send_changes_notification(
            test_changes,
            subject="IND Register Scheduler - Test Notification"
        )

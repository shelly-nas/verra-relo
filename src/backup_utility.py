#!/usr/bin/env python3
"""
Data backup utility for managing CSV backups and file integrity.
"""
import os
import sys
import argparse
import pandas as pd
from datetime import datetime

# Add src directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from data_handler import DataHandler

def list_files(handler):
    """List all Excel files and their backup status."""
    print("=== Data Files Overview ===\n")
    
    excel_files = handler.list_excel_files()
    csv_backups = handler.list_csv_backups()
    
    if not excel_files:
        print("No Excel files found.")
        return
    
    print(f"{'File':<30} {'Status':<15} {'Backup':<15} {'Last Updated'}")
    print("-" * 75)
    
    for excel_file in excel_files:
        # Skip temporary files
        if excel_file.startswith('~$'):
            continue
            
        is_valid = handler.verify_file_integrity(excel_file)
        status = "✓ Valid" if is_valid else "⚠ Modified"
        
        # Check if CSV backup exists
        metadata = handler.get_file_metadata(excel_file)
        sheet_names = metadata.get('sheet_names', ['Data'])
        has_backup = any(handler._get_csv_backup_path(excel_file, sheet).split('/')[-1] in csv_backups 
                        for sheet in sheet_names)
        backup_status = "✓ Yes" if has_backup else "✗ No"
        
        last_updated = metadata.get('last_updated', 'Unknown')
        if last_updated != 'Unknown':
            try:
                dt = datetime.fromisoformat(last_updated)
                last_updated = dt.strftime('%Y-%m-%d %H:%M')
            except:
                pass
        
        print(f"{excel_file:<30} {status:<15} {backup_status:<15} {last_updated}")

def check_integrity(handler, filename):
    """Check integrity of a specific file."""
    print(f"=== Checking Integrity: {filename} ===\n")
    
    if not os.path.exists(os.path.join(handler.data_directory, filename)):
        print(f"✗ File {filename} not found")
        return
    
    is_valid = handler.verify_file_integrity(filename)
    metadata = handler.get_file_metadata(filename)
    
    if is_valid:
        print(f"✓ File integrity: Valid")
    else:
        print(f"⚠ File integrity: Modified (checksum mismatch)")
    
    print(f"Last updated: {metadata.get('last_updated', 'Unknown')}")
    print(f"Sheet names: {', '.join(metadata.get('sheet_names', []))}")
    print(f"Stored checksum: {metadata.get('checksum', 'Unknown')}")
    
    current_checksum = handler._calculate_file_checksum(
        os.path.join(handler.data_directory, filename)
    )
    print(f"Current checksum: {current_checksum}")
    
    # Check CSV backups
    print("\nCSV Backup Status:")
    sheet_names = metadata.get('sheet_names', ['Data'])
    for sheet in sheet_names:
        csv_path = handler._get_csv_backup_path(filename, sheet)
        exists = os.path.exists(csv_path)
        print(f"  {sheet}: {'✓ Available' if exists else '✗ Missing'}")

def restore_file(handler, filename, force=False):
    """Restore a file from CSV backup."""
    print(f"=== Restoring: {filename} ===\n")
    
    if not force:
        is_valid = handler.verify_file_integrity(filename)
        if is_valid:
            print("File integrity is valid. Use --force to restore anyway.")
            return
        else:
            print("File has been modified. Restoring from CSV backup...")
    else:
        print("Force restoring from CSV backup...")
    
    try:
        metadata = handler.get_file_metadata(filename)
        sheet_names = metadata.get('sheet_names', ['Data'])
        handler.force_restore_from_backup(filename, sheet_names)
        print(f"✓ Successfully restored {filename}")
    except Exception as e:
        print(f"✗ Failed to restore {filename}: {e}")

def backup_file(handler, filename):
    """Create or update CSV backup for a file."""
    print(f"=== Creating Backup: {filename} ===\n")
    
    filepath = os.path.join(handler.data_directory, filename)
    if not os.path.exists(filepath):
        print(f"✗ File {filename} not found")
        return
    
    try:
        # Read the Excel file and create backups for each sheet
        workbook = pd.ExcelFile(filepath)
        sheet_names = workbook.sheet_names
        
        for sheet_name in sheet_names:
            df = pd.read_excel(filepath, sheet_name=sheet_name, dtype=str)
            df = df.fillna('')
            handler._create_csv_backup(df, filename, sheet_name)
            print(f"✓ Created backup for sheet: {sheet_name}")
        
        # Update metadata
        handler._update_file_metadata(filename, sheet_names)
        print(f"✓ Updated metadata for {filename}")
        
    except Exception as e:
        print(f"✗ Failed to create backup for {filename}: {e}")

def main():
    parser = argparse.ArgumentParser(description='Data Backup Utility')
    parser.add_argument('command', choices=['list', 'check', 'restore', 'backup'], 
                       help='Command to execute')
    parser.add_argument('--file', '-f', help='Specific file to operate on')
    parser.add_argument('--force', action='store_true', 
                       help='Force restore even if file appears valid')
    
    args = parser.parse_args()
    
    handler = DataHandler()
    
    if args.command == 'list':
        list_files(handler)
    
    elif args.command == 'check':
        if not args.file:
            print("Error: --file is required for check command")
            return
        check_integrity(handler, args.file)
    
    elif args.command == 'restore':
        if not args.file:
            print("Error: --file is required for restore command")
            return
        restore_file(handler, args.file, args.force)
    
    elif args.command == 'backup':
        if not args.file:
            print("Error: --file is required for backup command")
            return
        backup_file(handler, args.file)

if __name__ == "__main__":
    main()
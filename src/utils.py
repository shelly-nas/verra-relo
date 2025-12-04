"""
Utility functions for reading configuration and handling common tasks.
"""
import json
import os
from typing import List, Dict
from urllib.parse import urlparse


def get_config_path(config_path: str = "src/config.json") -> str:
    """
    Get the absolute path to the configuration file.
    
    Args:
        config_path (str): Path to the configuration file
        
    Returns:
        str: Absolute path to the configuration file
    """
    if not os.path.isabs(config_path):
        # If relative path, make it relative to project root
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        config_path = os.path.join(project_root, config_path)
    return config_path


def read_config(config_path: str = "src/config.json") -> dict:
    """
    Read configuration from JSON file.
    
    Args:
        config_path (str): Path to the configuration file
        
    Returns:
        dict: Configuration data
        
    Raises:
        FileNotFoundError: If config file doesn't exist
        json.JSONDecodeError: If config file contains invalid JSON
    """
    config_path = get_config_path(config_path)
    
    try:
        with open(config_path, 'r', encoding='utf-8') as file:
            config = json.load(file)
        return config
    except FileNotFoundError:
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    except json.JSONDecodeError as e:
        raise json.JSONDecodeError(f"Invalid JSON in configuration file: {e}")


def write_config(config: dict, config_path: str = "src/config.json") -> None:
    """
    Write configuration to JSON file.
    
    Args:
        config (dict): Configuration data to write
        config_path (str): Path to the configuration file
    """
    config_path = get_config_path(config_path)
    
    with open(config_path, 'w', encoding='utf-8') as file:
        json.dump(config, file, indent=2, ensure_ascii=False)


def get_scheduler_state() -> dict:
    """
    Get scheduler state from configuration file.
    
    Returns:
        dict: Scheduler state with keys: is_running, interval_hours, selected_day, last_run, next_run
    """
    try:
        config = read_config()
        return config.get('scheduler_state', {
            'is_running': False,
            'interval_hours': 672,
            'selected_day': 1,
            'last_run': None,
            'next_run': None
        })
    except (FileNotFoundError, json.JSONDecodeError):
        return {
            'is_running': False,
            'interval_hours': 672,
            'selected_day': 1,
            'last_run': None,
            'next_run': None
        }


def save_scheduler_state(state: dict) -> None:
    """
    Save scheduler state to configuration file.
    
    Args:
        state (dict): Scheduler state to save
    """
    try:
        config = read_config()
    except (FileNotFoundError, json.JSONDecodeError):
        config = {'fetch_urls': [], 'mailing_list': []}
    
    # Only save persistent state (not thread objects)
    config['scheduler_state'] = {
        'is_running': state.get('is_running', False),
        'interval_hours': state.get('interval_hours', 672),
        'selected_day': state.get('selected_day', 1),
        'last_run': state.get('last_run'),
        'next_run': state.get('next_run')
    }
    write_config(config)


def get_mailing_list() -> List[str]:
    """
    Get mailing list from configuration file.
    
    Returns:
        List[str]: List of email addresses
    """
    try:
        config = read_config()
        mailing_list = config.get('mailing_list', [])
        if isinstance(mailing_list, list):
            return mailing_list
        return []
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def save_mailing_list(emails: List[str]) -> None:
    """
    Save mailing list to configuration file.
    
    Args:
        emails (List[str]): List of email addresses to save
    """
    try:
        config = read_config()
    except (FileNotFoundError, json.JSONDecodeError):
        config = {'fetch_urls': [], 'mailing_list': []}
    
    # Clean and validate emails
    clean_emails = [email.strip() for email in emails if email.strip()]
    config['mailing_list'] = clean_emails
    write_config(config)


def get_sender_name() -> str:
    """
    Get sender name from configuration file.
    
    Returns:
        str: Sender display name for emails
    """
    try:
        config = read_config()
        return config.get('sender_name', 'IND Register Alerts')
    except (FileNotFoundError, json.JSONDecodeError):
        return 'IND Register Alerts'


def save_sender_name(name: str) -> None:
    """
    Save sender name to configuration file.
    
    Args:
        name (str): Sender display name to save
    """
    try:
        config = read_config()
    except (FileNotFoundError, json.JSONDecodeError):
        config = {'fetch_urls': [], 'mailing_list': []}
    
    config['sender_name'] = name.strip()
    write_config(config)

def get_url_objects(config_path: str = "src/config.json") -> List[Dict[str, str]]:
    """
    Get fetch URLs as standardized objects from configuration file.
    Converts old format (strings) to new format (dicts with name and url).
    
    Args:
        config_path (str): Path to the configuration file
        
    Returns:
        List[Dict[str, str]]: List of URL objects with 'name' and 'url' keys
        
    Raises:
        KeyError: If 'fetch_urls' key is not found in config
        ValueError: If URL object format is invalid
    """
    config = read_config(config_path)
    
    if 'fetch_urls' not in config:
        raise KeyError("'fetch_urls' key not found in configuration file")
    
    urls = config['fetch_urls']
    url_objects = []
    
    for i, url_item in enumerate(urls):
        if isinstance(url_item, str):
            # Old format: convert string URL to object
            # Generate a name from the URL or use index
            try:
                parsed = urlparse(url_item)
                domain = parsed.netloc.replace("www.", "").replace(".", "_")
                path_parts = [p for p in parsed.path.split('/') if p]
                if path_parts:
                    name = f"{domain}_{path_parts[-1]}"
                else:
                    name = f"{domain}_page"
            except:
                name = f"url_{i+1}"
            
            url_objects.append({
                "name": name,
                "url": url_item
            })
        elif isinstance(url_item, dict):
            # New format: validate required keys
            if 'name' not in url_item or 'url' not in url_item:
                raise ValueError(f"URL object at index {i} must contain 'name' and 'url' keys")
            
            url_objects.append({
                "name": url_item['name'],
                "url": url_item['url']
            })
        else:
            raise ValueError(f"Invalid URL item at index {i}: must be string or dict")
    
    return url_objects
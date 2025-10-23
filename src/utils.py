"""
Utility functions for reading configuration and handling common tasks.
"""
import json
import os
from typing import List, Dict


def read_config(config_path: str = "config.json") -> dict:
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
    if not os.path.isabs(config_path):
        # If relative path, make it relative to project root
        project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        config_path = os.path.join(project_root, config_path)
    
    try:
        with open(config_path, 'r', encoding='utf-8') as file:
            config = json.load(file)
        return config
    except FileNotFoundError:
        raise FileNotFoundError(f"Configuration file not found: {config_path}")
    except json.JSONDecodeError as e:
        raise json.JSONDecodeError(f"Invalid JSON in configuration file: {e}")


def get_url_objects(config_path: str = "config.json") -> List[Dict[str, str]]:
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
                from urllib.parse import urlparse
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
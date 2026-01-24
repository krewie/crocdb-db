"""
This module provides utilities for scraping web content and caching responses.
"""
import cloudscraper
import requests 
from utils import cache_manager
import socket
import requests.adapters
import urllib3.util.connection

CURL_HEADERS = {
    'User-Agent': 'curl/8.13.0',
    'Accept': '*/*'
}


def create_scraper_session(headers=None):
    """Create a scraper session and optionally apply custom headers."""
    session = cloudscraper.create_scraper()
    applied_headers = headers or CURL_HEADERS
    if applied_headers:
        session.headers.update(applied_headers)
    return session


def fetch_url(url, session=None, timeout=(15, 120)):
    if not session:
        session = create_scraper_session(CURL_HEADERS)

    try:
        r = session.get(url, timeout=timeout)
    except Exception as e:
        print(f"[FETCH][FAIL] {url} ({type(e).__name__})")
        return None   # <-- do NOT raise

    if not r.ok:
        return None

    response = r.text
    cache_manager.cache_response(url, response)
    return response



"""
This module provides functionality to scrape and parse entries from Myrient indexes.
It includes methods to fetch HTML responses, extract relevant data using regex, and
format the extracted data into structured entries.
"""
import re
import html
import sys
from utils import cache_manager
from utils.scrape_utils import fetch_url
from utils.parse_utils import size_bytes_to_str, size_str_to_bytes, join_urls
import requests
import threading
_thread_local = threading.local()

def get_session():
    if not hasattr(_thread_local, "session"):
        s = requests.Session()
        s.headers.update({
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5",
            "Accept-Encoding": "gzip, deflate, br",
            "Connection": "keep-alive",
            "Upgrade-Insecure-Requests": "1",
        })
        _thread_local.session = s
    return _thread_local.session

HOST_NAME = 'Myrient'


def extract_entries(response, source, platform, base_url):
    entries = []

    pattern = re.compile(
        r"<tr.*?>.*?"
        r"<td.*?class=\"link\".*?>.*?"
        r"<a.*?href=\"(.*?)\".*?>(.*?)</a>.*?"
        r"<td.*?class=\"size\".*?>(.*?)</td>",
        re.DOTALL | re.IGNORECASE
    )

    matches = pattern.findall(response)

    for link, raw_title, size_str in matches:
        raw_title = html.unescape(raw_title)

        match = re.search(source['filter'], raw_title)
        if not match:
            continue

        filename = raw_title
        title = match.group(1) if match.groups() else raw_title

        entries.append(
            create_entry(
                link,
                filename,
                title,
                size_str,
                source,
                platform,
                base_url,
            )
        )

    return entries



def create_entry(link, filename, title, size_str, source, platform, base_url):
    """Create a dictionary representing a single entry."""
    name = html.unescape(title)
    size = size_str_to_bytes(size_str)
    size_str = size_bytes_to_str(size)
    url = join_urls(base_url, link)

    return {
        'title': name,
        'platform': platform,
        'regions': source['regions'],
        'links': [
            {
                'name': name,
                'type': source['type'],
                'format': source['format'],
                'url': url,
                'filename': filename,
                'host': HOST_NAME,
                'size': size,
                'size_str': size_str,
                'source_url': base_url
            }
        ]
    }


def fetch_response(url, use_cached):
    if use_cached:
        cached = cache_manager.get_cached_response(url)
        if cached:
            return cached

    try:
        response = fetch_url(url, session=get_session())
        return response
    except Exception as e:
        print(f"[MYRIENT][FETCH FAIL] {url} ({type(e).__name__})")
        return None



def scrape(source, platform, use_cached=False):
    """Stream entries from Myrient based on the source configuration."""
    for url in source['urls']:
        response = fetch_response(url, use_cached)
        if not response:
            print(f"[MYRIENT] Failed to get response from {url}")
            continue

        found = False

        for entry in extract_entries(response, source, platform, url):
            found = True
            yield entry

        if not found:
            print(f"[MYRIENT] No entries parsed from {url}")


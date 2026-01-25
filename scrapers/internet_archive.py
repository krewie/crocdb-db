"""
Internet Archive scraper (metadata-based, directory-URL compatible).

This scraper interprets /download/.../A/ URLs as *path filters*,
NOT as fetch targets.
"""

import json
import html
import re
from urllib.parse import quote, urlparse

from utils import cache_manager
from utils.scrape_utils import fetch_url
from utils.parse_utils import size_bytes_to_str

HOST_NAME = "Internet Archive"
METADATA_URL = "https://archive.org/metadata"
DOWNLOAD_BASE = "https://archive.org/download"


def parse_ia_download_url(url):
    """
    Parse an IA /download URL into:
    - identifier
    - internal path prefix (may be empty)
    """
    path = urlparse(url).path.strip("/")

    # /download/{identifier}/optional/path/
    parts = path.split("/", 2)

    if len(parts) < 2 or parts[0] != "download":
        raise ValueError(f"Not an IA download URL: {url}")

    identifier = parts[1]
    prefix = ""

    if len(parts) == 3:
        prefix = parts[2].rstrip("/") + "/"

    return identifier, prefix


def build_download_url(identifier, name):
    return f"{DOWNLOAD_BASE}/{identifier}/{quote(name, safe='/')}"


def fetch_metadata(identifier, use_cached=False):
    url = f"{METADATA_URL}/{identifier}"

    if use_cached:
        cached = cache_manager.get_cached_response(url)
        if cached:
            return json.loads(cached)

    response = fetch_url(url)
    if not response:
        return None

    cache_manager.cache_response(url, response)
    return json.loads(response)

# Delivery containers that should never be part of identity
CONTAINER_EXTENSIONS = (
    ".zip",
    ".7z",
    ".pkg",
    ".chd",
    ".rap",
    ".iso",
    ".nds",
    ".wad",
    ".wbfs",
    ".wua"
)

# Rare but safe to handle if they appear
DOUBLE_CONTAINER_EXTENSIONS = (
    ".iso.zip",
    ".bin.zip",
    ".cue.zip",
)

def strip_extension(filename: str) -> str:
    lower = filename.lower()

    # Handle double-container cases first (insurance)
    for ext in DOUBLE_CONTAINER_EXTENSIONS:
        if lower.endswith(ext):
            return filename[:-len(ext)]

    # Strip only true containers
    for ext in CONTAINER_EXTENSIONS:
        if lower.endswith(ext):
            return filename[:-len(ext)]

    # Everything else is identity-relevant
    return filename


def create_entry(identifier, file_obj, source, platform):
    raw_name = html.unescape(file_obj["name"])
    filename = raw_name.split("/")[-1]
    title = strip_extension(filename)

    size = int(file_obj.get("size", 0))
    size_str = size_bytes_to_str(size)

    url = build_download_url(identifier, file_obj["name"])
    return {
        "title": title,
        "platform": platform,
        "regions": source["regions"],
        "links": [
            {
                "name": title,
                "type": source["type"],
                "format": source["format"],
                "url": url,
                "filename": filename,
                "host": HOST_NAME,
                "size": size,
                "size_str": size_str,
                "source_url": f"{DOWNLOAD_BASE}/{identifier}",
            }
        ],
    }


def scrape(source, platform, use_cached=False):
    entries = []

    # Compile filter once
    filter_pattern = re.compile(source["filter"], re.IGNORECASE)

    # Parse all source URLs into (identifier, prefix)
    parsed_sources = []
    for url in source["urls"]:
        identifier, prefix = parse_ia_download_url(url)
        parsed_sources.append((identifier, prefix))

    # Group prefixes per identifier
    prefixes_by_id = {}
    for identifier, prefix in parsed_sources:
        prefixes_by_id.setdefault(identifier, set()).add(prefix)

    # Process each IA item once
    for identifier, prefixes in prefixes_by_id.items():
        metadata = fetch_metadata(identifier, use_cached)
        if not metadata or "files" not in metadata:
            print(f"[IA] Failed to fetch metadata for {identifier}")
            continue

        for file_obj in metadata["files"]:
            if file_obj.get("source") != "original":
                continue

            name = file_obj.get("name", "")
            if not name:
                continue

            # Must match one of the directory prefixes
            if not any(name.startswith(prefix) for prefix in prefixes):
                continue

            basename = name.split("/")[-1]
            if not filter_pattern.match(basename):
                continue

            entry = create_entry(identifier, file_obj, source, platform)
            entries.append(entry)

    return entries
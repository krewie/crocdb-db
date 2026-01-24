#!/usr/bin/env python
"""
This script is responsible for initializing a database, processing sources for scraping and parsing,
and moving generated static files to a specified directory. It integrates various scrapers and parsers
to handle data from multiple platforms and formats.
"""
import json
import sys
import os
import shutil
import threading
import queue
import time
from parsers import no_intro
from scrapers import myrient, internet_archive, nopaystation, mariocube
from parsers import libretro, gametdb, mame, wii_rom_set_by_ghostware
from database import db_manager

class Stats:
    def __init__(self):
        self.lock = threading.Lock()
        self.enqueued = 0
        self.written = 0

stats = Stats()

SCRAPERS = {
    'myrient': myrient,
    'internet_archive': internet_archive,
    'nopaystation': nopaystation,
    'mariocube': mariocube
}

PARSERS = {
    'no_intro': no_intro,
    'libretro': libretro,
    'gametdb': gametdb,
    'mame': mame,
    'wii_rom_set_by_ghostware': wii_rom_set_by_ghostware
}


def load_sources(file_path='sources.json'):
    """Load sources from a JSON file."""
    with open(file_path, 'r') as file:
        return json.load(file)


def load_config(file_path='config.json'):
    """Load configuration from a JSON file."""
    with open(file_path, 'r') as file:
        return json.load(file)


def get_scraper(name):
    """Retrieve a scraper by its name."""
    return SCRAPERS.get(name)


def get_parser(name):
    """Retrieve a parser by its name."""
    return PARSERS.get(name)

def monitor_worker(entry_queue, stop_event):
    last_written = 0
    last_enqueued = 0
    last_time = time.time()

    while not stop_event.is_set():
        time.sleep(2)

        with stats.lock:
            enq = stats.enqueued
            wr = stats.written

        now = time.time()
        dt = max(now - last_time, 0.001)

        write_rate = (wr - last_written) / dt
        enqueue_rate = (enq - last_enqueued) / dt
        backlog = enq - wr
        backlog_delta = (enq - last_enqueued) - (wr - last_written)

        trend = (
            "↑ growing" if backlog_delta > 0 else
            "↓ draining" if backlog_delta < 0 else
            "→ stable"
        )
        
        if enqueue_rate == 0 and write_rate == 0 and entry_queue.qsize() == 0:
            continue


        print(
            f"[STATS] "
            f"enq={enq} wr={wr} "
            f"queue={entry_queue.qsize()} "
            f"backlog={backlog:+} ({trend}) "
            f"enq_rate={enqueue_rate:.1f}/s "
            f"wr_rate={write_rate:.1f}/s"
        )

        last_written = wr
        last_enqueued = enq
        last_time = now



## One writer, one db connection, serialized writes...
def db_writer_worker(entry_queue, stop_event):
    db_manager.init_database()
    last_report = time.time()

    try:
        while not stop_event.is_set() or not entry_queue.empty():
            try:
                entry = entry_queue.get(timeout=0.5)
            except queue.Empty:
                continue

            db_manager.insert_entry(entry)

            with stats.lock:
                stats.written += 1

            entry_queue.task_done()

            # Periodic progress report (UNCHANGED)
            now = time.time()
            if now - last_report >= 2.0:
                with stats.lock:
                    qsize = entry_queue.qsize()
                    enq = stats.enqueued
                    wr = stats.written

                print(
                    f"[DB] written={wr} "
                    f"queue={qsize} "
                    f"backlog={enq - wr}"
                )
                last_report = now

    finally:
        db_manager.close_database()
        print("[DB] database closed")




def process_sources(sources, use_cached, entry_queue):
    """Process the sources to scrape, parse, and insert data into the database."""
    for platform, source_list in sources.items():
        print(f"\n{platform}:")
        for i, source in enumerate(source_list, start=1):
            print(f"  {i}) ", end='')
            print(f"[{source['format']}] ", end='')
            if source['regions']:
                print(f"[{', '.join(source['regions'])}] ", end='')
            print(f"[{source['scraper']}] ", end='')
            print(f"[{source['type']}]")

            scraper = get_scraper(source['scraper'])
            if not scraper:
                raise RuntimeError(f"Scraper '{source['scraper']}' not found")

            entries = scraper.scrape(source, platform, use_cached)

            for parser_name, parser_flags in source['parsers'].items():
                print(parser_name)
                parser = get_parser(parser_name)
                if not parser:
                    raise RuntimeError(f"Parser '{parser_name}' not found")
                entries = parser.parse(entries, parser_flags)

            for entry in entries:
                entry_queue.put(entry)
                with stats.lock:
                    stats.enqueued += 1


def move_static_files(destination_dir, static_dir='static'):
    """Move the contents of the static directory to the destination directory, overwriting if necessary."""
    if not os.path.exists(static_dir):
        return

    if not os.path.exists(destination_dir):
        os.makedirs(destination_dir)

    for item in os.listdir(static_dir):
        source_path = os.path.join(static_dir, item)
        destination_path = os.path.join(destination_dir, item)

        if os.path.exists(destination_path):
            # Remove existing file or directory
            if os.path.isdir(destination_path):
                shutil.rmtree(destination_path)
            else:
                os.remove(destination_path)

        # Move the source to the destination
        shutil.move(source_path, destination_dir)


def make(use_cached=False):
    """Main function to initialize the database, process sources, and close the database."""
    config = load_config()
    sources = load_sources()

    entry_queue = queue.Queue(maxsize=2000)
    stop_event = threading.Event()

    writer_thread = threading.Thread(
        target=db_writer_worker,
        args=(entry_queue, stop_event),
        daemon=True
    )
    writer_thread.start()

    ## monitor thread

    monitor_thread = threading.Thread(
        target=monitor_worker,
        args=(entry_queue, stop_event),
        daemon=True
    )
    monitor_thread.start()

    try:
        process_sources(sources, use_cached, entry_queue)
    finally:
        entry_queue.join()
        stop_event.set()
        writer_thread.join()

    static_files_dir_path = config.get('static_files_dir_path')
    if static_files_dir_path:
        move_static_files(static_files_dir_path)
        print(f"Static files moved to '{static_files_dir_path}'.")


if __name__ == '__main__':
    # Change directory to script location
    os.chdir(os.path.dirname(os.path.realpath(__file__)))

    args = sys.argv[1:] if len(sys.argv) > 1 else []
    use_cached = '--use-cached' in args

    make(use_cached)

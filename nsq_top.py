#!/usr/bin/env python3
import argparse
import os
import requests
import time
import sys
import atexit
from blessed import Terminal
from datetime import datetime

# --- Configuration ---
# Characters to use for the simple in-flight message trend graph
SPARKLINE_CHARS = " ▂▃▄▅▆▇█"
# History length for the sparkline
SPARKLINE_LENGTH = 60
# Thresholds for coloring the backlog depth
DEPTH_WARN_THRESHOLD = 100
DEPTH_CRIT_THRESHOLD = 1000

def get_nsqd_nodes(lookupd_urls):
    """
    Fetches the list of active nsqd producers from one or more nsqlookupd instances.
    """
    all_producers = []
    errors = []
    
    for lookupd_url in lookupd_urls:
        try:
            response = requests.get(f"{lookupd_url}/nodes", timeout=2)
            response.raise_for_status()
            producers = response.json().get('producers', [])
            all_producers.extend(producers)
        except requests.exceptions.RequestException as e:
            errors.append(f"Failed to connect to {lookupd_url}: {e}")
        except ValueError:
            errors.append(f"Invalid JSON response from {lookupd_url}")
    
    if not all_producers and errors:
        return {'error': "; ".join(errors)}
    
    # Remove duplicates based on broadcast_address and http_port
    unique_producers = []
    seen = set()
    for producer in all_producers:
        key = (producer.get('broadcast_address'), producer.get('http_port'))
        if key not in seen:
            seen.add(key)
            unique_producers.append(producer)
    
    return unique_producers

def get_all_stats(nodes):
    """
    Fetches /stats from a list of nsqd nodes.
    """
    all_stats = []
    for producer in nodes:
        nsqd_http = f"http://{producer['broadcast_address']}:{producer['http_port']}"
        try:
            response = requests.get(f"{nsqd_http}/stats?format=json", timeout=2)
            response.raise_for_status()
            stats = response.json()
            # The 'version' key was removed in nsqd > 1.0, but the new stats are nested under 'data'
            # This handles both old and new formats.
            all_stats.append(stats.get('data', stats))
        except (requests.exceptions.RequestException, ValueError):
            # Ignore nodes that fail to respond; they might be down.
            continue
    return all_stats

def process_stats(nsqd_stats, previous_stats=None, interval_seconds=2):
    """
    Aggregates stats from all nsqd nodes into a single, sorted list.
    """
    channel_data = {}
    total_in_flight = 0

    for stats in nsqd_stats:
        for topic in stats.get('topics', []):
            for channel in topic.get('channels', []):
                channel_key = f"{topic['topic_name']}/{channel['channel_name']}"
                if channel_key not in channel_data:
                    channel_data[channel_key] = {
                        'topic': topic['topic_name'],
                        'channel': channel['channel_name'],
                        'depth': 0,
                        'in_flight_count': 0,
                        'message_count': channel.get('message_count', 0),
                    }
                
                # Sum up stats from different nsqd nodes for the same channel
                channel_data[channel_key]['depth'] += channel['depth'] + channel['backend_depth']
                channel_data[channel_key]['in_flight_count'] += channel['in_flight_count']
                channel_data[channel_key]['message_count'] += channel.get('message_count', 0)
                total_in_flight += channel['in_flight_count']

    # Calculate message rate (messages per second and per minute) if we have previous stats
    if previous_stats:
        for channel_key in channel_data:
            current_count = channel_data[channel_key]['message_count']
            previous_count = previous_stats.get(channel_key, {}).get('message_count', current_count)
            # Calculate rate based on actual time interval
            messages_per_second = max(0, current_count - previous_count) / interval_seconds
            messages_per_minute = messages_per_second * 60
            channel_data[channel_key]['rate_per_second'] = messages_per_second
            channel_data[channel_key]['rate_per_minute'] = messages_per_minute
    else:
        for channel_key in channel_data:
            channel_data[channel_key]['rate_per_second'] = 0
            channel_data[channel_key]['rate_per_minute'] = 0

    # Sort channels by depth in descending order
    sorted_channels = sorted(channel_data.values(), key=lambda x: x['depth'], reverse=True)
    return sorted_channels, total_in_flight

def get_sparkline(history):
    """
    Generates a simple sparkline from a list of numbers.
    """
    if not history:
        return ""
    
    max_val = max(history) if max(history) > 0 else 1
    # Scale values to the range of sparkline characters
    scaled_data = [int(((value / max_val) * (len(SPARKLINE_CHARS) - 1))) for value in history]
    return "".join(SPARKLINE_CHARS[i] for i in scaled_data)

def draw_ui(term, channels, total_in_flight, inflight_history, lookupd_urls, error=None):
    """
    Draws the TUI interface.
    """
    with term.location():
        print(term.clear)
        
        # Header
        lookupd_display = ", ".join(lookupd_urls) if len(lookupd_urls) <= 3 else f"{len(lookupd_urls)} servers"
        header = f" NSQ Top - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} - Connected to {lookupd_display} "
        print(term.bold_white_on_blue(header.center(term.width)))
        print()
        
        # Error message if any
        if error:
            print(term.bold_red(f"Error: {error}"))
            print()
            return
        
        # Summary stats
        sparkline = get_sparkline(inflight_history)
        summary = f"Total In-Flight: {total_in_flight:,} | Channels: {len(channels)} | Trend: {sparkline}"
        print(term.bold_yellow(summary))
        print()
        
        # Calculate column widths based on terminal width
        available_width = term.width - 6  # Leave some margin
        depth_width = 9
        inflight_width = 9
        rate_sec_width = 8
        rate_min_width = 8
        
        # Calculate Topic/Channel width to use all remaining space
        fixed_columns_width = depth_width + inflight_width + rate_sec_width + rate_min_width + 10  # 10 for separators
        topic_channel_width = available_width - fixed_columns_width
        
        # Ensure minimum width but allow it to expand to fill the screen
        topic_channel_width = max(topic_channel_width, 25)  # Minimum 25 chars
        
        # Table header with borders
        separator = "+" + "-" * (topic_channel_width + 2) + "+" + "-" * (depth_width + 2) + "+" + "-" * (inflight_width + 2) + "+" + "-" * (rate_sec_width + 2) + "+" + "-" * (rate_min_width + 2) + "+"
        header_row = f"| {'Topic/Channel':<{topic_channel_width}} | {'Depth':>{depth_width}} | {'In-Flight':>{inflight_width}} | {'Rate/sec':>{rate_sec_width}} | {'Rate/min':>{rate_min_width}} |"
        
        print(term.bold_white(separator))
        print(term.bold_white(header_row))
        print(term.bold_white(separator))
        
        # Channel data
        displayed_rows = 0
        max_rows = term.height - 12  # Reserve space for header, summary, and table borders
        
        for channel in channels[:max_rows]:
            topic_channel = f"{channel['topic']}/{channel['channel']}"
            
            # Truncate from the start if too long, keeping the end (channel name) visible
            if len(topic_channel) > topic_channel_width:
                topic_channel = "..." + topic_channel[-(topic_channel_width-3):]
            
            depth = channel['depth']
            in_flight = channel['in_flight_count']
            rate_sec = channel.get('rate_per_second', 0)
            rate_min = channel.get('rate_per_minute', 0)
            
            # Color coding based on depth
            if depth >= DEPTH_CRIT_THRESHOLD:
                depth_str = term.bold_red(f"{depth:>{depth_width},}")
            elif depth >= DEPTH_WARN_THRESHOLD:
                depth_str = term.bold_yellow(f"{depth:>{depth_width},}")
            else:
                depth_str = term.green(f"{depth:>{depth_width},}")
            
            # Format rates
            if rate_sec > 0:
                rate_sec_str = f"{rate_sec:>{rate_sec_width},.1f}"
            else:
                rate_sec_str = f"{'--':>{rate_sec_width}}"
                
            if rate_min > 0:
                rate_min_str = f"{rate_min:>{rate_min_width},.0f}"
            else:
                rate_min_str = f"{'--':>{rate_min_width}}"
            
            # Format the row with proper alignment
            row = f"| {topic_channel:<{topic_channel_width}} | {depth_str} | {in_flight:>{inflight_width},} | {rate_sec_str} | {rate_min_str} |"
            print(row)
            displayed_rows += 1
        
        # Bottom border
        print(term.bold_white(separator))
        
        # Show if there are more channels
        if len(channels) > max_rows:
            remaining = len(channels) - max_rows
            print(f"... and {remaining} more channels")
        
        # Footer
        print()
        print("Press Ctrl+C to exit")


def main():
    """
    Main application entry point.
    """
    parser = argparse.ArgumentParser(description="A `top`-like monitoring tool for NSQ clusters.")
    
    # Get defaults from environment variables
    default_lookupd = os.environ.get('NSQ_LOOKUPD_ADDRESSES', os.environ.get('NSQ_LOOKUPD_ADDRESS', ''))
    default_interval = os.environ.get('NSQ_TOP_INTERVAL', 2)
    
    # Normalize environment variable URLs too
    if default_lookupd:
        env_urls = [url.strip() for url in default_lookupd.split(',') if url.strip()]
        normalized_env_urls = []
        for url in env_urls:
            if not url.startswith(('http://', 'https://')):
                url = f"http://{url}"
            normalized_env_urls.append(url)
        default_lookupd = ','.join(normalized_env_urls)

    parser.add_argument(
        '--lookupd-http-address',
        default=default_lookupd,
        help="Comma-separated HTTP addresses of nsqlookupd instances (e.g., localhost:4161 or http://localhost:4161). "
             "http:// prefix will be added automatically if not specified. "
             "Can also be set with the NSQ_LOOKUPD_ADDRESSES environment variable (or NSQ_LOOKUPD_ADDRESS for single server)."
    )
    parser.add_argument(
        '--interval',
        type=int,
        default=default_interval,
        help="Refresh interval in seconds. "
             "Can also be set with the NSQ_TOP_INTERVAL environment variable."
    )
    args = parser.parse_args()

    if not args.lookupd_http_address:
        parser.error("the --lookupd-http-address argument is required, or set NSQ_LOOKUPD_ADDRESSES environment variable")

    # Parse comma-separated lookupd addresses
    lookupd_urls = [url.strip() for url in args.lookupd_http_address.split(',') if url.strip()]
    if not lookupd_urls:
        parser.error("at least one valid lookupd address is required")
    
    # Add http:// prefix if not present
    normalized_urls = []
    for url in lookupd_urls:
        if not url.startswith(('http://', 'https://')):
            url = f"http://{url}"
        normalized_urls.append(url)
    lookupd_urls = normalized_urls


    term = Terminal()
    inflight_history = []
    previous_channel_stats = None

    # Ensure cursor is shown on exit
    atexit.register(lambda: print(term.normal_cursor()))

    with term.fullscreen(), term.cbreak(), term.hidden_cursor():
        while True:
            error_message = None
            channels = []
            total_in_flight = 0

            nodes = get_nsqd_nodes(lookupd_urls)
            if isinstance(nodes, dict) and 'error' in nodes:
                 error_message = nodes['error']
            else:
                stats = get_all_stats(nodes)
                channels, total_in_flight = process_stats(stats, previous_channel_stats, args.interval)
                
                # Store current stats for next iteration
                previous_channel_stats = {f"{ch['topic']}/{ch['channel']}": ch for ch in channels}
            
            # Update history for the sparkline
            inflight_history.append(total_in_flight)
            if len(inflight_history) > SPARKLINE_LENGTH:
                inflight_history.pop(0)

            draw_ui(term, channels, total_in_flight, inflight_history, lookupd_urls, error_message)
            
            time.sleep(args.interval)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        # The atexit handler will clean up the cursor
        print("\nExiting.")
        sys.exit(0)

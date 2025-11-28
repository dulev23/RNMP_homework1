import json
from kafka import KafkaConsumer, KafkaProducer
from collections import defaultdict
import time

# Configuration
WINDOW_SIZE_MS = 5000  # X ms - 5 seconds (можеш да го промениш)
SLIDE_SIZE_MS = 2000  # Y ms - 2 seconds (можеш да го промениш)
bootstrap_servers = 'localhost:9092'

# Kafka Consumer and Producers
consumer = KafkaConsumer(
    'sensors',  # Input topic
    bootstrap_servers=bootstrap_servers,
    group_id='pyflink_window_processor',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    value_deserializer=lambda m: json.loads(m.decode('utf-8'))
)

producer_result1 = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

producer_result2 = KafkaProducer(
    bootstrap_servers=bootstrap_servers,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Window storage: {key: {window_start: [values]}}
windows = defaultdict(lambda: defaultdict(list))


def get_window_start(timestamp_ms):
    """Calculate window start based on tumbling/sliding window logic"""
    # For tumbling window: window_start = (timestamp // WINDOW_SIZE_MS) * WINDOW_SIZE_MS
    # For sliding window, we need to assign to multiple windows
    return (timestamp_ms // SLIDE_SIZE_MS) * SLIDE_SIZE_MS


def get_all_window_starts(timestamp_ms):
    """Get all window starts that this timestamp belongs to (for sliding windows)"""
    windows_list = []
    current_window = get_window_start(timestamp_ms)

    # Go back to find all windows this timestamp belongs to
    num_windows = (WINDOW_SIZE_MS // SLIDE_SIZE_MS)
    for i in range(num_windows):
        window_start = current_window - (i * SLIDE_SIZE_MS)
        window_end = window_start + WINDOW_SIZE_MS
        if timestamp_ms >= window_start and timestamp_ms < window_end:
            windows_list.append(window_start)

    return windows_list


def process_window(key, window_start, values):
    """Process a complete window and send results"""
    window_end = window_start + WINDOW_SIZE_MS
    count = len(values)

    # Result 1: Count of messages
    result1 = {
        'key': key,
        'window_start': window_start,
        'window_end': window_end,
        'count': count
    }
    producer_result1.send('results1', value=result1)
    print(f"[RESULTS1] Key: {key}, Window: [{window_start}-{window_end}], Count: {count}")

    # Result 2: Aggregated statistics
    if count > 0:
        result2 = {
            'key': key,
            'window_start': window_start,
            'window_end': window_end,
            'min_value': min(values),
            'count': count,
            'average': round(sum(values) / count, 2),
            'max_value': max(values)
        }
        producer_result2.send('results2', value=result2)
        print(f"[RESULTS2] Key: {key}, Window: [{window_start}-{window_end}], "
              f"Min: {result2['min_value']}, Avg: {result2['average']}, Max: {result2['max_value']}")


def check_and_emit_windows(current_time):
    """Check which windows are complete and emit them"""
    windows_to_remove = []

    for key in list(windows.keys()):
        for window_start in list(windows[key].keys()):
            window_end = window_start + WINDOW_SIZE_MS

            # If window has ended (current time is past window end + some buffer)
            if current_time >= window_end + SLIDE_SIZE_MS:
                values = windows[key][window_start]
                process_window(key, window_start, values)
                windows_to_remove.append((key, window_start))

    # Clean up processed windows
    for key, window_start in windows_to_remove:
        del windows[key][window_start]
        if not windows[key]:
            del windows[key]


print(f"PyFlink Application Started")
print(f"Window Size: {WINDOW_SIZE_MS}ms, Slide Size: {SLIDE_SIZE_MS}ms")
print(f"Listening to topic: sensors")
print(f"Output topics: results1, results2\n")

last_check_time = time.time() * 1000

try:
    for message in consumer:
        data = message.value
        key = data.get('key', 'unknown')
        value = data.get('value', 0)
        timestamp = data.get('timestamp', int(time.time() * 1000))

        print(f"Received: Key={key}, Value={value}, Timestamp={timestamp}")

        # Assign to all applicable windows (for sliding windows)
        window_starts = get_all_window_starts(timestamp)
        for window_start in window_starts:
            windows[key][window_start].append(value)

        # Periodically check and emit complete windows
        current_time = time.time() * 1000
        if current_time - last_check_time > SLIDE_SIZE_MS:
            check_and_emit_windows(current_time)
            last_check_time = current_time

except KeyboardInterrupt:
    print("\nShutting down...")
    # Emit remaining windows
    check_and_emit_windows(time.time() * 1000 + WINDOW_SIZE_MS * 2)
finally:
    consumer.close()
    producer_result1.close()
    producer_result2.close()
    print("Application closed.")
from prometheus_client import start_http_server, Summary, Counter
import random
import time

# Create a metric to track time spent and requests made
REQUEST_TIME = Summary('request_processing_seconds', 'Time spent processing request')
REQUEST_COUNT = Counter('request_count', 'Number of requests')


# Decorate function with metric
@REQUEST_TIME.time()
def process_request(t):
    """A dummy function that takes some time."""
    time.sleep(t)


def test_server():
    start_http_server(8000)
    REQUEST_COUNT.inc()

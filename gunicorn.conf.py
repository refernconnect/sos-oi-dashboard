import threading

def post_fork(server, worker):
    """Start background data fetcher after gunicorn forks."""
    from app import refresh_data
    t = threading.Thread(target=refresh_data, daemon=True)
    t.start()

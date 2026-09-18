import os
import threading


def post_fork(server, worker):
    """After gunicorn forks the single worker, start background threads:
    - refresh_data: the OI dashboard's own data fetcher (existing behaviour)
    - cas_watch.main: the CAS auction logger + option-chain recorder (v2).
      Runs here because this is the only always-on service with DATABASE_URL.
      Guarded so a recorder failure can never take down the web dashboard.
    """
    from app import refresh_data
    threading.Thread(target=refresh_data, daemon=True).start()

    if os.getenv("CAS_WATCH_ENABLED", "1") == "1":
        def _run_recorder():
            try:
                import cas_watch
                cas_watch.main()
            except SystemExit as e:
                print("[cas_watch] exited:", e)
            except Exception as e:
                print("[cas_watch] crashed:", e)
        threading.Thread(target=_run_recorder, daemon=True, name="cas_watch").start()
        print("[cas_watch] launch thread started")
    else:
        print("[cas_watch] disabled via CAS_WATCH_ENABLED=0")

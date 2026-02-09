import os
import threading
import time
import logging
from django.apps import AppConfig

logger = logging.getLogger(__name__)

_sync_thread_started = False


class MariaCacheConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'maria_cache'

    def ready(self):
        global _sync_thread_started
        if _sync_thread_started:
            return
        if os.getenv('MARIA_CACHE_AUTO_SYNC', '1') != '1':
            return
        if os.getenv('RUN_MAIN') != 'true' and os.getenv('WERKZEUG_RUN_MAIN') != 'true':
            return

        interval = int(os.getenv('MARIA_CACHE_INTERVAL_SEC', '300'))

        def _loop():
            while True:
                try:
                    from .sync import sync_reference_tables
                    sync_reference_tables()
                except Exception as exc:
                    logger.warning("Auto sync failed: %s", exc)
                time.sleep(interval)

        thread = threading.Thread(target=_loop, name='maria-cache-sync', daemon=True)
        thread.start()
        _sync_thread_started = True

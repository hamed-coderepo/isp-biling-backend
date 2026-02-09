import os
import atexit
from django.apps import AppConfig


def _should_start_scheduler():
    # Avoid double-start with runserver auto-reloader
    return os.environ.get('RUN_MAIN') == 'true' or os.environ.get('WERKZEUG_RUN_MAIN') == 'true'


class ReportsConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'reports'

    def ready(self):
        if not _should_start_scheduler():
            return

        from apscheduler.schedulers.background import BackgroundScheduler
        from .sync import sync_maria_to_bigquery, log_sync_event
        from maria_cache.sync import sync_reference_tables

        scheduler = BackgroundScheduler()

        if os.getenv('AUTO_SYNC_ENABLED', '0') == '1':
            interval = int(os.getenv('AUTO_SYNC_INTERVAL_MINUTES', '30'))
            days = int(os.getenv('AUTO_SYNC_DAYS', '90'))

            def _auto_sync_job():
                log_sync_event(
                    'auto_sync_start',
                    'Auto sync triggered',
                    interval_minutes=interval,
                    days=days,
                    auto=True,
                )
                return sync_maria_to_bigquery(days=days, auto=True)

            scheduler.add_job(
                _auto_sync_job,
                'interval',
                minutes=interval,
                id='maria_to_bq',
            )

        if os.getenv('CACHE_SYNC_ENABLED', '1') == '1':
            cache_interval = int(os.getenv('CACHE_SYNC_INTERVAL_MINUTES', '5'))
            scheduler.add_job(
                sync_reference_tables,
                'interval',
                minutes=cache_interval,
                id='maria_to_cache',
            )

        scheduler.start()
        atexit.register(lambda: scheduler.shutdown(wait=False))

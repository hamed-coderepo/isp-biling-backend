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
        if os.getenv('AUTO_SYNC_ENABLED', '0') != '1':
            return
        if not _should_start_scheduler():
            return

        from apscheduler.schedulers.background import BackgroundScheduler
        from .sync import sync_maria_to_bigquery

        interval = int(os.getenv('AUTO_SYNC_INTERVAL_MINUTES', '30'))
        days = int(os.getenv('AUTO_SYNC_DAYS', '90'))
        scheduler = BackgroundScheduler()
        scheduler.add_job(
            sync_maria_to_bigquery,
            'interval',
            minutes=interval,
            id='maria_to_bq',
            kwargs={'days': days},
        )
        scheduler.start()
        atexit.register(lambda: scheduler.shutdown(wait=False))

from django.core.management.base import BaseCommand
from reports.sync import sync_maria_to_bigquery


class Command(BaseCommand):
    help = 'Sync MariaDB report rows into BigQuery (full refresh by default)'

    def add_arguments(self, parser):
        parser.add_argument('--limit', type=int, default=0, help='Optional row limit (0 = no limit)')
        parser.add_argument('--days', type=int, default=0, help='Sync rows from the last N days (0 = all)')
        parser.add_argument('--write-disposition', type=str, default='WRITE_TRUNCATE',
                            help='BigQuery write disposition (WRITE_TRUNCATE or WRITE_APPEND)')

    def handle(self, *args, **options):
        limit = options['limit']
        days = options['days']
        write_disposition = options['write_disposition']
        days_value = days if days and days > 0 else None
        rows = sync_maria_to_bigquery(limit=limit, write_disposition=write_disposition, days=days_value)
        if rows == 0:
            self.stdout.write(self.style.WARNING('No rows returned from MariaDB.'))
        else:
            self.stdout.write(self.style.SUCCESS(f'Synced {rows} rows to BigQuery'))

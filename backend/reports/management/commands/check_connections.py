import os

from django.core.management.base import BaseCommand, CommandError
import pymysql
from pymysql.cursors import DictCursor

from reports.bq import get_bq_client, get_bq_table_id
from reports.sync import _parse_sources


class Command(BaseCommand):
    help = 'Check connectivity to MariaDB sources and BigQuery'

    def add_arguments(self, parser):
        parser.add_argument('--maria-only', action='store_true', help='Check MariaDB connections only')
        parser.add_argument('--bq-only', action='store_true', help='Check BigQuery connection only')
        parser.add_argument('--timeout', type=int, default=5, help='Connection timeout in seconds (default: 5)')

    def handle(self, *args, **options):
        maria_only = options['maria_only']
        bq_only = options['bq_only']
        timeout = options['timeout']

        if maria_only and bq_only:
            raise CommandError('Choose only one of --maria-only or --bq-only.')

        failures = []

        if not bq_only:
            self.stdout.write('Checking MariaDB sources...')
            sources = _parse_sources()
            if not sources:
                failures.append('No MariaDB sources configured (MARIA_* or MARIA_SOURCES).')
            for source in sources:
                name = source.get('name') or source.get('host')
                ok, error = self._check_maria_source(source, timeout)
                if ok:
                    self.stdout.write(self.style.SUCCESS(f'  OK: {name}'))
                else:
                    failures.append(f'MariaDB {name} failed: {error}')
                    self.stdout.write(self.style.ERROR(f'  FAIL: {name} ({error})'))

        if not maria_only:
            self.stdout.write('Checking BigQuery...')
            ok, error = self._check_bigquery()
            if ok:
                self.stdout.write(self.style.SUCCESS('  OK: BigQuery access'))
            else:
                failures.append(f'BigQuery failed: {error}')
                self.stdout.write(self.style.ERROR(f'  FAIL: BigQuery ({error})'))

        if failures:
            raise CommandError('Connection checks failed.')

    def _check_maria_source(self, source, timeout):
        cfg = {
            'host': source.get('host') or 'localhost',
            'port': int(source.get('port') or 3306),
            'user': source.get('user') or 'root',
            'password': source.get('password') or '',
            'db': source.get('db') or '',
            'charset': 'utf8mb4',
            'cursorclass': DictCursor,
            'connect_timeout': timeout,
            'read_timeout': timeout,
            'write_timeout': timeout,
        }
        conn = None
        try:
            conn = pymysql.connect(**cfg)
            with conn.cursor() as cur:
                cur.execute('SELECT 1')
                cur.fetchone()
            return True, None
        except Exception as exc:
            return False, str(exc)
        finally:
            if conn:
                conn.close()

    def _check_bigquery(self):
        creds_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', '').strip()
        if creds_path and not os.path.exists(creds_path):
            return False, f'Credentials file not found at {creds_path}'
        try:
            table_id = get_bq_table_id()
        except Exception as exc:
            return False, str(exc)

        try:
            client = get_bq_client()
            client.get_table(table_id)
            return True, None
        except Exception as exc:
            return False, str(exc)

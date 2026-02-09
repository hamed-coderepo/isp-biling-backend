import datetime

from django.core.management.base import BaseCommand, CommandError
import pandas as pd
import pymysql
from pymysql.cursors import DictCursor

from reports.sync import _parse_sources
from reports.views import export_df_to_pdf


class Command(BaseCommand):
    help = 'Generate a MariaDB PDF report from a single deltasib line'

    def add_arguments(self, parser):
        parser.add_argument('line', type=str, help='Single deltasib line (quoted)')
        parser.add_argument('--output', type=str, default='', help='Output PDF path')
        parser.add_argument('--limit', type=int, default=0, help='Limit rows (optional)')
        parser.add_argument('--timeout', type=int, default=10, help='DB timeout in seconds (default: 10)')

    def handle(self, *args, **options):
        line = options['line']
        parsed = self._parse_line(line)
        rs_username = parsed['rs_username']
        source_name = parsed.get('source_name')

        date_start, date_end, source = self._resolve_date_range(rs_username, source_name, options['timeout'])
        output_path = options.get('output') or f"report_{rs_username}_{date_start}_to_{date_end}.pdf"
        limit = options.get('limit') or 0
        timeout = options.get('timeout')

        df = self._fetch_report_rows(
            rs_username=rs_username,
            date_start=date_start,
            date_end=date_end,
            source=source,
            limit=limit,
            timeout=timeout,
        )

        if df.empty:
            self.stdout.write('No rows returned for this filter.')
            return

        df = self._append_totals(df)

        pdf_data = export_df_to_pdf(df)
        if not pdf_data:
            self.stdout.write('Failed to generate PDF.')
            return

        with open(output_path, 'wb') as handle:
            handle.write(pdf_data)

        self.stdout.write(f'PDF generated: {output_path}')

    def _parse_line(self, line):
        parts = line.strip().split()
        if len(parts) < 6:
            raise CommandError('Invalid deltasib line. Expected at least 6 space-separated fields.')

        rs_username = parts[4].strip()
        source_name = parts[5].strip() if len(parts) > 5 else ''

        return {
            'rs_username': rs_username,
            'source_name': source_name,
        }

    def _resolve_date_range(self, rs_username, source_name, timeout):
        source = self._select_source(source_name)
        min_date = self._fetch_first_create_date(rs_username, source, timeout)
        if min_date is None:
            raise CommandError('No data found for this reseller in MariaDB.')

        today = datetime.date.today()
        return min_date, today, source

    def _select_source(self, source_name):
        sources = _parse_sources()
        if not source_name:
            return sources[0] if sources else None

        for source in sources:
            if str(source.get('name', '')).strip().lower() == source_name.strip().lower():
                return source
        return sources[0] if sources else None

    def _get_conn(self, source, timeout):
        if source is None:
            raise CommandError('No MariaDB source configured.')
        return pymysql.connect(
            host=source.get('host') or 'localhost',
            port=int(source.get('port') or 3306),
            user=source.get('user') or 'root',
            password=source.get('password') or '',
            db=source.get('db') or '',
            charset='utf8mb4',
            cursorclass=DictCursor,
            connect_timeout=timeout,
            read_timeout=timeout,
            write_timeout=timeout,
        )

    def _fetch_first_create_date(self, rs_username, source, timeout):
        query = """
SELECT MIN(TName.CDT) AS MinCDT
FROM Huser_servicebase TName
LEFT JOIN Hreseller Hrc ON TName.Creator_Id = Hrc.Reseller_Id
WHERE TRIM(LOWER(Hrc.ResellerName)) = TRIM(LOWER(%s))
"""
        conn = self._get_conn(source, timeout)
        try:
            with conn.cursor() as cur:
                cur.execute(query, [rs_username])
                row = cur.fetchone() or {}
                min_cdt = row.get('MinCDT')
                if not min_cdt:
                    return None
                if isinstance(min_cdt, datetime.datetime):
                    return min_cdt.date()
                if isinstance(min_cdt, datetime.date):
                    return min_cdt
                return datetime.date.fromisoformat(str(min_cdt)[:10])
        finally:
            conn.close()

    def _fetch_report_rows(self, rs_username, date_start, date_end, source, limit, timeout):
        query = """
SELECT
    TName.User_ServiceBase_Id AS RowID,
    IF(TName.Creator_Id = 0, '- User_From_Site -', Hrc.ResellerName) AS Creator,
    Hse.ServiceName AS ServiceName,
    Hu.Username AS Username,
    DATE_FORMAT(TName.CDT, '%%Y-%%m-%%d %%H:%%i:%%s') AS CreateDT,
    TName.ServiceStatus AS ServiceStatus,
    FORMAT(TName.ServicePrice, 0) AS ServicePrice,
    DATE_FORMAT(NULLIF(TName.StartDate, '0000-00-00'), '%%Y-%%m-%%d') AS StartDate,
    DATE_FORMAT(NULLIF(TName.EndDate, '0000-00-00'), '%%Y-%%m-%%d') AS EndDate,
    CASE
        WHEN COALESCE(NULLIF(Hse.STrA, 0), NULLIF(Hse.MTrA, 0), NULLIF(Hse.DTrA, 0), NULLIF(Hse.YTrA, 0), NULLIF(Hse.ExtraTraffic, 0)) IS NULL THEN NULL
        ELSE ROUND(COALESCE(NULLIF(Hse.STrA, 0), NULLIF(Hse.MTrA, 0), NULLIF(Hse.DTrA, 0), NULLIF(Hse.YTrA, 0), NULLIF(Hse.ExtraTraffic, 0)) / 1073741824, 2)
    END AS Package
FROM Huser_servicebase TName
JOIN Huser Hu ON TName.User_Id = Hu.User_Id
LEFT JOIN Hreseller Hrc ON TName.Creator_Id = Hrc.Reseller_Id
LEFT JOIN Hservice Hse ON TName.Service_Id = Hse.Service_Id
WHERE TRIM(LOWER(Hrc.ResellerName)) = TRIM(LOWER(%s))
  AND DATE(TName.CDT) BETWEEN %s AND %s
ORDER BY TName.CDT DESC
"""
        params = [rs_username, date_start, date_end]
        if limit and int(limit) > 0:
            query += "\nLIMIT %s"
            params.append(int(limit))

        conn = self._get_conn(source, timeout)
        try:
            with conn.cursor() as cur:
                cur.execute(query, params)
                rows = cur.fetchall()
            return pd.DataFrame(rows)
        finally:
            conn.close()

    def _append_totals(self, df):
        total_count = int(len(df))
        pkg_series = pd.to_numeric(df.get('Package'), errors='coerce')
        total_gb = float(pkg_series.sum(skipna=True)) if not pkg_series.empty else 0.0

        if 'PackageSumGB' not in df.columns:
            df['PackageSumGB'] = None
        if 'PackageCount' not in df.columns:
            df['PackageCount'] = None

        total_row = {col: None for col in df.columns}
        total_row['ServiceName'] = 'TOTAL'
        total_row['PackageSumGB'] = round(total_gb, 2)
        total_row['PackageCount'] = total_count
        return pd.concat([df, pd.DataFrame([total_row])], ignore_index=True)

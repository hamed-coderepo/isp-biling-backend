import datetime
from django.core.management.base import BaseCommand, CommandError
from reports.bq import get_bq_client, get_bq_table_id
from reports.views import export_df_to_pdf
import pandas as pd
from google.cloud import bigquery


class Command(BaseCommand):
    help = "Generate a BigQuery PDF report for a single rs_username and date range"

    def add_arguments(self, parser):
        parser.add_argument("rs_username", type=str, help="Reseller username (rs_username)")
        parser.add_argument("date_start", type=str, help="Start date (YYYY-MM-DD)")
        parser.add_argument("date_end", type=str, help="End date (YYYY-MM-DD)")
        parser.add_argument("--output", type=str, default="", help="Output PDF path")
        parser.add_argument("--limit", type=int, default=0, help="Limit rows (optional)")

    def handle(self, *args, **options):
        rs_username = options["rs_username"].strip()
        date_start = self._parse_date(options["date_start"])
        date_end = self._parse_date(options["date_end"])
        if date_start > date_end:
            raise CommandError("date_start must be <= date_end")

        output_path = options.get("output") or f"report_{rs_username}_{date_start}_to_{date_end}.pdf"
        limit = options.get("limit") or 0

        client = get_bq_client()
        table_id = get_bq_table_id()

        limit_clause = ""
        if limit > 0:
            limit_clause = "LIMIT @limit"

        query = f"""
SELECT
    id,
    CreateDate,
    UserServiceID,
    rs_username,
    rs_name,
    ServiceName,
    username,
    ServiceStatus,
    ServicePrice,
    Package,
    StartDate,
    EndDate
FROM `{table_id}`
WHERE LOWER(TRIM(rs_username)) = LOWER(TRIM(@creator))
  AND CreateDate BETWEEN @start_date AND @end_date
ORDER BY rs_username ASC, UserServiceID ASC
{limit_clause}
"""

        params = [
            bigquery.ScalarQueryParameter("creator", "STRING", rs_username),
            bigquery.ScalarQueryParameter("start_date", "DATE", date_start),
            bigquery.ScalarQueryParameter("end_date", "DATE", date_end),
        ]
        if limit > 0:
            params.append(bigquery.ScalarQueryParameter("limit", "INT64", limit))

        job = client.query(query, job_config=bigquery.QueryJobConfig(query_parameters=params))
        rows = [dict(r) for r in job.result()]
        df = pd.DataFrame(rows)

        if df.empty:
            self.stdout.write("No rows returned for this filter.")
            return

        pdf_data = export_df_to_pdf(df)
        if not pdf_data:
            self.stdout.write("Failed to generate PDF.")
            return

        with open(output_path, "wb") as f:
            f.write(pdf_data)

        self.stdout.write(f"PDF generated: {output_path}")

    def _parse_date(self, value: str) -> datetime.date:
        try:
            return datetime.date.fromisoformat(value.strip())
        except Exception as exc:
            raise CommandError(f"Invalid date: {value}") from exc

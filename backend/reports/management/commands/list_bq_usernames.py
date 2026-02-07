from django.core.management.base import BaseCommand
from reports.bq import get_bq_client, get_bq_table_id


class Command(BaseCommand):
    help = "List unique rs_username values from BigQuery"

    def add_arguments(self, parser):
        parser.add_argument("--limit", type=int, default=0, help="Limit number of usernames returned")

    def handle(self, *args, **options):
        limit = options.get("limit") or 0
        client = get_bq_client()
        table_id = get_bq_table_id()

        query = f"""
SELECT DISTINCT rs_username
FROM `{table_id}`
WHERE rs_username IS NOT NULL AND rs_username != ''
ORDER BY rs_username ASC
"""
        if limit > 0:
            query += f"\nLIMIT {int(limit)}"

        results = client.query(query).result()
        rows = [row["rs_username"] for row in results]
        if not rows:
            self.stdout.write("No rs_username values found.")
            return

        for name in rows:
            self.stdout.write(str(name))

import time
from django.core.management.base import BaseCommand

from maria_cache.sync import sync_reference_tables
from reports.db import get_sources


class Command(BaseCommand):
    help = "Sync permission cache from all MariaDB sources."

    def handle(self, *args, **options):
        sources = [s.get('name') for s in get_sources() if s.get('name')]
        if not sources:
            self.stdout.write(self.style.ERROR("No MariaDB sources configured."))
            return

        start = time.monotonic()
        completed = 0
        total = len(sources)

        self.stdout.write(f"Starting permission cache sync for {total} source(s)...")

        for source_name in sources:
            source_start = time.monotonic()
            self.stdout.write(f"\n[{completed + 1}/{total}] Syncing source: {source_name}")
            try:
                sync_reference_tables(source_name=source_name)
            except Exception as exc:
                self.stdout.write(self.style.ERROR(f"Sync failed for {source_name}: {exc}"))
            else:
                elapsed = time.monotonic() - source_start
                completed += 1
                avg = (time.monotonic() - start) / max(completed, 1)
                remaining = total - completed
                eta = int(avg * remaining)
                self.stdout.write(self.style.SUCCESS(
                    f"Completed {source_name} in {elapsed:.1f}s. ETA ~{eta}s"
                ))

        total_elapsed = time.monotonic() - start
        self.stdout.write(self.style.SUCCESS(
            f"Permission cache sync finished in {total_elapsed:.1f}s."
        ))

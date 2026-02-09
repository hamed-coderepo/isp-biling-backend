from django.contrib import admin
from django.http import HttpResponseRedirect
from django.urls import path
from django.contrib import messages

from .models import ResellerProfile
from .sync import sync_maria_to_bigquery


@admin.register(ResellerProfile)
class ResellerProfileAdmin(admin.ModelAdmin):
    list_display = ('reseller_name', 'user')
    change_list_template = 'admin/reports/resellerprofile/change_list.html'

    def get_urls(self):
        urls = super().get_urls()
        custom = [
            path('sync-bigquery/', self.admin_site.admin_view(self.sync_bigquery), name='reports_sync_bigquery'),
        ]
        return custom + urls

    def sync_bigquery(self, request):
        try:
            rows = sync_maria_to_bigquery()
            messages.success(request, f'Synced {rows} rows to BigQuery')
        except Exception as exc:
            messages.error(request, f'BigQuery sync failed: {exc}')
        return HttpResponseRedirect('../')

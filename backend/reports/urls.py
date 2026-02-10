from django.urls import path
from .views import report_view, sync_logs_view, logout_view, create_package_view, download_created_users_pdf, download_created_users_qr_pdf, download_pdf_archive, manual_sync_permissions, login_view

app_name = 'reports'

urlpatterns = [
    path('reports/', report_view, name='report'),
    path('create-package/', create_package_view, name='create_package'),
    path('create-package/download-pdf/', download_created_users_pdf, name='create_package_download_pdf'),
    path('create-package/download-qr-pdf/', download_created_users_qr_pdf, name='create_package_download_qr_pdf'),
    path('create-package/download-archive/', download_pdf_archive, name='create_package_download_archive'),
    path('create-package/manual-sync/', manual_sync_permissions, name='create_package_manual_sync'),
    path('sync-logs/', sync_logs_view, name='sync_logs'),
    path('login/', login_view, name='login'),
    path('logout/', logout_view, name='logout'),
]

from django.urls import path
from .views import report_view, sync_logs_view, logout_view
from django.contrib.auth import views as auth_views

app_name = 'reports'

urlpatterns = [
    path('reports/', report_view, name='report'),
    path('sync-logs/', sync_logs_view, name='sync_logs'),
    path('login/', auth_views.LoginView.as_view(template_name='registration/login.html'), name='login'),
    path('logout/', logout_view, name='logout'),
]

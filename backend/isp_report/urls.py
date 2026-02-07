from django.contrib import admin
from django.urls import path, include
from django.views.generic import RedirectView
from reports.views import logout_view

urlpatterns = [
    path('admin/', admin.site.urls),
    path('accounts/logout/', logout_view, name='account_logout'),
    path('accounts/', include('django.contrib.auth.urls')),
    path('', include('reports.urls')),
    path('', RedirectView.as_view(url='/reports/')),
]

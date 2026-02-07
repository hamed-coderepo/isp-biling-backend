from django.urls import path
from .views import report_view
from django.contrib.auth import views as auth_views

app_name = 'reports'

urlpatterns = [
    path('reports/', report_view, name='report'),
    path('login/', auth_views.LoginView.as_view(template_name='registration/login.html'), name='login'),
    path('logout/', auth_views.LogoutView.as_view(), name='logout'),
]

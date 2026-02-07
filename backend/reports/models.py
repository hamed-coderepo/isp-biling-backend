from django.db import models
from django.contrib.auth.models import User


class ResellerProfile(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE)
    reseller_name = models.CharField(max_length=200, unique=True)

    def __str__(self):
        return f"{self.reseller_name} ({self.user.username})"

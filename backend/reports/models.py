from django.db import models
from django.contrib.auth.models import User


class ResellerProfile(models.Model):
    user = models.OneToOneField(User, on_delete=models.CASCADE)
    reseller_name = models.CharField(max_length=200, unique=True)

    def __str__(self):
        return f"{self.reseller_name} ({self.user.username})"


class PdfArchive(models.Model):
    TYPE_DETAILS = 'details'
    TYPE_QR = 'qr'
    TYPE_CHOICES = [
        (TYPE_DETAILS, 'Details'),
        (TYPE_QR, 'QR'),
    ]

    pdf_type = models.CharField(max_length=20, choices=TYPE_CHOICES)
    file = models.FileField(upload_to='generated_pdfs/')
    created_at = models.DateTimeField(auto_now_add=True)
    created_by = models.ForeignKey(User, on_delete=models.SET_NULL, null=True, blank=True)
    batch_id = models.IntegerField(null=True, blank=True)
    batch_name = models.CharField(max_length=255, blank=True)
    reseller_username = models.CharField(max_length=255, blank=True)
    service_name = models.CharField(max_length=255, blank=True)
    user_count = models.IntegerField(null=True, blank=True)

    def __str__(self):
        return f"{self.pdf_type} #{self.id}"

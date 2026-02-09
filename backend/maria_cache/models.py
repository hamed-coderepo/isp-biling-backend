from django.db import models


class Reseller(models.Model):
    source_name = models.CharField(max_length=64)
    source_id = models.IntegerField()
    name = models.CharField(max_length=64)
    name_norm = models.CharField(max_length=64, db_index=True)
    is_enabled = models.BooleanField(default=True)

    class Meta:
        unique_together = ('source_name', 'source_id')
        indexes = [
            models.Index(fields=['source_name', 'name_norm']),
        ]

    def __str__(self):
        return self.name


class Visp(models.Model):
    source_name = models.CharField(max_length=64)
    source_id = models.IntegerField()
    name = models.CharField(max_length=64)
    is_enabled = models.BooleanField(default=True)

    class Meta:
        unique_together = ('source_name', 'source_id')

    def __str__(self):
        return self.name


class Center(models.Model):
    source_name = models.CharField(max_length=64)
    source_id = models.IntegerField()
    name = models.CharField(max_length=64)
    is_enabled = models.BooleanField(default=True)
    visp_access = models.CharField(max_length=16, default='All')

    class Meta:
        unique_together = ('source_name', 'source_id')

    def __str__(self):
        return self.name


class Supporter(models.Model):
    source_name = models.CharField(max_length=64)
    source_id = models.IntegerField()
    name = models.CharField(max_length=64)
    is_enabled = models.BooleanField(default=True)

    class Meta:
        unique_together = ('source_name', 'source_id')

    def __str__(self):
        return self.name


class Status(models.Model):
    source_name = models.CharField(max_length=64)
    source_id = models.IntegerField()
    name = models.CharField(max_length=64)
    is_enabled = models.BooleanField(default=True)
    reseller_access = models.CharField(max_length=16, default='All')
    visp_access = models.CharField(max_length=16, default='All')

    class Meta:
        unique_together = ('source_name', 'source_id')

    def __str__(self):
        return self.name


class Service(models.Model):
    source_name = models.CharField(max_length=64)
    source_id = models.IntegerField()
    name = models.CharField(max_length=132)
    is_enabled = models.BooleanField(default=True)
    is_deleted = models.BooleanField(default=False)
    reseller_access = models.CharField(max_length=16, default='All')
    visp_access = models.CharField(max_length=16, default='All')

    class Meta:
        unique_together = ('source_name', 'source_id')

    def __str__(self):
        return self.name


class ServiceResellerAccess(models.Model):
    source_name = models.CharField(max_length=64)
    service_id = models.IntegerField()
    reseller_id = models.IntegerField()
    checked = models.BooleanField(default=False)

    class Meta:
        unique_together = ('source_name', 'service_id', 'reseller_id')


class StatusResellerAccess(models.Model):
    source_name = models.CharField(max_length=64)
    status_id = models.IntegerField()
    reseller_id = models.IntegerField()
    checked = models.BooleanField(default=False)

    class Meta:
        unique_together = ('source_name', 'status_id', 'reseller_id')


class ServiceVispAccess(models.Model):
    source_name = models.CharField(max_length=64)
    service_id = models.IntegerField()
    visp_id = models.IntegerField()
    checked = models.BooleanField(default=False)

    class Meta:
        unique_together = ('source_name', 'service_id', 'visp_id')


class StatusVispAccess(models.Model):
    source_name = models.CharField(max_length=64)
    status_id = models.IntegerField()
    visp_id = models.IntegerField()
    checked = models.BooleanField(default=False)

    class Meta:
        unique_together = ('source_name', 'status_id', 'visp_id')


class CenterVispAccess(models.Model):
    source_name = models.CharField(max_length=64)
    center_id = models.IntegerField()
    visp_id = models.IntegerField()
    checked = models.BooleanField(default=False)

    class Meta:
        unique_together = ('source_name', 'center_id', 'visp_id')


class ResellerPermit(models.Model):
    source_name = models.CharField(max_length=64)
    reseller_id = models.IntegerField()
    visp_id = models.IntegerField()
    permit_item_id = models.IntegerField(null=True, blank=True)
    is_permit = models.BooleanField(default=False)

    class Meta:
        unique_together = ('source_name', 'reseller_id', 'visp_id', 'permit_item_id')

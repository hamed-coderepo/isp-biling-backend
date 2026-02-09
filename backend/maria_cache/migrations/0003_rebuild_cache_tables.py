from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('maria_cache', '0002_source_and_access'),
    ]

    operations = [
        migrations.DeleteModel(name='Reseller'),
        migrations.DeleteModel(name='Visp'),
        migrations.DeleteModel(name='Center'),
        migrations.DeleteModel(name='Supporter'),
        migrations.DeleteModel(name='Status'),
        migrations.DeleteModel(name='Service'),
        migrations.DeleteModel(name='ServiceResellerAccess'),
        migrations.DeleteModel(name='StatusResellerAccess'),
        migrations.DeleteModel(name='ServiceVispAccess'),
        migrations.DeleteModel(name='StatusVispAccess'),
        migrations.DeleteModel(name='CenterVispAccess'),
        migrations.CreateModel(
            name='Reseller',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('source_name', models.CharField(max_length=64)),
                ('source_id', models.IntegerField()),
                ('name', models.CharField(max_length=64)),
                ('is_enabled', models.BooleanField(default=True)),
            ],
            options={'unique_together': {('source_name', 'source_id')}},
        ),
        migrations.CreateModel(
            name='Visp',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('source_name', models.CharField(max_length=64)),
                ('source_id', models.IntegerField()),
                ('name', models.CharField(max_length=64)),
                ('is_enabled', models.BooleanField(default=True)),
            ],
            options={'unique_together': {('source_name', 'source_id')}},
        ),
        migrations.CreateModel(
            name='Center',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('source_name', models.CharField(max_length=64)),
                ('source_id', models.IntegerField()),
                ('name', models.CharField(max_length=64)),
                ('is_enabled', models.BooleanField(default=True)),
                ('visp_access', models.CharField(default='All', max_length=16)),
            ],
            options={'unique_together': {('source_name', 'source_id')}},
        ),
        migrations.CreateModel(
            name='Supporter',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('source_name', models.CharField(max_length=64)),
                ('source_id', models.IntegerField()),
                ('name', models.CharField(max_length=64)),
                ('is_enabled', models.BooleanField(default=True)),
            ],
            options={'unique_together': {('source_name', 'source_id')}},
        ),
        migrations.CreateModel(
            name='Status',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('source_name', models.CharField(max_length=64)),
                ('source_id', models.IntegerField()),
                ('name', models.CharField(max_length=64)),
                ('is_enabled', models.BooleanField(default=True)),
                ('reseller_access', models.CharField(default='All', max_length=16)),
                ('visp_access', models.CharField(default='All', max_length=16)),
            ],
            options={'unique_together': {('source_name', 'source_id')}},
        ),
        migrations.CreateModel(
            name='Service',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('source_name', models.CharField(max_length=64)),
                ('source_id', models.IntegerField()),
                ('name', models.CharField(max_length=132)),
                ('is_enabled', models.BooleanField(default=True)),
                ('is_deleted', models.BooleanField(default=False)),
                ('reseller_access', models.CharField(default='All', max_length=16)),
                ('visp_access', models.CharField(default='All', max_length=16)),
            ],
            options={'unique_together': {('source_name', 'source_id')}},
        ),
        migrations.CreateModel(
            name='ServiceResellerAccess',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('source_name', models.CharField(max_length=64)),
                ('service_id', models.IntegerField()),
                ('reseller_id', models.IntegerField()),
                ('checked', models.BooleanField(default=False)),
            ],
            options={'unique_together': {('source_name', 'service_id', 'reseller_id')}},
        ),
        migrations.CreateModel(
            name='StatusResellerAccess',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('source_name', models.CharField(max_length=64)),
                ('status_id', models.IntegerField()),
                ('reseller_id', models.IntegerField()),
                ('checked', models.BooleanField(default=False)),
            ],
            options={'unique_together': {('source_name', 'status_id', 'reseller_id')}},
        ),
        migrations.CreateModel(
            name='ServiceVispAccess',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('source_name', models.CharField(max_length=64)),
                ('service_id', models.IntegerField()),
                ('visp_id', models.IntegerField()),
                ('checked', models.BooleanField(default=False)),
            ],
            options={'unique_together': {('source_name', 'service_id', 'visp_id')}},
        ),
        migrations.CreateModel(
            name='StatusVispAccess',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('source_name', models.CharField(max_length=64)),
                ('status_id', models.IntegerField()),
                ('visp_id', models.IntegerField()),
                ('checked', models.BooleanField(default=False)),
            ],
            options={'unique_together': {('source_name', 'status_id', 'visp_id')}},
        ),
        migrations.CreateModel(
            name='CenterVispAccess',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('source_name', models.CharField(max_length=64)),
                ('center_id', models.IntegerField()),
                ('visp_id', models.IntegerField()),
                ('checked', models.BooleanField(default=False)),
            ],
            options={'unique_together': {('source_name', 'center_id', 'visp_id')}},
        ),
        migrations.CreateModel(
            name='ResellerPermit',
            fields=[
                ('id', models.BigAutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('source_name', models.CharField(max_length=64)),
                ('reseller_id', models.IntegerField()),
                ('visp_id', models.IntegerField()),
                ('permit_item_id', models.IntegerField(blank=True, null=True)),
                ('is_permit', models.BooleanField(default=False)),
            ],
            options={'unique_together': {('source_name', 'reseller_id', 'visp_id', 'permit_item_id')}},
        ),
    ]

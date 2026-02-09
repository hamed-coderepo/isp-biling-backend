from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [
        ('maria_cache', '0001_initial'),
    ]

    operations = [
        migrations.AddField(
            model_name='reseller',
            name='source_name',
            field=models.CharField(default='', max_length=64),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name='visp',
            name='source_name',
            field=models.CharField(default='', max_length=64),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name='center',
            name='source_name',
            field=models.CharField(default='', max_length=64),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name='supporter',
            name='source_name',
            field=models.CharField(default='', max_length=64),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name='status',
            name='source_name',
            field=models.CharField(default='', max_length=64),
            preserve_default=False,
        ),
        migrations.AddField(
            model_name='service',
            name='source_name',
            field=models.CharField(default='', max_length=64),
            preserve_default=False,
        ),
        migrations.CreateModel(
            name='CenterVispAccess',
            fields=[
                ('id', models.IntegerField(primary_key=True, serialize=False)),
                ('source_name', models.CharField(max_length=64)),
                ('center_id', models.IntegerField()),
                ('visp_id', models.IntegerField()),
                ('checked', models.BooleanField(default=False)),
            ],
        ),
        migrations.CreateModel(
            name='ServiceResellerAccess',
            fields=[
                ('id', models.IntegerField(primary_key=True, serialize=False)),
                ('source_name', models.CharField(max_length=64)),
                ('service_id', models.IntegerField()),
                ('reseller_id', models.IntegerField()),
                ('checked', models.BooleanField(default=False)),
            ],
        ),
        migrations.CreateModel(
            name='ServiceVispAccess',
            fields=[
                ('id', models.IntegerField(primary_key=True, serialize=False)),
                ('source_name', models.CharField(max_length=64)),
                ('service_id', models.IntegerField()),
                ('visp_id', models.IntegerField()),
                ('checked', models.BooleanField(default=False)),
            ],
        ),
        migrations.CreateModel(
            name='StatusResellerAccess',
            fields=[
                ('id', models.IntegerField(primary_key=True, serialize=False)),
                ('source_name', models.CharField(max_length=64)),
                ('status_id', models.IntegerField()),
                ('reseller_id', models.IntegerField()),
                ('checked', models.BooleanField(default=False)),
            ],
        ),
        migrations.CreateModel(
            name='StatusVispAccess',
            fields=[
                ('id', models.IntegerField(primary_key=True, serialize=False)),
                ('source_name', models.CharField(max_length=64)),
                ('status_id', models.IntegerField()),
                ('visp_id', models.IntegerField()),
                ('checked', models.BooleanField(default=False)),
            ],
        ),
    ]

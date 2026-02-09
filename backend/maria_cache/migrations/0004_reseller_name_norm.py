from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('maria_cache', '0003_rebuild_cache_tables'),
    ]

    operations = [
        migrations.AddField(
            model_name='reseller',
            name='name_norm',
            field=models.CharField(db_index=True, default='', max_length=64),
            preserve_default=False,
        ),
        migrations.AddIndex(
            model_name='reseller',
            index=models.Index(fields=['source_name', 'name_norm'], name='maria_cache_reseller_source_name_norm_idx'),
        ),
    ]

# Generated by Django 3.0.3 on 2020-04-02 04:25

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('api', '0012_auto_20200331_0337'),
    ]

    operations = [
        migrations.AlterField(
            model_name='appsetting',
            name='update_rate',
            field=models.IntegerField(blank=True, default=None, null=True),
        ),
    ]

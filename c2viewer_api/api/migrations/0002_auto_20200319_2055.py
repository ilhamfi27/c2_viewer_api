# Generated by Django 3.0.3 on 2020-03-19 13:55

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ('api', '0001_initial'),
    ]

    operations = [
        migrations.AlterModelTable(
            name='location',
            table='locations',
        ),
        migrations.AlterModelTable(
            name='user',
            table='users',
        ),
    ]

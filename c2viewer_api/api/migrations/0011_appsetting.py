# Generated by Django 3.0.3 on 2020-03-22 15:27

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('api', '0010_auto_20200322_1603'),
    ]

    operations = [
        migrations.CreateModel(
            name='AppSetting',
            fields=[
                ('id', models.AutoField(auto_created=True, primary_key=True, serialize=False, verbose_name='ID')),
                ('update_rate', models.IntegerField()),
            ],
            options={
                'db_table': 'app_setting',
            },
        ),
    ]
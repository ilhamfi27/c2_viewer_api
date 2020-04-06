from django.db import models


class Location(models.Model):
    name = models.CharField(max_length=50)
    latitude = models.DecimalField(max_digits=10, decimal_places=2, blank=True, null=True)
    longitude = models.DecimalField(max_digits=10, decimal_places=2, blank=True, null=True)

    class Meta:
        db_table = 'locations'


class User(models.Model):
    USER_LEVEL = [
        ('superadmin', 'Superadmin'),
        ('admin', 'Admin'),
        ('operator', 'Operator'),
    ]
    username = models.CharField(max_length=40, unique=True)
    password = models.CharField(max_length=100)
    level = models.CharField(max_length=20, choices=USER_LEVEL)
    location = models.ForeignKey(Location, on_delete=models.CASCADE)

    class Meta:
        db_table = 'users'


class AccessToken(models.Model):
    token = models.CharField(max_length=64)
    user = models.ForeignKey(User, on_delete=models.CASCADE)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = 'access_token'


class Session(models.Model):
    name = models.CharField(max_length=100)
    start_time = models.DateTimeField(auto_now_add=True)
    end_time = models.DateTimeField(blank=True, null=True)

    class Meta:
        db_table = 'sessions'
        managed = False


class StoredReplay(models.Model):
    session = models.ForeignKey(Session, on_delete=models.CASCADE, null=True)
    update_rate = models.CharField(max_length=100)
    data = models.CharField(max_length=500)
    sequence = models.IntegerField(blank=True, null=True, default=None)

    class Meta:
        db_table = 'stored_replay'
        managed = False


class AppSetting(models.Model):
    update_rate = models.IntegerField(blank=True, null=True, default=None)

    class Meta:
        db_table = 'app_setting'

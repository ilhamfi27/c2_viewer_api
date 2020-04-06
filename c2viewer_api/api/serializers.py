from django.contrib.auth import get_user_model, authenticate
from .models import Location, User, StoredReplay, Session, AppSetting
from rest_framework import serializers, exceptions
from rest_framework.exceptions import ValidationError

class LocationSerializer(serializers.ModelSerializer):
    class Meta:
        model = Location
        fields = '__all__' # menampilkan semua field dari location

class UserSerializer(serializers.ModelSerializer):
    location_detail = LocationSerializer(source='location', read_only=True)

    class Meta:
        model = User
        fields = ('id', 'username', 'password', 'level', 'location', 'location_detail')


class SessionSerializer(serializers.ModelSerializer):
    class Meta:
        model = Session
        fields = '__all__'

class LoginSerializer(serializers.Serializer):
    username = serializers.CharField(required=True, allow_blank=True)
    password = serializers.CharField(required=True, style={'input_type': 'password'})

class ChangePasswordSerializer(serializers.Serializer):
    token = serializers.CharField(required=True)
    password = serializers.CharField(required=True, style={'input_type': 'password'})
    new_password = serializers.CharField(required=True, style={'input_type': 'password'})
    new_password_confirm = serializers.CharField(required=True, style={'input_type': 'password'})

class UnlockSessionSerializer(serializers.Serializer):
    token = serializers.CharField(required=True)
    password = serializers.CharField(required=True, style={'input_type': 'password'})

class StoredReplaySerializer(serializers.ModelSerializer):
    class Meta:
        model = StoredReplay
        fields = '__all__'

class AppSettingSerializer(serializers.ModelSerializer):
    class Meta:
        model = AppSetting
        fields = '__all__'

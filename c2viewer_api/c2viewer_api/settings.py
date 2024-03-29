"""
Django settings for c2viewer_api project.

Generated by 'django-admin startproject' using Django 3.0.4.

For more information on this file, see
https://docs.djangoproject.com/en/3.0/topics/settings/

For the full list of settings and their values, see
https://docs.djangoproject.com/en/3.0/ref/settings/
"""

import os
import environ

env = environ.Env()

# Build paths inside the project like this: os.path.join(BASE_DIR, ...)
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

# reading .env file
environ.Env.read_env(os.path.join(BASE_DIR, '.env'))

# Quick-start development settings - unsuitable for production
# See https://docs.djangoproject.com/en/3.0/howto/deployment/checklist/

# SECURITY WARNING: keep the secret key used in production secret!
SECRET_KEY = '++ukfoft2*_txtsmqx&)=1%f^05+c1*(19(v-si+$89d*16#ob'

# SECURITY WARNING: don't run with debug turned on in production!
# False if not in os.environ
DEBUG = env('APP_DEBUG')

ALLOWED_HOSTS = []

# Application definition

INSTALLED_APPS = [
    'django.contrib.admin',
    'django.contrib.auth',
    'django.contrib.contenttypes',
    'django.contrib.sessions',
    'django.contrib.messages',
    'django.contrib.staticfiles',
    'rest_framework',
    'rest_framework.authtoken',
    'corsheaders',
    'api',
    'channels',
    'userauth',
]

REST_FRAMEWORK = {
    # 'AUTH_USER_MODEL': 'api.Models.User',
    # 'UNAUTHENTICATED_USER': None,
    'DEFAULT_AUTHENTICATION_CLASSES': [
        # 'c2viewer_api.authentication.JwtServiceOnlyAuthentication',
    ],
    'DEFAULT_PERMISSION_CLASSES': [
        # 'rest_framework.permissions.IsAuthenticated',
    ]
}

MIDDLEWARE = [
    'corsheaders.middleware.CorsMiddleware',
    'django.middleware.security.SecurityMiddleware',
    'django.contrib.sessions.middleware.SessionMiddleware',
    'django.middleware.common.CommonMiddleware',
    'django.middleware.csrf.CsrfViewMiddleware',
    'django.contrib.auth.middleware.AuthenticationMiddleware',
    'django.contrib.messages.middleware.MessageMiddleware',
    'django.middleware.clickjacking.XFrameOptionsMiddleware',
]

CORS_ORIGIN_ALLOW_ALL = True

CORS_ALLOW_METHODS = [
    'DELETE',
    'GET',
    'OPTIONS',
    'PATCH',
    'POST',
    'PUT',
]

CORS_ALLOW_HEADERS = [
    'accept',
    'accept-encoding',
    'authorization',
    'content-type',
    'dnt',
    'origin',
    'user-agent',
    'x-csrftoken',
    'x-requested-with',
    'access-control-allow-origin',
    'access-control-allow-methods',
    'access-control-allow-headers',
]

ALLOWED_HOSTS = [str(host) for host in env('HOST_ALLOWED').split(",")]

ROOT_URLCONF = 'c2viewer_api.urls'

TEMPLATES = [
    {
        'BACKEND': 'django.template.backends.django.DjangoTemplates',
        'DIRS': [],
        'APP_DIRS': True,
        'OPTIONS': {
            'context_processors': [
                'django.template.context_processors.debug',
                'django.template.context_processors.request',
                'django.contrib.auth.context_processors.auth',
                'django.contrib.messages.context_processors.messages',
            ],
        },
    },
]

WSGI_APPLICATION = 'c2viewer_api.wsgi.application'

# Channels
ASGI_APPLICATION = 'c2viewer_api.routing.application'
CHANNEL_LAYERS = {
    'default': {
        'BACKEND': 'channels_redis.core.RedisChannelLayer',
        'CONFIG': {
            'hosts': [(env('REDIS_HOST'), env('REDIS_PORT'))],
        }
    }
}

# Caching
CACHES = {
    "default": {
        "BACKEND": "django_redis.cache.RedisCache",
        "LOCATION": "redis://127.0.0.1:6379/1",
        "OPTIONS": {
            "CLIENT_CLASS": "django_redis.client.DefaultClient"
        },
    }
}

# Database
# https://docs.djangoproject.com/en/3.0/ref/settings/#databases

# -- THE ORIGINAL SCRIPT --
# DATABASES = {
#     'default': {
#         'ENGINE': 'django.db.backends.sqlite3',
#         'NAME': os.path.join(BASE_DIR, 'db.sqlite3'),
#     }
# }
# -- END OF ORIGINAL SCRIPT --

# -- MODIFIED SCRIPT --
DATABASES = {}

db_connection = env('DB_CONNECTION')

if db_connection == 'sqlite3':
    DATABASES.setdefault('default', {})['ENGINE'] = env('DB_ENGINE')
    DATABASES.setdefault('default', {})['NAME'] = os.path.join(BASE_DIR, env('DB_name'))
else:
    schema = env('DB_SCHEMA') if env('DB_SCHEMA') not in [None, ""] else "public"
    DATABASES.setdefault('default', {})['ENGINE'] = env('DB_ENGINE')
    DATABASES.setdefault('default', {})['OPTIONS'] = {
        'options': '-c search_path={}'.format(schema)
    }
    DATABASES.setdefault('default', {})['NAME'] = env('DB_NAME')
    DATABASES.setdefault('default', {})['USER'] = env('DB_USER')
    DATABASES.setdefault('default', {})['PASSWORD'] = env('DB_PASSWORD')
    DATABASES.setdefault('default', {})['HOST'] = env('DB_HOST')
    DATABASES.setdefault('default', {})['PORT'] = env('DB_PORT')
# -- END OF MODIFIED SCRIPT --

# Password validation
# https://docs.djangoproject.com/en/3.0/ref/settings/#auth-password-validators

AUTH_PASSWORD_VALIDATORS = [
    {
        'NAME': 'django.contrib.auth.password_validation.UserAttributeSimilarityValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.MinimumLengthValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.CommonPasswordValidator',
    },
    {
        'NAME': 'django.contrib.auth.password_validation.NumericPasswordValidator',
    },
]

# Internationalization
# https://docs.djangoproject.com/en/3.0/topics/i18n/

LANGUAGE_CODE = 'en-us'

TIME_ZONE = 'UTC'

USE_I18N = True

USE_L10N = True

USE_TZ = True

# Static files (CSS, JavaScript, Images)
# https://docs.djangoproject.com/en/3.0/howto/static-files/

STATIC_URL = '/static/'
STATIC_ROOT = os.path.join(BASE_DIR, 'static')

# JWT secret key
JWT_TOKEN_KEY=env('JWT_TOKEN_KEY') if  env('JWT_TOKEN_KEY') != "" else "TOKEN SECRET"
JWT_USER_KEY=env('JWT_USER_KEY') if  env('JWT_USER_KEY') != "" else "USER SECRET"

# native redis
REDIS_HOST = env('REDIS_HOST')
REDIS_PORT = env('REDIS_PORT')

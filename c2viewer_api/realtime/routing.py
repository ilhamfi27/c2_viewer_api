from django.urls import re_path

from . import consumers

websocket_urlpatterns = [
    re_path(r'ws/realtime/$', consumers.RealtimeAccess),
    re_path(r'ws/tracker/$', consumers.RealtimeTracker),
]

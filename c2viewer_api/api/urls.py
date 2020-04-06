import api.views as views
from rest_framework import routers
from django.urls import path, include
from django.conf.urls import url

router = routers.DefaultRouter()

router.register('location', views.LocationViewSet)
router.register('user', views.UserViewSet)
router.register('stored_replay', views.StoredReplayViewSet)
router.register('session', views.SessionViewSet)
router.register('setting', views.AppSettingViewSet)

user_auth = views.AuthViewSet.as_view({
    'post': 'post',
})

stored_replay = views.StoredReplayViewSet.as_view({
    'get': 'get',
})

streamed_stored_replay = views.StoredReplayViewSet.as_view({
    'get': 'streamed',
})

change_password = views.ChangePasswordViewSet.as_view({
    'post': 'post',
})

unlock_session = views.UnlockSessionViewSet.as_view({
    'post': 'post',
})

current_setting = views.AppSettingViewSet.as_view({
    'get': 'current_setting',
})

db_op_backup = views.DatabaseOperationViewSet.as_view({
    'get': 'backup',
})

urlpatterns = [
    url(r'^auth/$', user_auth, name='user_auth'),
    url(r'^stored_replay/(?P<session_id>\d+)', stored_replay, name='stored_replay'),
    url(r'^streamed_stored_replay/(?P<session_id>\d+)', streamed_stored_replay, name='streamed_stored_replay'),
    url(r'^change_password/$', change_password, name='change_password'),
    url(r'^unlock_session/$', unlock_session, name='unlock_session'),
    url(r'^setting/current/$', current_setting, name='current_setting'),
    url(r'^database_operation/backup/(?P<session_id>\d+)', db_op_backup, name='db_op_backup'),
    url(r'', include(router.urls)),
]
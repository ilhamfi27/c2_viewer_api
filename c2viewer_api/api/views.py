from .models import Location, User, AccessToken, StoredReplay, Session, AppSetting
from rest_framework import viewsets
from rest_framework.response import Response
from django.utils import timezone
from django.http import StreamingHttpResponse
from .serializers import LocationSerializer, UserSerializer, LoginSerializer, StoredReplaySerializer, SessionSerializer, \
    AppSettingSerializer, ChangePasswordSerializer, UnlockSessionSerializer
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync
from django.db.models import Q
from django.conf import settings
from wsgiref.util import FileWrapper
import os
import api.db_op as db_operation
import json
import rest_framework.status as st
import hashlib
import csv


class LocationViewSet(viewsets.ModelViewSet):
    queryset = Location.objects.all()
    serializer_class = LocationSerializer


class UserViewSet(viewsets.ModelViewSet):
    queryset = User.objects.filter(~Q(level="superadmin"))
    serializer_class = UserSerializer


class SessionViewSet(viewsets.ModelViewSet):
    queryset = Session.objects.all().order_by('id')
    serializer_class = SessionSerializer


class AuthViewSet(viewsets.ModelViewSet):
    serializer_class = LoginSerializer

    def login(self, request, **kwargs):
        login_serializer = self.serializer_class(data=kwargs)
        valid = login_serializer.is_valid()

        if valid:
            token, user = self.authenticate_user(request, **kwargs)

            if user != None:
                response = {
                    "token": token,
                    "username": user.username,
                    "level": user.level,
                }

                # to get newest AccessToken
                access = AccessToken.objects.filter(user_id=user.id).order_by('-id')[:2]
                if len(access) > 1:
                    self.disconnect_order(access[1].token)

                return Response(response, status=st.HTTP_200_OK)
            else:
                response = {
                    "message": "Invalid Username or Password"
                }
                return Response(response, status=st.HTTP_401_UNAUTHORIZED)

        else:
            response = {
                "message": "Incorrect Field Value"
            }
            return Response(response, status=st.HTTP_400_BAD_REQUEST)

    def get_client_ip(self, request):
        x_forwarded_for = request.META.get('HTTP_X_FORWARDED_FOR')
        if x_forwarded_for:
            ip = x_forwarded_for.split(',')[0]
        else:
            ip = request.META.get('REMOTE_ADDR')
        return ip

    def authenticate_user(self, request, **kwargs):
        try:
            user = User.objects.get(**kwargs)
        except User.DoesNotExist:
            return None, None

        client_ip = self.get_client_ip(request)
        server_timezone = timezone.now().strftime('%Y-%m-%d %H:%M:%S')

        string_to_hash = client_ip + server_timezone
        hash_result = hashlib.sha256(string_to_hash.encode()).hexdigest()

        token_data = {
            'token': hash_result,
            'user_id': user.id,
        }

        data_token = AccessToken.objects.create(**token_data)

        return data_token.token, user

    def disconnect_order(self, token):
        message = json.dumps({
            'token': token
        })
        channel_layer = get_channel_layer()
        async_to_sync(channel_layer.group_send)(
            'realtime',
            {'type': 'disconnect.client', 'message': message}
        )

    def post(self, request):
        post_data = request.data

        print("POST DATA====================================================", post_data, flush=True)

        username = post_data['username']
        password = post_data['password']

        ip_address = self.get_client_ip(request)

        return self.login(request, username=username, password=password)


class ChangePasswordViewSet(viewsets.ModelViewSet):
    serializer_class = ChangePasswordSerializer

    def post(self, request):
        post_data = request.data

        print("POST DATA====================================================", post_data, flush=True)

        password_data = {}
        password_data['token'] = post_data['token']
        password_data['password'] = post_data['password']
        password_data['new_password'] = post_data['new_password']
        password_data['new_password_confirm'] = post_data['new_password_confirm']

        user_token = post_data['token']

        forgot_password_serialize = self.serializer_class(data=password_data)
        valid = forgot_password_serialize.is_valid()
        if valid:
            if password_data['new_password'] != password_data['new_password_confirm']:
                response = {
                    "message": "Password Confirm Unmatch"
                }
                return Response(response, status=st.HTTP_400_BAD_REQUEST)

            # to get newest AccessToken
            access = AccessToken.objects.filter(token=user_token).order_by('-id')[0]
            user = User.objects.get(pk=access.user_id)

            if user.password != password_data['password']:
                response = {
                    "message": "Invalid Password for Username {}".format(user.username)
                }
                return Response(response, status=st.HTTP_401_UNAUTHORIZED)

            user.password = password_data['new_password']
            user.save()

            response = {
                'user_id': user.id,
            }

            return Response(response, status=st.HTTP_200_OK)
        else:
            response = {
                "message": "Incorrect Field Value"
            }
            return Response(response, status=st.HTTP_400_BAD_REQUEST)


class UnlockSessionViewSet(viewsets.ModelViewSet):
    serializer_class = UnlockSessionSerializer

    def post(self, request):
        post_data = request.data

        print("POST DATA====================================================", post_data, flush=True)

        unlock_data = {}
        unlock_data['token'] = post_data['token']
        unlock_data['password'] = post_data['password']

        session_unlock = self.serializer_class(data=unlock_data)
        valid = session_unlock.is_valid()
        if valid:
            try:
                access = AccessToken.objects.get(token=unlock_data['token'])
                user = User.objects.get(pk=access.user_id, password=unlock_data['password'])
            except User.DoesNotExist:
                response = {
                    "message": "Invalid Password"
                }
                return Response(response, status=st.HTTP_401_UNAUTHORIZED)
            except AccessToken.DoesNotExist:
                response = {
                    "message": "Invalid Token"
                }
                return Response(response, status=st.HTTP_401_UNAUTHORIZED)

            response = {
                'token': access.token,
            }
            return Response(response, status=st.HTTP_200_OK)
        else:
            response = {
                "message": "Incorrect Field Value"
            }
            return Response(response, status=st.HTTP_400_BAD_REQUEST)


class StoredReplayViewSet(viewsets.ModelViewSet):
    queryset = StoredReplay.objects.all()
    serializer_class = StoredReplaySerializer

    def get(self, request, session_id):
        session = Session.objects.get(pk=session_id)
        stored_replays = self.queryset.filter(session_id=session_id)

        tracks = []
        replay_data = {}
        durasi_session = 0

        for stored_replay in stored_replays:
            res = json.loads(stored_replay.data)
            update_rate = res["update_rate"]
            for key, value in res["track_play"].items():
                tracks.append((key, value))
                durasi_session += 1

        replay_data["session_id"] = session.id
        replay_data["start_time"] = session.start_time
        replay_data["end_time"] = session.end_time
        replay_data["session_name"] = session.name
        replay_data["update_rate"] = update_rate
        replay_data["durasi_session"] = int(durasi_session) * int(update_rate)
        replay_data["track_play"] = dict(tracks)

        response = {
            "data": replay_data
        }

        return Response(response, status=st.HTTP_200_OK)

        # res = json.loads(stored_replays[1].data)
        # print(stored_replays[0].id, flush=True)
        # return Response(res["track_play"], status=st.HTTP_200_OK)
        # return Response({}, status=st.HTTP_200_OK)


    def streamed(self, request, session_id):
        session = Session.objects.get(pk=session_id)
        stored_replays = self.queryset.filter(session_id=session_id)


        response = {
            "data": self.iter_items(session, stored_replays)
        }

        # return Response(response, status=st.HTTP_200_OK)
        return StreamingHttpResponse(response)

    def iter_items(self, session, stored_replays):
        tracks = []
        replay_data = {}
        durasi_session = 0

        for stored_replay in stored_replays:
            res = json.loads(stored_replay.data)
            update_rate = res["update_rate"]
            for key, value in res["track_play"].items():
                tracks.append((key, value))
                durasi_session += 1

        print(session.id, flush=True)

        replay_data["session_id"] = session.id
        replay_data["start_time"] = session.start_time
        replay_data["end_time"] = session.end_time
        replay_data["session_name"] = session.name
        replay_data["update_rate"] = update_rate
        replay_data["durasi_session"] = int(durasi_session) * int(update_rate)
        replay_data["track_play"] = dict(tracks)

        return replay_data

    def hello(self):
        yield 'Hello,'
        yield 'there!'



class AppSettingViewSet(viewsets.ModelViewSet):
    queryset = AppSetting.objects.all()
    serializer_class = AppSettingSerializer

    def current_setting(self, request):
        settings = self.queryset.order_by('-id')[:1]

        if len(settings) < 1:
            response = {
                "message": "Data Not Found"
            }
            return Response(response, status=st.HTTP_404_NOT_FOUND)

        response = {
            "data": settings.values()[0]
        }
        return Response(response, status=st.HTTP_200_OK)


class DatabaseOperationViewSet(viewsets.ViewSet):

    class Echo:
        """An object that implements just the write method of the file-like
        interface.
        """

        def write(self, value):
            """Write the value by returning it, instead of storing in a buffer."""
            return value

    def iter_items(self, items, pseudo_buffer):
        for item in items:
            yield pseudo_buffer.write(item)

    def backup(self, request, session_id):
        file_path, string_query = db_operation.operation_backup(session_id)

        print(len(string_query), flush=True)

        response = StreamingHttpResponse(self.iter_items(string_query, self.Echo()),
                                         content_type="text/plain")
        response['Content-Disposition'] = 'attachment; filename=' + os.path.basename(file_path)
        return response

        # file_path = os.path.join(settings.BASE_DIR, file_path)
        # if os.path.exists(file_path):
        #     with open(file_path, 'r') as fh:
        #         response = Response(fh.read(), content_type="text/plain", status=st.HTTP_200_OK)
        #         response['Content-Type'] = 'text/plain'
        #         response['Content-Disposition'] = 'attachment; filename=' + os.path.basename(file_path)
        #         return response

        # return Response({}, status=st.HTTP_200_OK)
        # return Response({"message": "File Not Found"}, status=st.HTTP_404_NOT_FOUND)
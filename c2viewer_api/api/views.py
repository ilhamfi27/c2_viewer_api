from .models import Location, User, AccessToken, StoredReplay, Session, AppSetting
from rest_framework import viewsets
from rest_framework import views
from rest_framework.response import Response
from django.utils import timezone
from django.http import StreamingHttpResponse, HttpResponse
from .serializers import LocationSerializer, UserSerializer, LoginSerializer, StoredReplaySerializer, SessionSerializer, \
    AppSettingSerializer, ChangePasswordSerializer, UnlockSessionSerializer, RestoreFileSerializer
from channels.layers import get_channel_layer
from asgiref.sync import async_to_sync
from django.db.models import Q
from django.conf import settings
from c2viewer_api.authentication import MyCustomAuthentication
from c2viewer_api.permissions import IsAuthenticated
from zipfile import ZipFile
import api.db_op as db_operation
import json
import rest_framework.status as st
import hashlib
import jwt
import os


class LocationViewSet(viewsets.ModelViewSet):
    authentication_classes = (MyCustomAuthentication,)
    permission_classes = [IsAuthenticated]
    queryset = Location.objects.all()
    serializer_class = LocationSerializer


class UserViewSet(viewsets.ModelViewSet):
    # queryset = User.objects.filter(~Q(level="superadmin"))
    authentication_classes = (MyCustomAuthentication,)
    permission_classes = [IsAuthenticated]
    queryset = User.objects.all()
    serializer_class = UserSerializer

    def list(self, request):
        if request.user.level == "superadmin":
            current_user_id = request.user.id
            users = self.queryset.order_by('id')
        else:
            users = self.queryset.filter(~Q(level="superadmin")).order_by('id')

        serializer = UserSerializer(users, many=True)
        data = serializer.data

        return Response(data, status=st.HTTP_200_OK)


class SessionViewSet(viewsets.ModelViewSet):
    authentication_classes = (MyCustomAuthentication,)
    permission_classes = [IsAuthenticated]
    queryset = Session.objects.all().order_by('id')
    serializer_class = SessionSerializer


class AuthViewSet(views.APIView):
    serializer_class = LoginSerializer

    def login(self, request, **kwargs):
        login_serializer = self.serializer_class(data=kwargs)
        valid = login_serializer.is_valid()

        if valid:
            token, user, location = self.authenticate_user(request, **kwargs)

            if user != None:
                response = {
                    "token": token,
                    "username": user.username,
                    "level": user.level,
                    "location": {
                        "id": location.id,
                        "name": location.name,
                        "latitude": location.latitude,
                        "longitude": location.longitude,
                    },
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
        # get user credentials by encrypted password
        string_to_hash = kwargs["password"] + kwargs["username"]

        user_password = jwt.encode({
            'username':kwargs["username"],
            'password':kwargs["password"]
        }, settings.JWT_USER_KEY).decode()
        hash_result = hashlib.sha256(string_to_hash.encode()).hexdigest()

        kwargs["password"] = user_password

        try:
            user = User.objects.get(**kwargs)
            location = Location.objects.get(pk=user.location.id)
        except User.DoesNotExist:
            return None, None, None

        client_ip = self.get_client_ip(request)
        server_timezone = timezone.now().strftime('%Y-%m-%d %H:%M:%S')

        string_to_hash = client_ip + server_timezone
        hash_result = hashlib.sha256(string_to_hash.encode()).hexdigest()

        user_token = jwt.encode({
            'user_id':user.id,
            'random_string': string_to_hash
        }, settings.JWT_TOKEN_KEY).decode()

        token_data = {
            'token': user_token,
            'user_id': user.id,
        }

        data_token = AccessToken.objects.create(**token_data)

        return data_token.token, user, location

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
    authentication_classes = (MyCustomAuthentication,)
    permission_classes = [IsAuthenticated]
    serializer_class = ChangePasswordSerializer

    def post(self, request):
        post_data = request.data
        user = request.user

        print("POST DATA====================================================", post_data, flush=True)

        # get user credentials by encrypted password
        string_to_hash = post_data["password"] + request.user.username
        hash_result = hashlib.sha256(string_to_hash.encode()).hexdigest()

        password_data = {}
        password_data['password'] = hash_result
        password_data['new_password'] = post_data['new_password']
        password_data['new_password_confirm'] = post_data['new_password_confirm']

        forgot_password_serialize = self.serializer_class(data=password_data)
        valid = forgot_password_serialize.is_valid()
        if valid:
            if password_data['new_password'] != password_data['new_password_confirm']:
                response = {
                    "message": "Password Confirm Unmatch"
                }
                return Response(response, status=st.HTTP_400_BAD_REQUEST)

            # to get newest AccessToken
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
    authentication_classes = (MyCustomAuthentication,)
    permission_classes = [IsAuthenticated]
    serializer_class = UnlockSessionSerializer

    def post(self, request):
        post_data = request.data

        print("POST DATA====================================================", post_data, flush=True)

        # get user credentials by encrypted password
        string_to_hash = post_data["password"] + request.user.username

        user_password = jwt.encode({
            'username':request.user.username,
            'password':post_data["password"],
        }, settings.JWT_USER_KEY).decode()
        hash_result = hashlib.sha256(string_to_hash.encode()).hexdigest()

        post_data["username"] = request.user.username
        post_data["password"] = user_password

        session_unlock = self.serializer_class(data=post_data)
        valid = session_unlock.is_valid()
        if valid:
            try:
                user = User.objects.get(**post_data)
            except User.DoesNotExist:
                response = {
                    "message": "Invalid Password"
                }
                return Response(response, status=st.HTTP_401_UNAUTHORIZED)

            response = {
                'message': "Unlocked!",
            }

            return Response(response, status=st.HTTP_200_OK)
        else:
            response = {
                "message": "Incorrect Field Value"
            }
            return Response(response, status=st.HTTP_400_BAD_REQUEST)


class StoredReplayViewSet(viewsets.ModelViewSet):
    authentication_classes = (MyCustomAuthentication,)
    permission_classes = [IsAuthenticated]
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

        print("NOT SEQUENCED", session_id, flush=True)

        response = {
            "data": replay_data
        }

        return Response(response, status=st.HTTP_200_OK)

        # res = json.loads(stored_replays[1].data)
        # print(stored_replays[0].id, flush=True)
        # return Response(res["track_play"], status=st.HTTP_200_OK)
        # return Response({}, status=st.HTTP_200_OK)

    def sequenced(self, request, session_id, sequence):
        try:
            session = Session.objects.get(pk=session_id)

            # get maximum sequence on this session
            total_sequence = self.queryset.filter(session=session).order_by('-sequence').count()

            sequence_data = self.queryset.filter(session=session, sequence=sequence).first()
            if sequence_data:
                sequence_response = json.loads(sequence_data.data)
                sequence_response["sequence"] = sequence
                sequence_response["total_sequence"] = total_sequence
            else:
                if total_sequence == 0:
                    # cek apakah session ada tapi stored replay belom ada alias masih generate
                    response = {
                        "message": "In progress of generating session"
                    }
                    return Response(response, status=st.HTTP_404_NOT_FOUND)
                elif int(sequence) > total_sequence - 1:
                    # cek apakah nilai sequence yang diminta melebihi jumlah sequence yang ada
                    sequence_response = {}

            response = {
                "data": sequence_response
            }

        except Session.DoesNotExist:
            response = {
                "message": "Session does not exist"
            }

            return Response(response, status=st.HTTP_404_NOT_FOUND)

        return Response(response, status=st.HTTP_200_OK)


class AppSettingViewSet(viewsets.ModelViewSet):
    authentication_classes = (MyCustomAuthentication,)
    permission_classes = [IsAuthenticated]
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
    # authentication_classes = (MyCustomAuthentication, )
    # permission_classes = [IsAuthenticated]
    serializer_class = RestoreFileSerializer

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
        file_name = "sav_backup_session_" + str(session_id) + ".sql"

        # ==================================================================================
        # STREAMED DOWNLOAD RESPONSE
        # ==================================================================================
        response = StreamingHttpResponse(self.iter_items(string_query, self.Echo()),
                                         content_type="application/sql")
        response['Content-Disposition'] = 'attachment; filename=' + file_name
        return response

        # print(file_name, flush=True)
        # file_path = os.path.join(settings.BASE_DIR, file_path)
        # if os.path.exists(file_path):
        #     with open(file_path, 'r') as fh:
        #         response = HttpResponse(content_type="application/zip", status=st.HTTP_200_OK)
        #
        #         zf = ZipFile(response, 'w')
        #         zf.writestr(file_name, fh.read())
        #
        #     response['Content-Disposition'] = 'attachment; filename=' + "sav_backup_session_"+str(session_id)+".zip"
        #     return response
        # return Response({"message": "File Not Found"}, status=st.HTTP_404_NOT_FOUND)

    def restore(self, request):
        must_replace = request.query_params.get('replace')
        must_replace = True if must_replace == "true" else False

        dump_file = request.FILES["dump_file"]

        file_name = dump_file.name.split('.')
        splitted_name = file_name[0].split('_')

        session_id = splitted_name[-1]

        if file_name[1] != 'sql':
            response = {
                "message": "File extension must be .sql"
            }
            return Response(response, status=st.HTTP_400_BAD_REQUEST)

        if 'sav' != splitted_name[0] or 'backup' != splitted_name[1] or 'session' != splitted_name[2]:
            response = {
                "message": "Invalid file name"
            }
            return Response(response, status=st.HTTP_400_BAD_REQUEST)

        session = Session.objects.filter(pk=session_id)
        if session.exists() and not must_replace:  # must_replace = False
            response = {
                "message": "Session exists"
            }
            return Response(response, status=st.HTTP_400_BAD_REQUEST)

        success, error = db_operation.restore_file_handler(dump_file, replaced=must_replace,
                                                           session_id=session_id)  # must_replace = True

        if not success:
            response = {
                "message": error
            }

            return Response(response, status=st.HTTP_400_BAD_REQUEST)

        response = {
            "success": True
        }
        return Response(response, status=st.HTTP_200_OK)

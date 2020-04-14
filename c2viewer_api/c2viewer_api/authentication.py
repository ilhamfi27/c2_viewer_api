from rest_framework import authentication
from rest_framework import exceptions
from api.models import User
from rest_framework_jwt.authentication import JSONWebTokenAuthentication

class MyCustomAuthentication(authentication.BaseAuthentication):
    def authenticate(self, request):
        username = request.META.get('X_USERNAME') # get the username request header
        if not username: # no username passed in request headers
            return None # authentication did not succeed

        try:
            user = User.objects.get(username=username) # get the user
        except User.DoesNotExist:
            raise exceptions.AuthenticationFailed('No such user') # raise exception if user does not exist

        return (user, None) # authentication successful


class AuthenticatedServiceClient:
    def is_authenticated(self):
        return True


class JwtServiceOnlyAuthentication(JSONWebTokenAuthentication):
    def authenticate_credentials(self, payload):
        # Assign properties from payload to the AuthenticatedServiceClient object if necessary
        return AuthenticatedServiceClient()

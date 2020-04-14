from rest_framework import authentication
from rest_framework import exceptions
from api.models import AccessToken

class MyCustomAuthentication(authentication.BaseAuthentication):
    def authenticate(self, request):
        token = request.META.get('HTTP_AUTHORIZATION') # get the username request header
        try:
            access_token = AccessToken.objects.get(token=token)
        except AccessToken.DoesNotExist:
            raise exceptions.AuthenticationFailed('Invalid Token') # raise exception if user does not exist

        if not token: # no username passed in request headers
            return None # authentication did not succeed

        return (access_token, None) # authentication successful

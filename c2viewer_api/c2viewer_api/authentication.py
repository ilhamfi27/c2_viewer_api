from rest_framework import authentication
from rest_framework import exceptions
from api.models import AccessToken, User

class MyCustomAuthentication(authentication.BaseAuthentication):
    def authenticate(self, request):
        header_token = request.META.get('HTTP_AUTHORIZATION') # get the username request header

        try:
            token_string = header_token.split()

            token_indicator = token_string[0]
            token = token_string[1]

            if token_indicator != "Token":
                raise exceptions.AuthenticationFailed('Invalid Token') # raise exception if token isn't correct
        except IndexError:
            raise exceptions.AuthenticationFailed('Invalid Token') # raise exception if token isn't correct

        try:
            access_token = AccessToken.objects.get(token=token)
            user = access_token.user
        except AccessToken.DoesNotExist:
            raise exceptions.AuthenticationFailed('Invalid Token') # raise exception if user does not exist

        return (user, None) # authentication successful

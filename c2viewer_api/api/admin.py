from django.contrib import admin

from .models import Location, User, StoredReplay, Session, AppSetting

admin.site.register(Location)
admin.site.register(User)
admin.site.register(StoredReplay)
admin.site.register(Session)
admin.site.register(AppSetting)

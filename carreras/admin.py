from django.contrib import admin
from django.http import HttpRequest
from .models import Carrera
from guardian.admin import GuardedModelAdmin
class CarreraAdmin(GuardedModelAdmin):
    pass


admin.site.register(Carrera, CarreraAdmin)

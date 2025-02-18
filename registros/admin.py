from django.contrib import admin
from .models import Ingreso, Egreso, Titulacion, LiberacionIngles

admin.site.register([Ingreso, Egreso, Titulacion, LiberacionIngles])

from django.urls import path
from . import views

urlpatterns = [
    path('nuevo-ingreso/', views.ReportesNuevoIngreso.as_view(), name='nuevo-ingreso'),
    path('egreso/', views.ReportesEgreso.as_view(), name='egreso'),
    path('titulacion/', views.ReportesTitulacion.as_view(), name='titulacion'),
]
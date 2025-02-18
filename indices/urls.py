from django.urls import path
from . import views

urlpatterns = [
    path('permanencia/', views.IndicesPermanencia.as_view(), name='permanencia'),
    path('egreso/', views.IndicesEgreso.as_view(), name='egreso'),
    path('titulacion/', views.IndicesTitulacion.as_view(), name='titulacion'),
    path('desercion/', views.IndicesDesercion.as_view(), name='desercion'),
]
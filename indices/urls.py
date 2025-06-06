from django.urls import path
from . import views

urlpatterns = [
    path('permanencia/', views.IndicesPermanencia.as_view(), name='permanencia'),
    path('egreso/', views.IndicesEgreso.as_view(), name='egreso'),
    path('titulacion/', views.IndicesTitulacion.as_view(), name='titulacion'),
    path('desercion/', views.IndicesDesercion.as_view(), name='desercion'),
    path('desercion/generacional', views.IndicesGeneracionalDesercion.as_view(), name='desercion_generacional'),
    path('permanencia/generacional', views.IndicesGeneracionalPermanencia.as_view(), name='permanencia_generacional'),
    path('egreso/generacional', views.IndicesGeneracionalEgreso.as_view(), name='egreso_generacional'),
    path('titulacion/generacional', views.IndicesGeneracionalTitulacion.as_view(), name='titulacion_generacional'),
]
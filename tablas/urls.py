from django.urls import path
from . import views

urlpatterns = [
    path('poblacion/', views.TablasPoblacion.as_view(), name='poblacion'),
    path('crecimiento/', views.TablasCrecimiento.as_view(), name='crecimiento'),
]
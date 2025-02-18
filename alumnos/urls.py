from django.urls import path
from . import views

urlpatterns = [
    path('', views.AlumnoList.as_view(), name='alumno-list'),
    path('<str:pk>/', views.AlumnoDetail.as_view(), name='alumno-view'),
    path('historial', views.HistorialList.as_view(), name='historial-list'),
    path('historial/<str:pk>/', views.HistorialDetail.as_view(), name='historial-detail'),
]
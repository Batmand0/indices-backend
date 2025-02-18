from django.urls import path
from . import views

urlpatterns = [
    path('', views.DiscapacidadList.as_view(), name='discapacidad-list'),
    path('<int:pk>/', views.DiscapacidadDetail.as_view(), name='discapacidad-detail'),
]
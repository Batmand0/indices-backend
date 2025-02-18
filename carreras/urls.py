from django.urls import path
from . import views

urlpatterns = [
    path('', views.CarreraList.as_view(), name='carrera-list'),
    path('todas/', views.CarreraListAll.as_view(), name='carrera-list'),
    path('usuario/', views.CarreraListForUser.as_view(), name='carrera-list'),
    path('permisos/', views.AsignarPermisos.as_view(), name='carrera-permisos'),
    path('remover-permisos/', views.RemoverPermisos.as_view(), name='carrera-remover-permisos'),
    path('remover-permisos/todos/', views.RemoverTodosPermisos.as_view(), name='carrera-remover-permisos'),
    path('<str:pk>/', views.CarreraDetail.as_view(), name='carrera-detail'),
]
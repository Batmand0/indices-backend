from django.urls import path, re_path
from . import views

urlpatterns = [
    path('ingresos/', views.IngresoList.as_view(), name='ingresos-list'),
    path('ingresos/<int:pk>/', views.IngresoDetail.as_view(), name='ingresos-detail'),
    re_path(r'^ingresos/subir/(?P<filename>[^/]+)$', views.IngresoUpload.as_view(), name='ingresos-upload'),
    path('egresos/', views.EgresoList.as_view(), name='egresos-list'),
    path('egresos/<int:pk>/', views.EgresoDetail.as_view(), name='egresos-detail'),
    re_path(r'^egresos/subir/(?P<filename>[^/]+)$', views.EgresoUpload.as_view(), name='egresos-upload'),
    path('titulaciones/', views.TitulacionList.as_view(), name='titulaciones-list'),
    path('titulaciones/<int:pk>/', views.TitulacionDetail.as_view(), name='titulaciones-detail'),
    re_path(r'^titulaciones/subir/(?P<filename>[^/]+)$', views.TitulacionUpload.as_view(), name='titulaciones-upload'),
    path('liberaciones-ingles/', views.LiberacionInglesList.as_view(), name='liberaciones-ingles-list'),
    path('liberaciones-ingles/<int:pk>/', views.LiberacionInglesDetail.as_view(), name='liberaciones-ingles-detail'),
    re_path(r'^liberaciones-ingles/subir/(?P<filename>[^/]+)$', views.LiberacionInglesUpload.as_view(), name='liberaciones-ingles-upload'),
    path('realizar-corte/', views.corte, name='corte'),
]
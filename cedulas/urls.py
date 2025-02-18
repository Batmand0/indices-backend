from django.urls import path
from . import views

urlpatterns = [
    path('cacei/', views.CedulasCACEI.as_view(), name='cacei'),
    path('caceca/', views.CedulasCACECA.as_view(), name='caceca'),

]
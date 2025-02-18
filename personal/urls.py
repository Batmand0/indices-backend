from django.urls import path
from . import views

urlpatterns = [
    path('', views.PersonalList.as_view(), name='personal-list'),
    path('<str:pk>/', views.PersonalDetail.as_view(), name='personal-detail'),
]
from django.urls import path
from . import views

urlpatterns = [
    path('', views.PlanList.as_view(), name='plan-list'),
    path('<str:pk>/', views.PlanDetail.as_view(), name='plan-detail'),

]
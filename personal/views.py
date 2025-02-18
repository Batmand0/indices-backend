from .models import Personal
from .serializers import PersonalSerializer
from rest_framework import generics
from rest_framework.permissions import IsAuthenticated
from backend.permissions import IsAdminUserOrReadOnly

class PersonalList(generics.ListCreateAPIView):
    queryset = Personal.objects.all()
    serializer_class = PersonalSerializer
    permission_classes = [IsAuthenticated&IsAdminUserOrReadOnly]

class PersonalDetail(generics.RetrieveUpdateDestroyAPIView):
    queryset = Personal.objects.all()
    serializer_class = PersonalSerializer
    permission_classes = [IsAuthenticated&IsAdminUserOrReadOnly]
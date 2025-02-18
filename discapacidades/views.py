from rest_framework.generics import ListCreateAPIView, RetrieveUpdateDestroyAPIView
from rest_framework.permissions import IsAuthenticated
from backend.permissions import IsAdminUserOrReadOnly
from .serializers import DiscapacidadSerializer
from .models import Discapacidad

class DiscapacidadList(ListCreateAPIView):
    queryset = Discapacidad.objects.all()
    serializer_class = DiscapacidadSerializer
    permission_classes = [IsAuthenticated&IsAdminUserOrReadOnly]

class DiscapacidadDetail(RetrieveUpdateDestroyAPIView):
    queryset = Discapacidad.objects.all()
    serializer_class = DiscapacidadSerializer
    permission_classes = [IsAuthenticated&IsAdminUserOrReadOnly]

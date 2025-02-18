from rest_framework import generics
from rest_framework.response import Response
from rest_framework.decorators import api_view, permission_classes
from rest_framework.permissions import AllowAny, IsAuthenticated
from backend.permissions import IsAdminUserOrReadOnly, IsOwnerOrReadOnly
from rest_framework_simplejwt.views import TokenObtainPairView
from django.contrib.auth.password_validation import validate_password
from django.core.exceptions import ValidationError
from carreras.models import Carrera

from .serializers import CustomTokenObtainPairSerializer, RegisterSerializer, UserSerializer, UserListSerializer
from .models import Usuario

class CustomTokenObtainPairView(TokenObtainPairView):
    serializer_class = CustomTokenObtainPairSerializer


class RegisterView(generics.CreateAPIView):
    queryset = Usuario.objects.all()
    permission_classes = (AllowAny,)
    serializer_class = RegisterSerializer

class UserListView(generics.ListCreateAPIView):
    queryset = Usuario.objects.all().exclude(username = "AnonymousUser").values('id', 'username', 'email')
    permission_classes = [IsAuthenticated&IsAdminUserOrReadOnly]
    serializer_class = UserListSerializer

class UserDetail(generics.RetrieveUpdateAPIView):
    queryset = Usuario.objects.all().exclude(username = 'AnonymousUser')
    permission_classes = [IsAuthenticated&IsAdminUserOrReadOnly|IsOwnerOrReadOnly]
    serializer_class = UserSerializer

@api_view(['POST'])
@permission_classes([IsAuthenticated,])
def cambio_contrasena(request):
    new_password1 = request.data['new_password1']
    new_password2 = request.data['new_password2']
    print(new_password1,new_password2)

    if new_password1 is None or new_password1 != new_password2:
        return Response(status=400, data={'message': 'Las contraseñas no son iguales'})

    try:
        validate_password(new_password1)
    except ValidationError as ex:
        return Response(status=400, data={'errors': ex})

    user = request.user
    user.set_password(new_password1)
    user.save()
    return Response(status=200, data={'message': 'Contraseña cambiada exitosamente'})
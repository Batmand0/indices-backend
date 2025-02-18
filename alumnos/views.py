from rest_framework.permissions import IsAuthenticated
from rest_framework import generics

from backend.permissions import IsAdminUserOrReadOnly
from .serializers import AlumnoSerializer, HistorialSerializer
from carreras.models import Carrera
from planes.models import Plan
from .models import Alumno


class AlumnoList(generics.ListCreateAPIView):
    queryset = Alumno.objects.all()
    serializer_class = AlumnoSerializer
    permission_classes = [IsAuthenticated&IsAdminUserOrReadOnly]

class AlumnoDetail(generics.RetrieveUpdateDestroyAPIView):
    queryset = Alumno.objects.all()
    serializer_class = AlumnoSerializer
    permission_classes = [IsAuthenticated&IsAdminUserOrReadOnly]

class HistorialList(generics.ListAPIView):
    serializer_class = HistorialSerializer
    permission_classes = [IsAuthenticated]

    def get_queryset(self):
        queryset = Alumno.objects.all()
        tipos_ingresos = []
        if self.request.query_params.get('nuevo-ingreso'):
            tipos_ingresos.extend(['EX','CO'])
        if self.request.query_params.get('traslado-equivalencia'):
            tipos_ingresos.extend(['TR','EQ'])
        carrera_param = self.request.query_params.get('carrera')
        cohorte_param = self.request.query_params.get('cohorte')

        if carrera_param is not None:
            try:
                carrera_obj = Carrera.objects.get(pk=carrera_param)
                planes = Plan.objects.filter(carrera=carrera_obj)
                queryset = queryset.filter(plan__in=planes)
            except:
                print(f'No se encontró una carrera con la clave "{carrera_param}"')
        if cohorte_param is not None:
            queryset = queryset.filter(ingreso__periodo=cohorte_param, ingreso__tipo__in=tipos_ingresos)
        else:
            queryset = queryset.filter(ingreso__tipo__in=tipos_ingresos)
        return queryset

class HistorialDetail(generics.RetrieveAPIView):
    queryset = Alumno.objects.all()
    serializer_class = HistorialSerializer
    permission_classes = [IsAuthenticated]
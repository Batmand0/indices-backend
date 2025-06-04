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
        
        # Convertir explícitamente a booleano
        nuevo_ingreso = self.request.query_params.get('nuevo-ingreso', '').lower() == 'true'
        traslado_equivalencia = self.request.query_params.get('traslado-equivalencia', '').lower() == 'true'
        
        # Agregar logging para debug
        import logging
        logger = logging.getLogger(__name__)
        logger.info(f"""
            Filtros solicitados:
            nuevo_ingreso: {nuevo_ingreso}
            traslado_equivalencia: {traslado_equivalencia}
        """)
        
        # Aplicar filtros según los booleanos
        if nuevo_ingreso:
            tipos_ingresos.extend(['EX', 'CO'])
        if traslado_equivalencia:
            tipos_ingresos.extend(['TR', 'EQ'])
            
        # Si no hay tipos seleccionados, retornar queryset vacío
        if not tipos_ingresos:
            return Alumno.objects.none()
            
        carrera_param = self.request.query_params.get('carrera')
        cohorte_param = self.request.query_params.get('cohorte')

        # Log de tipos de ingreso a filtrar
        logger.info(f"Tipos de ingreso a filtrar: {tipos_ingresos}")

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
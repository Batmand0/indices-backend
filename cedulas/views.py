from django.http import JsonResponse
# Create your views here.
from django.db.models import Count, F, Q
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import permissions

from registros.models import Ingreso, Egreso, Titulacion
from registros.periodos import calcularPeriodos, getPeriodoActual

from decimal import Decimal
class CedulasCACEI(APIView):
    """
    Vista para generar la tabla de cédulas CACEI.

    * Requiere autenticación por token.

    ** nuevo-ingreso: Alumnos ingresando en 1er por examen o convalidacion
    ** traslado-equivalencia: Alumnos ingresando de otro TEC u otra escuela
    ** cohorte: El periodo donde empezara el calculo
    ** semestres: Cuantos semestres seran calculados desde el cohorte
    ** carrera: El programa educativo que se esta midiendo
    """
    permission_classes = [permissions.IsAuthenticated]

    def get(self, request, format=None):
        nuevo_ingreso = request.GET.get('nuevo-ingreso')
        traslado_equivalencia = request.GET.get('traslado-equivalencia')
        cohorte = request.GET.get('cohorte') if request.GET.get('cohorte') else getPeriodoActual()
        carrera = request.GET.get('carrera')

        tipos = []
        if nuevo_ingreso:
            tipos.extend(['EX', 'CO'])
        if traslado_equivalencia:
            tipos.extend(['TR', 'EQ'])

        response_data = {}
        periodoInicial = cohorte
        periodos = calcularPeriodos(periodoInicial, int(24))
        inicioP = 0
        for gen in range(10):
            poblacion_nuevo_ingreso = 0
            alumnos_total = Count("alumno__plan__carrera__pk", filter=Q(tipo__in=tipos, periodo=periodoInicial))
            poblacion_total = Ingreso.objects.aggregate(poblacion=alumnos_total)
            alumnos_carrera = Ingreso.objects.filter(tipo__in=tipos, periodo=periodoInicial,alumno__plan__carrera__pk=carrera).annotate(clave=F("alumno_id")
                ).values("clave")
            poblacion_egr = 0
            poblacion_titulo = 0
            # print(list(filter(lambda p: periodos.index(p) >= inicioP and periodos.index(p) < (inicioP+14), periodos)))
            for i, periodo in enumerate(list(filter(lambda p: periodos.index(p) >= inicioP and periodos.index(p) <= (inicioP+10), periodos))):
                if periodo == periodoInicial:
                    activos = Count("alumno__plan__carrera__pk", filter=Q(tipo__in=tipos, periodo=periodos[inicioP],alumno__plan__carrera__pk=carrera))
                    poblacion_act = Ingreso.objects.aggregate(poblacion=activos)
                    poblacion_nuevo_ingreso = poblacion_act['poblacion']
                else:
                    activos = Count("alumno__plan__carrera__pk", filter=Q(tipo='RE', periodo=periodo, alumno__plan__carrera__pk=carrera))
                    poblacion_act = Ingreso.objects.aggregate(poblacion=activos)
                inactivos = Count("alumno__plan__carrera__pk", filter=Q(alumno_id__in=alumnos_carrera, periodo=periodo))
                if int(i) < 10:
                    poblacion_titulo = poblacion_titulo + Titulacion.objects.aggregate(titulados=inactivos)['titulados']
                poblacion_egr = poblacion_egr + Egreso.objects.aggregate(egresados=inactivos)['egresados']
            if poblacion_nuevo_ingreso == 0:
                tasa_egreso = 0
                tasa_titulo = 0
            else :
                tasa_egreso = Decimal((poblacion_egr*100)/poblacion_nuevo_ingreso)
                tasa_egreso = round(tasa_egreso, 2)
                tasa_titulo = Decimal((poblacion_titulo*100)/poblacion_nuevo_ingreso)
                tasa_titulo = round(tasa_titulo, 2)
            if poblacion_total['poblacion'] == 0:
                porcentaje_alumnos_carrera = 0
            else :
                porcentaje_alumnos_carrera = Decimal((poblacion_nuevo_ingreso*100)/poblacion_total['poblacion'])
                porcentaje_alumnos_carrera = round(porcentaje_alumnos_carrera, 2)
            inicio = ""
            fin = ""
            if periodos[inicioP].endswith('1'):
                inicio = "2/{p}".format(p=periodos[inicioP])
            else:
                inicio = "8/{p}".format(p=periodos[inicioP])
            if periodos[inicioP+8].endswith('1'):
                fin = "6/{p}".format(p=periodos[inicioP+8])
            else:
                fin = "12/{p}".format(p=periodos[inicioP+8])
            generacion = "{inicio} - {fin}".format(inicio=inicio, fin=fin)
            response_data[generacion] = dict(poblacion_total=poblacion_total['poblacion'], poblacion=poblacion_nuevo_ingreso, porcentaje_alumnos_carrera=porcentaje_alumnos_carrera, egresados=poblacion_egr, tasa_egreso=tasa_egreso, titulados=poblacion_titulo, tasa_titulacion=tasa_titulo)
            inicioP += 1
            periodoInicial = periodos[inicioP]

        return Response(response_data)

class CedulasCACECA(APIView):
    """
    Vista para generar la tabla de cédulas CACECA.

    * Requiere autenticación por token.

    ** nuevo-ingreso: Alumnos ingresando en 1er por examen o convalidacion
    ** traslado-equivalencia: Alumnos ingresando de otro TEC u otra escuela
    ** cohorte: El periodo donde empezara el calculo
    ** carrera: El programa educativo que se esta midiendo
    """
    permission_classes = [permissions.IsAuthenticated]

    def get(self, request, format=None):
        nuevo_ingreso = request.GET.get('nuevo-ingreso')
        traslado_equivalencia = request.GET.get('traslado-equivalencia')
        cohorte = request.GET.get('cohorte') if request.GET.get('cohorte') else getPeriodoActual()
        carrera = request.GET.get('carrera')

        tipos = []
        if nuevo_ingreso:
            tipos.extend(['EX', 'CO'])
        if traslado_equivalencia:
            tipos.extend(['TR', 'EQ'])

        response_data = {}
        periodoInicial = cohorte
        for gen in range(3):
            periodos = calcularPeriodos(periodoInicial, int(9))
            poblacion_nuevo_ingreso = 0
            alumnos = Ingreso.objects.filter(tipo__in=tipos, periodo=periodoInicial,alumno__plan__carrera__pk=carrera).annotate(clave=F("alumno_id")
                ).values("clave")
            alumnos_corte_anterior = 0
            poblacion_egr = 0
            poblacion_titulo = 0
            desercion = 0
            for periodo in periodos:
                if periodo == periodos[0]:
                    activos = Count("alumno__plan__carrera__pk", filter=Q(tipo__in=tipos, alumno__in=alumnos, periodo=periodoInicial,alumno__plan__carrera__pk=carrera))
                    poblacion_act = Ingreso.objects.aggregate(poblacion=activos)
                    poblacion_nuevo_ingreso = poblacion_act['poblacion']
                    alumnos_corte_anterior = poblacion_act['poblacion']
                else:
                    activos = Count("alumno__plan__carrera__pk", filter=Q(tipo='RE', alumno__in=alumnos, periodo=periodo, alumno__plan__carrera__pk=carrera))
                    poblacion_act = Ingreso.objects.aggregate(poblacion=activos)
                inactivos = Count("alumno__plan__carrera__pk", filter=Q(alumno_id__in=alumnos, periodo=periodo))
                poblacion_egr = poblacion_egr + Egreso.objects.aggregate(egresados=inactivos)['egresados']
                poblacion_titulo = poblacion_titulo + Titulacion.objects.aggregate(titulados=inactivos)['titulados']
                desercion = desercion + (alumnos_corte_anterior - poblacion_act['poblacion'] - poblacion_egr)
                if desercion < 0:
                    desercion = 0
                alumnos_corte_anterior = poblacion_act['poblacion']
            if poblacion_nuevo_ingreso == 0:
                tasa_desercion = 0
                tasa_egreso = 0
                desercion = 0
                reprobacion = 0
                tasa_reprobacion = 0
                tasa_titulo = 0
            else :
                tasa_desercion = Decimal((desercion*100)/poblacion_nuevo_ingreso)
                tasa_desercion = round(tasa_desercion, 2)
                tasa_egreso = Decimal((poblacion_egr*100)/poblacion_nuevo_ingreso)
                tasa_egreso = round(tasa_egreso, 2)
                tasa_titulo = Decimal((poblacion_titulo*100)/poblacion_nuevo_ingreso)
                tasa_titulo = round(tasa_titulo, 2)
                reprobacion = poblacion_nuevo_ingreso - poblacion_egr - desercion
                tasa_reprobacion = Decimal((reprobacion*100)/poblacion_nuevo_ingreso)
                tasa_reprobacion = round(tasa_reprobacion, 2)
            generacion = "{inicio} - {fin}".format(inicio=periodos[0], fin=periodos[8])
            response_data[generacion] = dict(poblacion=poblacion_nuevo_ingreso, desercion=desercion, tasa_desercion=tasa_desercion, reprobacion=reprobacion, tasa_reprobacion=tasa_reprobacion, egresados=poblacion_egr, titulados=poblacion_titulo, tasa_titulacion=tasa_titulo, tasa_egreso=tasa_egreso)
            periodoInicial = periodos[1]
        return Response(response_data)
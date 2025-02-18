from django.http import JsonResponse
# Create your views here.
from django.db.models import Count, F, Q
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import permissions

from registros.models import Ingreso, Egreso, Titulacion
from planes.models import Plan
from carreras.models import Carrera
from registros.periodos import calcularPeriodos, getPeriodoActual

from decimal import Decimal
from indices.views import obtenerPoblacionEgreso, obtenerPoblacionTitulada, obtenerPoblacionActiva, calcularTasa, calcularTipos

def actualizarTotales(registros, datos_nuevos):
    registros['total'] = registros['total'] + datos_nuevos['total']
    registros['hombres'] = registros['hombres'] + datos_nuevos['hombres']
    registros['mujeres'] = registros['mujeres'] + datos_nuevos['mujeres']
    return registros

def crearTotales():
    registros = {}
    registros['total'] = 0
    registros['hombres'] = 0
    registros['mujeres'] = 0
    return registros

def obtenerPoblacionNuevoIngreso(tipos_ingreso, periodo, carrera):
    hombres = Count("alumno__plan__carrera__pk", filter=Q(tipo__in=tipos_ingreso, periodo=periodo,alumno__plan__carrera__pk=carrera, alumno__curp__genero='H'))
    mujeres = Count("alumno__plan__carrera__pk", filter=Q(tipo__in=tipos_ingreso, periodo=periodo,alumno__plan__carrera__pk=carrera, alumno__curp__genero='M'))
    activos = Count("alumno__plan__carrera__pk", filter=Q(tipo__in=tipos_ingreso, periodo=periodo,alumno__plan__carrera__pk=carrera))
    poblacion = Ingreso.objects.aggregate(poblacion=activos, hombres=hombres, mujeres=mujeres)
    return poblacion

class ReportesNuevoIngreso(APIView):
    """
    Vista para listar la cantidad de alumnos de nuevo ingreso por carrera.

    * Requiere autenticación por token.

    ** nuevo-ingreso: Alumnos ingresando en 1er por examen o convalidacion
    ** traslado-equivalencia: Alumnos ingresando de otro TEC u otra escuela
    ** cohorte: El periodo donde empezara el calculo
    ** semestres: Cuantos semestres seran calculados desde el cohorte
    """
    permission_classes = [permissions.IsAuthenticated]

    def get(self, request, format=None):
        nuevo_ingreso = request.query_params.get('nuevo-ingreso')
        traslado_equivalencia = request.query_params.get('traslado-equivalencia')
        cohorte = request.query_params.get('cohorte') if request.query_params.get('cohorte') else getPeriodoActual()
        semestres = request.query_params.get('semestres') if request.query_params.get('semestres') else '9'

        tipos = calcularTipos(nuevo_ingreso,traslado_equivalencia)

        response_data = {}
        periodos = calcularPeriodos(cohorte, int(semestres))
        planes = Carrera.objects.select_related('clave__plan').values_list('clave', 'plan', 'nombre')
        for plan in planes:
            plan_regs = {}
            for periodo in periodos:
                plan_regs[periodo] = dict(periodo=periodo)
                nuevo_ingreso = obtenerPoblacionNuevoIngreso(tipos, periodo, plan[0])
                plan_regs[periodo] = dict(hombres=nuevo_ingreso['hombres'], mujeres=nuevo_ingreso['mujeres'], periodo=periodo)
            response_data[plan[2]] = plan_regs

        return Response(response_data)

class ReportesEgreso(APIView):
    """
    Vista para listar la cantidad de alumnos de nuevo ingreso por carrera.

    * Requiere autenticación por token.

    ** nuevo-ingreso: Alumnos ingresando en 1er por examen o convalidacion
    ** traslado-equivalencia: Alumnos ingresando de otro TEC u otra escuela
    ** cohorte: El periodo donde empezara el calculo
    ** semestres: Cuantos semestres seran calculados desde el cohorte
    """
    permission_classes = [permissions.IsAuthenticated]

    def get(self, request, format=None):
        nuevo_ingreso = request.query_params.get('nuevo-ingreso')
        traslado_equivalencia = request.query_params.get('traslado-equivalencia')
        cohorte = request.query_params.get('cohorte') if request.query_params.get('cohorte') else getPeriodoActual()
        semestres = request.query_params.get('semestres') if request.query_params.get('semestres') else '9'

        tipos = calcularTipos(nuevo_ingreso, traslado_equivalencia)

        response_data = {}
        periodos = calcularPeriodos(cohorte, int(semestres))
        carreras = Carrera.objects.select_related('clave__plan').values_list('clave', 'plan', 'nombre')
        for carrera in carreras:
            alumnos = Ingreso.objects.filter(tipo__in=tipos, periodo=cohorte,alumno__plan__carrera__pk=carrera[0]).annotate(clave=F("alumno_id")
                ).values("clave")
            poblacion_nuevo_ingreso = obtenerPoblacionActiva(tipos, alumnos, cohorte, carrera[0])
            registros__semestres = {}
            egreso_total = crearTotales()

            for sem in range(8,int(semestres) if int(semestres) <= 12 else 12):
                egreso_periodo = obtenerPoblacionEgreso(alumnos, periodos[sem])
                egreso_total = actualizarTotales(egreso_total, egreso_periodo)
                registros__semestres[sem+1] = dict(hombres=egreso_periodo['hombres'], mujeres=egreso_periodo['mujeres'])

            # registros__semestres['total_1'] = dict(hombres=egreso_total['hombres'], mujeres=egreso_total['mujeres'])
            registros__semestres['total_1'] = dict(valor=egreso_total['total'])
            tasa_egreso = calcularTasa(egreso_total['total'], poblacion_nuevo_ingreso['poblacion'])
            registros__semestres['tasa_egreso_1'] = dict(valor="{tasa} %".format(tasa=tasa_egreso))

            egreso_total = crearTotales()
            if int(semestres) > 12:
                for i in range(12,int(semestres)):
                    egreso_periodo = obtenerPoblacionEgreso(alumnos, periodos[i])
                    egreso_total = actualizarTotales(egreso_total, egreso_periodo)

                registros__semestres[13] = dict(hombres=egreso_total['hombres'], mujeres=egreso_total['mujeres'])
                tasa_egreso = calcularTasa(egreso_total['total'], poblacion_nuevo_ingreso['poblacion'])
                registros__semestres['tasa_egreso_2'] = dict(valor="{tasa} %".format(tasa=tasa_egreso))
            response_data[carrera[2]] = dict(carrera=carrera[2],poblacion_nuevo_ingreso=poblacion_nuevo_ingreso , registros=registros__semestres)

        return Response(response_data)

class ReportesTitulacion(APIView):
    """
    Vista para listar la cantidad de alumnos de nuevo ingreso por carrera.

    * Requiere autenticación por token.

    ** nuevo-ingreso: Alumnos ingresando en 1er por examen o convalidacion
    ** traslado-equivalencia: Alumnos ingresando de otro TEC u otra escuela
    ** cohorte: El periodo donde empezara el calculo
    ** semestres: Cuantos semestres seran calculados desde el cohorte
    """
    permission_classes = [permissions.IsAuthenticated]

    def get(self, request, format=None):
        nuevo_ingreso = request.query_params.get('nuevo-ingreso')
        traslado_equivalencia = request.query_params.get('traslado-equivalencia')
        cohorte = request.query_params.get('cohorte') if request.query_params.get('cohorte') else getPeriodoActual()
        semestres = request.query_params.get('semestres') if request.query_params.get('semestres') else '9'

        tipos = calcularTipos(nuevo_ingreso, traslado_equivalencia)

        response_data = {}
        periodos = calcularPeriodos(cohorte, int(semestres))
        carreras = Carrera.objects.select_related('clave__plan').values_list('clave', 'plan', 'nombre')
        for carrera in carreras:
            alumnos = Ingreso.objects.filter(tipo__in=tipos, periodo=cohorte,alumno__plan__carrera__pk=carrera[0]).annotate(clave=F("alumno_id")
                ).values("clave")
            registros__semestres = {}
            poblacion_nuevo_ingreso = obtenerPoblacionActiva(tipos, alumnos, cohorte, carrera[0])
            titulados_total = crearTotales()
            for i in range(8,int(semestres) if int(semestres) <= 12 else 12):
                titulados_periodo = obtenerPoblacionTitulada(alumnos, periodos[i])
                titulados_total = actualizarTotales(titulados_total, titulados_periodo)
                registros__semestres[i+1] = dict(hombres=titulados_periodo['hombres'], mujeres=titulados_periodo['mujeres'])

            # registros__semestres['total_1'] = dict(hombres=titulados_total['hombres'], mujeres=titulados_total['mujeres'])
            registros__semestres['total_1'] = dict(valor=titulados_total['total'])
            tasa_titulados = calcularTasa(titulados_total['total'], poblacion_nuevo_ingreso['poblacion'])
            registros__semestres['tasa_titulacion_1'] = dict(valor="{tasa_titulados} %".format(tasa_titulados=tasa_titulados))

            titulados_total = crearTotales()
            if int(semestres) > 12:
                for i in range(12,int(semestres)):
                    titulados_periodo = obtenerPoblacionTitulada(alumnos, periodos[i])
                    titulados_total = actualizarTotales(titulados_total, titulados_periodo)
                registros__semestres[13] = dict(hombres=titulados_total['hombres'], mujeres=titulados_total['mujeres'])
                tasa_titulados = calcularTasa(titulados_total['total'], poblacion_nuevo_ingreso['poblacion'])
                registros__semestres['tasa_titulacion_2'] = dict(valor="{tasa_titulados} %".format(tasa_titulados=tasa_titulados))
            response_data[carrera[2]] = dict(poblacion_nuevo_ingreso=poblacion_nuevo_ingreso , registros=registros__semestres)

        return Response(response_data)

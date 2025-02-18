from django.http import JsonResponse
# Create your views here.
from django.db.models import Count, F, Q
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import permissions

from registros.models import Ingreso, Egreso, Titulacion
from registros.periodos import calcularPeriodos
from personal.models import Personal

from decimal import Decimal

def calcularTasa(poblacion, poblacion_nuevo_ingreso):
    if poblacion_nuevo_ingreso > 0:
        tasa_permanencia = Decimal((poblacion*100)/poblacion_nuevo_ingreso)
        tasa_permanencia = round(tasa_permanencia, 2)
    else:
        tasa_permanencia = 0
    return tasa_permanencia

def calcularTipos(nuevo_ingreso, traslado_equivalencia):
    tipos = []
    if nuevo_ingreso:
            tipos.extend(['EX', 'CO'])
    if traslado_equivalencia:
        tipos.extend(['TR', 'EQ'])
    return tipos

def obtenerPoblacionActiva(tipos_ingreso, lista_alumnos, periodo, carrera):
    hombres = Count("alumno__plan__carrera__pk", filter=Q(tipo__in=tipos_ingreso, alumno_id__in=lista_alumnos, periodo=periodo,alumno__plan__carrera__pk=carrera, alumno__curp__genero='H'))
    mujeres = Count("alumno__plan__carrera__pk", filter=Q(tipo__in=tipos_ingreso, alumno_id__in=lista_alumnos, periodo=periodo,alumno__plan__carrera__pk=carrera, alumno__curp__genero='M'))
    activos = Count("alumno__plan__carrera__pk", filter=Q(tipo__in=tipos_ingreso, alumno_id__in=lista_alumnos, periodo=periodo,alumno__plan__carrera__pk=carrera))
    poblacion = Ingreso.objects.aggregate(poblacion=activos, hombres=hombres, mujeres=mujeres)
    return poblacion

def obtenerPoblacionInactiva(lista_alumnos, periodo):
    inactivos = Count("alumno__plan__carrera__pk", filter=Q(alumno_id__in=lista_alumnos, periodo=periodo))
    hombres = Count("alumno__plan__carrera__pk", filter=Q(alumno_id__in=lista_alumnos, periodo=periodo, alumno__curp__genero='H'))
    mujeres = Count("alumno__plan__carrera__pk", filter=Q(alumno_id__in=lista_alumnos, periodo=periodo, alumno__curp__genero='M'))
    poblacion_egr = Egreso.objects.aggregate(egresados=inactivos, hombres=hombres, mujeres=mujeres)
    poblacion_titulo = Titulacion.objects.aggregate(titulados=inactivos, hombres=hombres, mujeres=mujeres)
    poblacion = {'egreso': poblacion_egr,
                 'titulacion': poblacion_titulo
                }
    return poblacion

def obtenerPoblacionEgreso(lista_alumnos, periodo):
    inactivos = Count("alumno__plan__carrera__pk", filter=Q(alumno_id__in=lista_alumnos, periodo=periodo))
    hombres = Count("alumno__plan__carrera__pk", filter=Q(alumno_id__in=lista_alumnos, periodo=periodo, alumno__curp__genero='H'))
    mujeres = Count("alumno__plan__carrera__pk", filter=Q(alumno_id__in=lista_alumnos, periodo=periodo, alumno__curp__genero='M'))
    poblacion_egr = Egreso.objects.aggregate(total=inactivos, hombres=hombres, mujeres=mujeres)

    return poblacion_egr

def obtenerPoblacionTitulada(lista_alumnos, periodo):
    inactivos = Count("alumno__plan__carrera__pk", filter=Q(alumno_id__in=lista_alumnos, periodo=periodo))
    hombres = Count("alumno__plan__carrera__pk", filter=Q(alumno_id__in=lista_alumnos, periodo=periodo, alumno__curp__genero='H'))
    mujeres = Count("alumno__plan__carrera__pk", filter=Q(alumno_id__in=lista_alumnos, periodo=periodo, alumno__curp__genero='M'))
    poblacion_titulacion = Titulacion.objects.aggregate(total=inactivos, hombres=hombres, mujeres=mujeres)

    return poblacion_titulacion

class IndicesPermanencia(APIView):
    """
    Vista para listar la cantidad de alumnos por carrera.

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
        cohorte = request.GET.get('cohorte') if request.GET.get('cohorte') else '20241'
        semestres = request.GET.get('semestres') if request.GET.get('semestres') else '9'
        carrera = request.GET.get('carrera')

        tipos = calcularTipos(nuevo_ingreso, traslado_equivalencia)

        response_data = {}
        periodos = calcularPeriodos(cohorte, int(semestres))
        poblacion_nuevo_ingreso = 0
        alumnos = Ingreso.objects.filter(tipo__in=tipos, periodo=cohorte,alumno__plan__carrera__pk=carrera).annotate(clave=F("alumno_id")
            ).values("clave")
        periodo_anterior=cohorte
        for periodo in periodos:
            if periodo == cohorte:
                poblacion_act = obtenerPoblacionActiva(tipos, alumnos, cohorte, carrera)
                poblacion_nuevo_ingreso = poblacion_act['poblacion']
                alumnos_periodo_anterior = alumnos
                alumnos_periodo = Ingreso.objects.filter(tipo__in=tipos, periodo=periodo, alumno_id__in=alumnos, alumno__plan__carrera__pk=carrera).annotate(clave=F("alumno_id")
                    ).values("clave")
            else:
                poblacion_act = obtenerPoblacionActiva(['RE'], alumnos, periodo, carrera)
                alumnos_periodo = Ingreso.objects.filter(tipo='RE', periodo=periodo, alumno_id__in=alumnos, alumno__plan__carrera__pk=carrera).annotate(clave=F("alumno_id")
                    ).values("clave")
            poblacion_inactiva = obtenerPoblacionInactiva(alumnos, periodo)

            tasa_permanencia = calcularTasa(poblacion_act['poblacion'], poblacion_nuevo_ingreso)
            egresados_periodo = Egreso.objects.filter(periodo=periodo_anterior, alumno_id__in=alumnos).annotate(clave=F("alumno_id")).values("clave")
            desercion = calcularDesercion(alumnos_periodo_anterior, alumnos_periodo, egresados_periodo)
            alumnos_periodo_anterior = alumnos_periodo
            periodo_anterior = periodo
            response_data[periodo] = dict(
                                            hombres=poblacion_act['hombres'],
                                            mujeres=poblacion_act['mujeres'],
                                            hombres_egresados=poblacion_inactiva['egreso']['hombres'],
                                            mujeres_egresadas=poblacion_inactiva['egreso']['mujeres'],
                                            hombres_titulados=poblacion_inactiva['titulacion']['hombres'],
                                            mujeres_tituladas=poblacion_inactiva['titulacion']['mujeres'],
                                            hombres_desertores=desercion['hombres'],
                                            mujeres_desertoras=desercion['mujeres'],
                                            tasa_permanencia=tasa_permanencia
                                        )
        return Response(response_data)

class IndicesEgreso(APIView):
    """
    Vista para listar la cantidad de alumnos por carrera.

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
        cohorte = request.GET.get('cohorte') if request.GET.get('cohorte') else '20241'
        semestres = request.GET.get('semestres') if request.GET.get('semestres') else '9'
        carrera = request.GET.get('carrera')

        tipos = calcularTipos(nuevo_ingreso, traslado_equivalencia)

        response_data = {}
        periodos = calcularPeriodos(cohorte, int(semestres))
        poblacion_nuevo_ingreso = 0
        tasa_egreso = 0
        alumnos = Ingreso.objects.filter(tipo__in=tipos, periodo=cohorte,alumno__plan__carrera__pk=carrera).annotate(clave=F("alumno_id")
            ).values("clave")
        for periodo in periodos:
            if periodo == cohorte:
                poblacion_act = obtenerPoblacionActiva(tipos, alumnos, cohorte, carrera)
                poblacion_nuevo_ingreso = poblacion_act['poblacion']
            else:
                poblacion_act = obtenerPoblacionActiva(['RE'], alumnos, periodo, carrera)
            poblacion_inactiva = obtenerPoblacionInactiva(alumnos, periodo)
            tasa_egreso += calcularTasa(poblacion_inactiva['egreso']['egresados'], poblacion_nuevo_ingreso)
            response_data[periodo] = dict(hombres=poblacion_act['hombres'], mujeres=poblacion_act['mujeres'], hombres_egresados=poblacion_inactiva['egreso']['hombres'], mujeres_egresadas=poblacion_inactiva['egreso']['mujeres'], tasa_egreso=tasa_egreso)
        return Response(response_data)

class IndicesTitulacion(APIView):
    """
    Vista para listar la cantidad de alumnos por carrera.

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
        cohorte = request.GET.get('cohorte') if request.GET.get('cohorte') else '20241'
        semestres = request.GET.get('semestres') if request.GET.get('semestres') else '9'
        carrera = request.GET.get('carrera')

        tipos = calcularTipos(nuevo_ingreso, traslado_equivalencia)

        response_data = {}
        periodos = calcularPeriodos(cohorte, int(semestres))
        poblacion_nuevo_ingreso = 0
        tasa_titulacion = 0
        alumnos = Ingreso.objects.filter(tipo__in=tipos, periodo=cohorte,alumno__plan__carrera__pk=carrera).annotate(clave=F("alumno_id")
            ).values("clave")
        for periodo in periodos:
            if periodo == cohorte:
                poblacion_act = obtenerPoblacionActiva(tipos, alumnos, cohorte, carrera)
                poblacion_nuevo_ingreso = poblacion_act['poblacion']
            else:
                poblacion_act = obtenerPoblacionActiva(['RE'], alumnos, periodo, carrera)
            poblacion_inactiva = obtenerPoblacionInactiva(alumnos, periodo)

            tasa_titulacion += calcularTasa(poblacion_inactiva['titulacion']['titulados'], poblacion_nuevo_ingreso)
            response_data[periodo] = dict(hombres=poblacion_act['hombres'], mujeres=poblacion_act['mujeres'], hombres_egresados=poblacion_inactiva['egreso']['hombres'], mujeres_egresadas=poblacion_inactiva['egreso']['mujeres'], hombres_titulados=poblacion_inactiva['titulacion']['hombres'], mujeres_tituladas=poblacion_inactiva['titulacion']['mujeres'], tasa_titulacion=tasa_titulacion)

        return Response(response_data)

class IndicesDesercion(APIView):
    """
    Vista para listar la cantidad de alumnos por carrera.

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
        cohorte = request.GET.get('cohorte') if request.GET.get('cohorte') else '20241'
        semestres = request.GET.get('semestres') if request.GET.get('semestres') else '9'
        carrera = request.GET.get('carrera')

        tipos = calcularTipos(nuevo_ingreso, traslado_equivalencia)

        response_data = {}
        periodos = calcularPeriodos(cohorte, int(semestres))
        poblacion_nuevo_ingreso = 0
        desercion_total = 0
        alumnos = Ingreso.objects.filter(tipo__in=tipos, periodo=cohorte,alumno__plan__carrera__pk=carrera).annotate(clave=F("alumno_id")
            ).values("clave")
        periodo_anterior = cohorte
        for periodo in periodos:
            if periodo == cohorte:
                poblacion_act = obtenerPoblacionActiva(tipos, alumnos, periodo, carrera)
                poblacion_nuevo_ingreso = poblacion_act['poblacion']
                alumnos_periodo_anterior = alumnos
                alumnos_periodo = Ingreso.objects.filter(tipo__in=tipos, periodo=periodo, alumno_id__in=alumnos, alumno__plan__carrera__pk=carrera).annotate(clave=F("alumno_id")
                    ).values("clave")
            else:
                poblacion_inactiva = obtenerPoblacionInactiva(alumnos, periodo)
                alumnos_periodo = Ingreso.objects.filter(tipo='RE', periodo=periodo, alumno_id__in=alumnos, alumno__plan__carrera__pk=carrera).annotate(clave=F("alumno_id")
                    ).values("clave")
            poblacion_inactiva = obtenerPoblacionInactiva(alumnos, periodo)

            egresados_periodo = Egreso.objects.filter(periodo=periodo_anterior, alumno_id__in=alumnos).annotate(clave=F("alumno_id")).values("clave")
            desercion = calcularDesercion(alumnos_periodo_anterior, alumnos_periodo, egresados_periodo)
            alumnos_periodo_anterior = alumnos_periodo
            desercion_total += desercion['hombres'] + desercion['mujeres']
            tasa_desercion = calcularTasa(desercion_total, poblacion_nuevo_ingreso)

            periodo_anterior = periodo
            response_data[periodo] = dict(hombres=poblacion_act['hombres'], mujeres=poblacion_act['mujeres'], hombres_egresados=poblacion_inactiva['egreso']['hombres'], mujeres_egresadas=poblacion_inactiva['egreso']['mujeres'], hombres_desertores=desercion['hombres'], mujeres_desertoras=desercion['mujeres'], tasa_desercion=tasa_desercion)

        return Response(response_data)

def calcularDesercion(lista_alumnos_periodo_anterior, lista_alumnos_periodo_actual, lista_alumnos_egresados):
    desertores = {'hombres': 0, 'mujeres': 0}
    for alumno in lista_alumnos_periodo_anterior:
        if alumno not in lista_alumnos_periodo_actual:
            if alumno not in lista_alumnos_egresados:
                datos_alumno = Personal.objects.get(alumno__no_control=alumno['clave'])
                if datos_alumno.genero == 'H':
                    desertores['hombres'] += 1
                elif datos_alumno.genero == 'M':
                    desertores['mujeres'] += 1
    return desertores
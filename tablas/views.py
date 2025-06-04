from django.db.models import Count, F, Q
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import permissions

from registros.models import Ingreso
from registros.periodos import calcularPeriodos, getPeriodoActual
from carreras.models import Carrera  # Agregar esta importación al inicio

import logging
logger = logging.getLogger(__name__)

class TablasPoblacion(APIView):
    """
    Vista para listar la cantidad de alumnos por carrera.

    * Requiere autenticación por token.

    ** nuevo-ingreso: Alumnos ingresando en 1er por examen o convalidacion
    ** traslado-equivalencia: Alumnos ingresando de otro TEC u otra escuela
    ** cohorte: El periodo donde empezara el calculo
    ** semestres: Cuantos semestres seran calculados desde el cohorte
    """
    permission_classes = [permissions.IsAuthenticated]

    def get(self, request, format=None):
        try:
            # Convertir explícitamente a booleano
            nuevo_ingreso = request.query_params.get('nuevo-ingreso', '').lower() == 'true'
            traslado_equivalencia = request.query_params.get('traslado-equivalencia', '').lower() == 'true'
            cohorte = request.query_params.get('cohorte') if request.query_params.get('cohorte') else getPeriodoActual()
            semestres = request.query_params.get('semestres') if request.query_params.get('semestres') else '9'

            # Asegurar formato correcto del cohorte
            cohorte = cohorte.replace('-', '')

            # Aplicar filtros según los booleanos
            tipos = []
            if nuevo_ingreso:
                tipos.extend(['EX'])
            if traslado_equivalencia:
                tipos.extend(['TR', 'EQ'])

            # Log de configuración
            logger.info(f"""
                Configuración:
                Nuevo ingreso: {nuevo_ingreso}
                Traslado/Equivalencia: {traslado_equivalencia}
                Tipos seleccionados: {tipos}
                Cohorte: {cohorte}
                Semestres: {semestres}
                ------------------------
            """)

            # Si no hay tipos seleccionados, retornar respuesta vacía
            if not tipos:
                return Response({})

            response_data = {}
            periodos = calcularPeriodos(cohorte, int(semestres))
            
            # Log inicial de períodos
            logger.info(f"""
                Configuración:
                Cohorte original: {cohorte}
                Semestres: {semestres}
                Períodos calculados: {periodos}
                ------------------------
            """)

            # Asegurar formato correcto de todos los periodos
            periodos = [p.replace('-', '') for p in periodos]
            
            # Log después de normalizar
            logger.info(f"""
                Períodos normalizados: {periodos}
                ------------------------
            """)

            for periodo in periodos:
                logger.info(f"Procesando período: {periodo}")
                
                # Obtener todas las carreras primero
                todas_carreras = Carrera.objects.values('pk', 'nombre')
                carreras_dict = {carrera['pk']: {
                    'clave': carrera['pk'],
                    'nombre': carrera['nombre'],
                    'poblacion': 0
                } for carrera in todas_carreras}

                # Obtener poblaciones existentes
                poblacion_qs = Ingreso.objects.filter(
                    tipo__in=tipos, 
                    periodo=periodo
                ).annotate(
                    clave=F("alumno__plan__carrera__pk"),
                    nombre=F("alumno__plan__carrera__nombre")
                ).values("clave", "nombre").annotate(
                    poblacion=Count("alumno_id", distinct=True)
                )

                # Actualizar el diccionario con las poblaciones existentes
                for entry in poblacion_qs:
                    if entry['clave'] in carreras_dict:
                        carreras_dict[entry['clave']]['poblacion'] = entry['poblacion']

                # Convertir el diccionario a lista
                carreras_list = list(carreras_dict.values())
                total = sum(entry['poblacion'] for entry in carreras_list)

                response_data[periodo] = {
                    "total": {"poblacion": total},
                    "carreras": carreras_list
                }

                logger.info(f"""
                    Resultados período {periodo}:
                    Total carreras: {len(carreras_list)}
                    Población total: {total}
                    ------------------------
                """)

            return Response(response_data)
            
        except Exception as e:
            logger.error(f"Error en TablasPoblacion: {str(e)}")
            return Response({'error': str(e)}, status=500)

class TablasCrecimiento(APIView):
    """
    Vista para listar la cantidad de alumnos por carrera.

    * Requiere autenticación por token.

    ** nuevo-ingreso: Alumnos ingresando en 1er por examen o convalidacion
    ** traslado-equivalencia: Alumnos ingresando de otro TEC u otra escuela
    ** cohorte: El periodo donde empezara el calculo
    ** semestres: Cuantos semestres seran calculados desde el cohorte
    """
    permission_classes = [permissions.IsAuthenticated]

    def get(self, request, format=None):
        # Convertir explícitamente a booleano
        nuevo_ingreso = request.query_params.get('nuevo-ingreso', '').lower() == 'true'
        traslado_equivalencia = request.query_params.get('traslado-equivalencia', '').lower() == 'true'
        cohorte = request.query_params.get('cohorte') if request.query_params.get('cohorte') else getPeriodoActual()
        semestres = request.query_params.get('semestres') if request.query_params.get('semestres') else '9'
        carrera = request.query_params.get('carrera') if request.query_params.get('carrera') else 'TODAS'

        # Aplicar filtros según los booleanos
        tipos = []
        if nuevo_ingreso:
            tipos.extend(['EX'])
        if traslado_equivalencia:
            tipos.extend(['TR', 'EQ'])

        # Log de configuración
        logger.info(f"""
            Configuración TablasCrecimiento:
            Nuevo ingreso: {nuevo_ingreso}
            Traslado/Equivalencia: {traslado_equivalencia}
            Tipos seleccionados: {tipos}
            Cohorte: {cohorte}
            Semestres: {semestres}
            Carrera: {carrera}
            ------------------------
        """)

        # Si no hay tipos seleccionados, retornar respuesta vacía
        if not tipos:
            return Response({})

        response_data = {}
        periodos = calcularPeriodos(cohorte, int(semestres))
        for periodo in periodos:
            # SELECT "planes_plan"."carrera_id" AS "clave", "carreras_carrera"."nombre" AS "nombre", COUNT("planes_plan"."carrera_id") AS "poblacion"
            # FROM "registros_ingreso"
            # INNER JOIN "alumnos_alumno" ON ("registros_ingreso"."alumno_id" = "alumnos_alumno"."no_control")
            # INNER JOIN "planes_plan" ON ("alumnos_alumno"."plan_id" = "planes_plan"."clave")
            # INNER JOIN "carreras_carrera" ON ("planes_plan"."carrera_id" = "carreras_carrera"."clave")
            # WHERE ("registros_ingreso"."periodo" = cohorte AND "registros_ingreso"."tipo" IN tipos)
            # GROUP BY "planes_plan"."carrera_id"
            # poblacion_qs = Ingreso.objects.filter(tipo__in=tipos, periodo=periodo).annotate(
            #     clave=F("alumno__plan__carrera__pk"), nombre=F("alumno__plan__carrera__nombre")
            #     ).values("clave", "nombre").annotate(poblacion=Count("alumno_id"))
            # poblacion_list = [entry for entry in poblacion_qs]
            if carrera == 'TODAS':
                activos = Count("alumno_id", filter=Q(tipo__in=tipos, periodo=periodo))
            else:
                activos = Count("alumno__plan__carrera__pk", filter=Q(tipo__in=tipos, periodo=periodo,alumno__plan__carrera__pk=carrera))
            poblacion_act = Ingreso.objects.aggregate(poblacion=activos)
            response_data[periodo] = poblacion_act

        return Response(response_data)
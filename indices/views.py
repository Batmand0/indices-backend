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
import logging

# Función para calcular la tasa de permanencia
def calcularTasa(poblacion, poblacion_nuevo_ingreso):
    if poblacion_nuevo_ingreso > 0:
        tasa_permanencia = Decimal((poblacion*100)/poblacion_nuevo_ingreso)
        tasa_permanencia = round(tasa_permanencia, 2)
    else:
        tasa_permanencia = 0
    return tasa_permanencia

# Función para determinar los tipos de ingreso
def calcularTipos(nuevo_ingreso, traslado_equivalencia):
    tipos = []
    if nuevo_ingreso:
            tipos.extend(['EX', 'CO'])
    if traslado_equivalencia:
        tipos.extend(['TR', 'EQ'])
    return tipos

# Función para obtener la población activa
def obtenerPoblacionActiva(tipos_ingreso, lista_alumnos, periodo, carrera):
    #Esto cuenta la cantidad de hombres en la población activa
    hombres = Count("alumno__plan__carrera__pk", 
                    # Filter sirve para hacer consultas simples
                    # Q sirve para hacer consultas complejas
                    #Filter retorna un QuerySet, 
                    # que es un conjunto de objetos de la base de datos
                    filter=Q(
                        tipo__in=tipos_ingreso, 
                        alumno_id__in=lista_alumnos, 
                        periodo=periodo,
                        alumno__plan__carrera__pk=carrera, 
                        alumno__curp__genero='H')
                    )
    mujeres = Count("alumno__plan__carrera__pk", 
                    filter=Q(
                        tipo__in=tipos_ingreso, 
                        alumno_id__in=lista_alumnos, 
                        periodo=periodo,
                        alumno__plan__carrera__pk=carrera, 
                        alumno__curp__genero='M')
                    )
    activos = Count("alumno__plan__carrera__pk", 
                    filter=Q(
                        tipo__in=tipos_ingreso, 
                        alumno_id__in=lista_alumnos, 
                        periodo=periodo,
                        alumno__plan__carrera__pk=carrera)
                    )
    # poblacion hace referencia a la cantidad de alumnos activos
    poblacion = Ingreso.objects.aggregate(
                    poblacion=activos, 
                    hombres=hombres, 
                    mujeres=mujeres)
    return poblacion

# Función para obtener la población inactiva
def obtenerPoblacionInactiva(lista_alumnos, periodo):
    inactivos = Count(
        "alumno__plan__carrera__pk", 
        filter=Q(alumno_id__in=lista_alumnos, periodo=periodo)
    )
    hombres = Count(
        "alumno__plan__carrera__pk", 
        filter=Q(alumno_id__in=lista_alumnos, periodo=periodo, alumno__curp__genero='H')
    )
    mujeres = Count(
        "alumno__plan__carrera__pk", 
        filter=Q(alumno_id__in=lista_alumnos, periodo=periodo, alumno__curp__genero='M')
    )
    #Esto retorna un diccionario con la cantidad de egresados y titulados
    poblacion_egresada = Egreso.objects.aggregate(egresados=inactivos, hombres=hombres, mujeres=mujeres)
    poblacion_titulo = Titulacion.objects.aggregate(titulados=inactivos, hombres=hombres, mujeres=mujeres)
    poblacion = {'egreso': poblacion_egresada,
                 'titulacion': poblacion_titulo
                }
    return poblacion

# Función para calcular los estudiantes desertores
def obtenerPoblacionEgreso(lista_alumnos, periodo):
    inactivos = Count(
        "alumno__plan__carrera__pk", 
        filter=Q(alumno_id__in=lista_alumnos, periodo=periodo))
    hombres = Count(
        "alumno__plan__carrera__pk", 
        filter=Q(alumno_id__in=lista_alumnos, periodo=periodo, alumno__curp__genero='H'))
    mujeres = Count(
        "alumno__plan__carrera__pk", 
        filter=Q(alumno_id__in=lista_alumnos, periodo=periodo, alumno__curp__genero='M'))
    poblacion_egresada = Egreso.objects.aggregate(total=inactivos, hombres=hombres, mujeres=mujeres)

    return poblacion_egresada

# Función para calcular los estudiantes titulados
def obtenerPoblacionTitulada(lista_alumnos, periodo):
    inactivos = Count(
        "alumno__plan__carrera__pk", 
        filter=Q(alumno_id__in=lista_alumnos, periodo=periodo))
    hombres = Count(
        "alumno__plan__carrera__pk", 
        filter=Q(alumno_id__in=lista_alumnos, periodo=periodo, alumno__curp__genero='H'))
    mujeres = Count(
        "alumno__plan__carrera__pk", 
        filter=Q(alumno_id__in=lista_alumnos, periodo=periodo, alumno__curp__genero='M'))
    poblacion_titulacion = Titulacion.objects.aggregate(total=inactivos, hombres=hombres, mujeres=mujeres)

    return poblacion_titulacion

# APIView para obtener la cantidad de alumnos por carrera
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
    # Se requiere autenticación por token
    permission_classes = [permissions.IsAuthenticated]

    # Método GET para obtener los datos
    def get(self, request, format=None):
        nuevo_ingreso = request.GET.get('nuevo-ingreso')
        traslado_equivalencia = request.GET.get('traslado-equivalencia')
        # Si se obtiene el get de cohorte, se asigna a la variable cohorte, si no, se asigna el valor 20241
        cohorte = request.GET.get('cohorte') if request.GET.get('cohorte') else '20241'
        semestres = request.GET.get('semestres') if request.GET.get('semestres') else '9'
        carrera = request.GET.get('carrera')

        tipos = calcularTipos(nuevo_ingreso, traslado_equivalencia)

        response_data = {}
        periodos = calcularPeriodos(cohorte, int(semestres))
        poblacion_nuevo_ingreso = 0
        # Se obtienen los alumnos que ingresaron en el periodo de cohorte
        alumnos = Ingreso.objects.filter(
            tipo__in=tipos, periodo=cohorte,alumno__plan__carrera__pk=carrera).annotate(
                clave=F("alumno_id")).values("clave")
        periodo_anterior=cohorte
        # Se recorren los periodos
        for periodo in periodos:
            if periodo == cohorte:
                poblacion_act = obtenerPoblacionActiva(tipos, alumnos, cohorte, carrera)
                # Se asigna la población activa del periodo de cohorte a la variable poblacion_nuevo_ingreso
                poblacion_nuevo_ingreso = poblacion_act['poblacion']
                alumnos_periodo_anterior = alumnos
                alumnos_periodo = Ingreso.objects.filter(
                    tipo__in=tipos, 
                    periodo=periodo, 
                    alumno_id__in=alumnos, 
                    alumno__plan__carrera__pk=carrera
                ).annotate(clave=F("alumno_id")).values("clave")
            else:
                # Se obtiene la población activa del periodo actual
                poblacion_act = obtenerPoblacionActiva(['RE'], alumnos, periodo, carrera)
                alumnos_periodo = Ingreso.objects.filter(
                    tipo='RE', 
                    periodo=periodo, 
                    alumno_id__in=alumnos, 
                    alumno__plan__carrera__pk=carrera
                ).annotate(clave=F("alumno_id")).values("clave")
            
            # Se obtiene la población inactiva del periodo actual
            poblacion_inactiva = obtenerPoblacionInactiva(alumnos, periodo)

            # Se calcula la tasa de permanencia
            tasa_permanencia = calcularTasa(poblacion_act['poblacion'], poblacion_nuevo_ingreso)
            egresados_periodo = Egreso.objects.filter(periodo=periodo_anterior, alumno_id__in=alumnos).annotate(clave=F("alumno_id")).values("clave")
            desercion = calcularDesercion(alumnos_periodo_anterior, alumnos_periodo, egresados_periodo)
            alumnos_periodo_anterior = alumnos_periodo
            periodo_anterior = periodo
            # Se asignan los datos al diccionario de respuesta
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

# APIView para obtener la cantidad de alumnos por carrera
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

    # Método GET para obtener los datos
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
        # Se recorren los periodos
        for periodo in periodos:
            if periodo == cohorte:
                poblacion_act = obtenerPoblacionActiva(tipos, alumnos, cohorte, carrera)
                poblacion_nuevo_ingreso = poblacion_act['poblacion']
            else:
                poblacion_act = obtenerPoblacionActiva(['RE'], alumnos, periodo, carrera)
            # Se obtiene la población inactiva del periodo actual
            poblacion_inactiva = obtenerPoblacionInactiva(alumnos, periodo)
            # Se calcula la tasa de egreso
            tasa_egreso += calcularTasa(poblacion_inactiva['egreso']['egresados'], poblacion_nuevo_ingreso)
            # Se asignan los datos al diccionario de respuesta
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

            # Se calcula la tasa de titulación, se obtiene de la población inactiva
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
    
class IndicesGeneracionalDesercion(APIView):
    """
    Vista para listar los índices de deserción por generación.

    * Requiere autenticación por token.
    """
    permission_classes = [permissions.IsAuthenticated]

    def get_generaciones(self, cohorte, num_generaciones=9):
        """
        Obtiene las generaciones a analizar, una por cada semestre
        Ejemplo: [20241, 20243, 20251, 20253, 20261, 20263, 20271, 20273, 20281]
        """
        cohorte_actual = int(cohorte)
        año_base = cohorte_actual // 10
        semestre_actual = cohorte_actual % 10
        generaciones = []
        
        for i in range(num_generaciones):
            año = año_base + (i // 2)  # Incrementa año cada 2 iteraciones
            semestre = 1 if i % 2 == 0 else 3  # Alterna entre 1 y 3
            generacion = str(año * 10 + semestre)
            generaciones.append(generacion)
        
        return generaciones

    def get_poblacion_data(self, tipos, generacion, carrera, periodos):
        """Obtiene datos de población para una generación usando lógica acumulativa"""
        alumnos = Ingreso.objects.filter(
            tipo__in=tipos,
            periodo=generacion,
            alumno__plan__carrera__pk=carrera
        ).annotate(
            clave=F("alumno_id")
        ).values("clave")

        # Población inicial
        poblacion_inicial = obtenerPoblacionActiva(tipos, alumnos, generacion, carrera)
        total_inicial = poblacion_inicial['poblacion']

        # Calcular deserción acumulada
        desercion_total = 0
        alumnos_periodo_anterior = alumnos
        periodo_anterior = generacion
        ultimo_periodo = periodos[-1]  # Obtener el último periodo

        for periodo in periodos:
            if periodo == generacion:
                alumnos_periodo = Ingreso.objects.filter(
                    tipo__in=tipos,
                    periodo=periodo,
                    alumno_id__in=alumnos,
                    alumno__plan__carrera__pk=carrera
                ).annotate(
                    clave=F("alumno_id")
                ).values("clave")
            else:
                alumnos_periodo = Ingreso.objects.filter(
                    tipo='RE',
                    periodo=periodo,
                    alumno_id__in=alumnos,
                    alumno__plan__carrera__pk=carrera
                ).annotate(
                    clave=F("alumno_id")
                ).values("clave")

            egresados_periodo = Egreso.objects.filter(
                periodo=periodo_anterior,
                alumno_id__in=alumnos
            ).annotate(
                clave=F("alumno_id")
            ).values("clave")

            desercion = calcularDesercion(
                alumnos_periodo_anterior,
                alumnos_periodo,
                egresados_periodo
            )
            
            desercion_total += desercion['hombres'] + desercion['mujeres']
            print(f"Periodo {periodo}: deserción={desercion['hombres'] + desercion['mujeres']}, acumulada={desercion_total}")
            alumnos_periodo_anterior = alumnos_periodo
            periodo_anterior = periodo

        # Obtener población actual del último periodo
        poblacion_actual = obtenerPoblacionActiva(['RE'], alumnos, ultimo_periodo, carrera)
        total_actual = poblacion_actual['poblacion']

        return total_inicial, desercion_total, total_actual

    def get(self, request, format=None):
        try:
            # Validación de parámetros
            cohorte = request.GET.get('cohorte', '20241')
            num_semestres = int(request.GET.get('semestres', '9'))
            carrera = request.GET.get('carrera')
            
            if not carrera:
                return Response(
                    {'error': 'Carrera es requerida'}, 
                    status=400
                )

            # Calcular tipos de ingreso
            tipos = calcularTipos(
                request.GET.get('nuevo-ingreso'),
                request.GET.get('traslado-equivalencia')
            )

            # Procesar generaciones
            response_data = {}
            generaciones = self.get_generaciones(cohorte, num_semestres)

            for gen in generaciones:
                # Calcular periodos asegurando 9 semestres completos
                periodos = calcularPeriodos(gen, 9)  # Forzar a 9 semestres
                print(f"Generación {gen} tiene periodos: {periodos}")   
                
                ultimo_periodo = periodos[8]  # Índice 8 para el 9no semestre

                total_inicial, desercion_total, total_actual = self.get_poblacion_data(
                    tipos, 
                    gen, 
                    carrera, 
                    periodos  # Pasar todos los periodos
                )

                tasa_desercion = calcularTasa(desercion_total, total_inicial)
                
                # Agregar esta parte que falta
                response_data[gen] = {
                    'total_inicial': total_inicial,
                    'total_actual': total_actual,
                    'desercion_total': desercion_total,
                    'tasa_desercion': tasa_desercion,
                    'ultimo_periodo': ultimo_periodo
                }
                

            return Response(response_data)

        except Exception as ex:
            logger.error(f"Error en índices generacionales: {str(ex)}")
            return Response(
                {'error': str(ex)}, 
                status=500
            )
        
class IndicesGeneracionalPermanencia(APIView):
    """
    Vista para listar los índices de permanencia por generación.

    * Requiere autenticación por token.
    """
    permission_classes = [permissions.IsAuthenticated]

    def get_generaciones(self, cohorte, num_generaciones=9):
        """
        Obtiene las generaciones a analizar, una por cada semestre
        Ejemplo: [20241, 20243, 20251, 20253, 20261, 20263, 20271, 20273, 20281]
        """
        cohorte_actual = int(cohorte)
        año_base = cohorte_actual // 10
        semestre_actual = cohorte_actual % 10
        generaciones = []
        
        for i in range(num_generaciones):
            año = año_base + (i // 2)  # Incrementa año cada 2 iteraciones
            semestre = 1 if i % 2 == 0 else 3  # Alterna entre 1 y 3
            generacion = str(año * 10 + semestre)
            generaciones.append(generacion)
        
        return generaciones

    def get_poblacion_data(self, tipos, generacion, carrera, periodos):
        """Obtiene datos de población para una generación usando lógica acumulativa"""
        alumnos = Ingreso.objects.filter(
            tipo__in=tipos,
            periodo=generacion,
            alumno__plan__carrera__pk=carrera
        ).annotate(
            clave=F("alumno_id")
        ).values("clave")

        # Población inicial
        poblacion_inicial = obtenerPoblacionActiva(tipos, alumnos, generacion, carrera)
        total_inicial = poblacion_inicial['poblacion']

        alumnos_periodo_anterior = alumnos
        periodo_anterior = generacion
        ultimo_periodo = periodos[-1]  # Obtener el último periodo

        for periodo in periodos:
            if periodo == generacion:
                alumnos_periodo = Ingreso.objects.filter(
                    tipo__in=tipos,
                    periodo=periodo,
                    alumno_id__in=alumnos,
                    alumno__plan__carrera__pk=carrera
                ).annotate(
                    clave=F("alumno_id")
                ).values("clave")
            else:
                alumnos_periodo = Ingreso.objects.filter(
                    tipo='RE',
                    periodo=periodo,
                    alumno_id__in=alumnos,
                    alumno__plan__carrera__pk=carrera
                ).annotate(
                    clave=F("alumno_id")
                ).values("clave")

            alumnos_periodo_anterior = alumnos_periodo
            periodo_anterior = periodo

        # Obtener población actual del último periodo
        poblacion_actual = obtenerPoblacionActiva(['RE'], alumnos, ultimo_periodo, carrera)
        total_actual = poblacion_actual['poblacion']

        return total_inicial, total_actual

    def get(self, request, format=None):
        try:
            # Validación de parámetros
            cohorte = request.GET.get('cohorte', '20241')
            num_semestres = int(request.GET.get('semestres', '9'))
            carrera = request.GET.get('carrera')
            
            if not carrera:
                return Response(
                    {'error': 'Carrera es requerida'}, 
                    status=400
                )

            # Calcular tipos de ingreso
            tipos = calcularTipos(
                request.GET.get('nuevo-ingreso'),
                request.GET.get('traslado-equivalencia')
            )

            # Procesar generaciones
            response_data = {}
            generaciones = self.get_generaciones(cohorte, num_semestres)

            for gen in generaciones:
                # Calcular periodos asegurando 9 semestres completos
                periodos = calcularPeriodos(gen, 9)  # Forzar a 9 semestres
                print(f"Generación {gen} tiene periodos: {periodos}")   
                
                total_inicial, total_actual = self.get_poblacion_data(
                    tipos, 
                    gen, 
                    carrera, 
                    periodos  # Pasar todos los periodos
                )

                tasa_permanencia = calcularTasa(total_actual, total_inicial)
                
                response_data[gen] = {
                    'total_inicial': total_inicial,
                    'total_actual': total_actual,
                    'tasa_permanencia': tasa_permanencia
                }

            return Response(response_data)

        except Exception as ex:
            logger.error(f"Error en índices generacionales: {str(ex)}")
            return Response(
                {'error': str(ex)}, 
                status=500
            )

class IndicesGeneracionalEgreso(APIView):
    """
    Vista para listar los índices de egreso por generación.

    * Requiere autenticación por token.
    """
    permission_classes = [permissions.IsAuthenticated]

    def get_generaciones(self, cohorte, num_generaciones=9):
        """
        Obtiene las generaciones a analizar, una por cada semestre
        Ejemplo: [20241, 20243, 20251, 20253, 20261, 20263, 20271, 20273, 20281]
        """
        cohorte_actual = int(cohorte)
        año_base = cohorte_actual // 10
        semestre_actual = cohorte_actual % 10
        generaciones = []
        
        for i in range(num_generaciones):
            año = año_base + (i // 2)  # Incrementa año cada 2 iteraciones
            semestre = 1 if i % 2 == 0 else 3  # Alterna entre 1 y 3
            generacion = str(año * 10 + semestre)
            generaciones.append(generacion)
        
        return generaciones

    def get_poblacion_data(self, tipos, generacion, carrera, periodos):
        """Obtiene datos de población para una generación usando lógica acumulativa"""
        alumnos = Ingreso.objects.filter(
            tipo__in=tipos,
            periodo=generacion,
            alumno__plan__carrera__pk=carrera
        ).annotate(
            clave=F("alumno_id")
        ).values("clave")

        # Población inicial
        poblacion_inicial = obtenerPoblacionActiva(tipos, alumnos, generacion, carrera)
        total_inicial = poblacion_inicial['poblacion']

        alumnos_periodo_anterior = alumnos
        periodo_anterior = generacion
        ultimo_periodo = periodos[-1]  # Obtener el último periodo

        for periodo in periodos:
            if periodo == generacion:
                alumnos_periodo = Ingreso.objects.filter(
                    tipo__in=tipos,
                    periodo=periodo,
                    alumno_id__in=alumnos,
                    alumno__plan__carrera__pk=carrera
                ).annotate(
                    clave=F("alumno_id")
                ).values("clave")
            else:
                alumnos_periodo = Ingreso.objects.filter(
                    tipo='RE',
                    periodo=periodo,
                    alumno_id__in=alumnos,
                    alumno__plan__carrera__pk=carrera
                ).annotate(
                    clave=F("alumno_id")
                ).values("clave")

            alumnos_periodo_anterior = alumnos_periodo
            periodo_anterior = periodo

        # Obtener población de egresados del último periodo
        poblacion_egresada = obtenerPoblacionEgreso(alumnos, ultimo_periodo)
        total_egresados = poblacion_egresada['total']

        return total_inicial, total_egresados

    def get(self, request, format=None):
        try:
            # Validación de parámetros
            cohorte = request.GET.get('cohorte', '20241')
            num_semestres = int(request.GET.get('semestres', '9'))
            carrera = request.GET.get('carrera')
            
            if not carrera:
                return Response(
                    {'error': 'Carrera es requerida'}, 
                    status=400
                )

            # Calcular tipos de ingreso
            tipos = calcularTipos(
                request.GET.get('nuevo-ingreso'),
                request.GET.get('traslado-equivalencia')
            )

            # Procesar generaciones
            response_data = {}
            generaciones = self.get_generaciones(cohorte, num_semestres)

            for gen in generaciones:
                # Calcular periodos asegurando 9 semestres completos
                periodos = calcularPeriodos(gen, 9)  # Forzar a 9 semestres
                print(f"Generación {gen} tiene periodos: {periodos}")   
                
                total_inicial, total_egresados = self.get_poblacion_data(
                    tipos, 
                    gen, 
                    carrera, 
                    periodos  # Pasar todos los periodos
                )

                tasa_egreso = calcularTasa(total_egresados, total_inicial)
                
                response_data[gen] = {
                    'total_inicial': total_inicial,
                    'total_egresados': total_egresados,
                    'tasa_egreso': tasa_egreso
                }

            return Response(response_data)

        except Exception as ex:
            logger.error(f"Error en índices generacionales: {str(ex)}")
            return Response(
                {'error': str(ex)}, 
                status=500
            )

# Función para calcular los desertores
def calcularDesercion(lista_alumnos_periodo_anterior, lista_alumnos_periodo_actual, lista_alumnos_egresados):
    desertores = {'hombres': 0, 'mujeres': 0}
    # Se recorren los alumnos del periodo anterior
    for alumno in lista_alumnos_periodo_anterior:
        # Si el alumno no se encuentra en la lista de alumnos del periodo actual
        if alumno not in lista_alumnos_periodo_actual:
            # Si el alumno no se encuentra en la lista de alumnos egresados
            if alumno not in lista_alumnos_egresados:
                datos_alumno = Personal.objects.get(alumno__no_control=alumno['clave'])
                if datos_alumno.genero == 'H':
                    desertores['hombres'] += 1
                elif datos_alumno.genero == 'M':
                    desertores['mujeres'] += 1
    return desertores
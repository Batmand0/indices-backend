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

# Configurar el logger
logging.basicConfig(
    level=logging.INFO,  # Nivel de log (DEBUG, INFO, WARNING, ERROR, CRITICAL)
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',  # Formato del mensaje de log
    handlers=[
        logging.StreamHandler()  # Enviar los mensajes de log a la consola
    ]
)

# Configurar el logger
logger = logging.getLogger(__name__)

# Función para calcular la tasa de permanencia
def calcularTasa(poblacion, poblacion_nuevo_ingreso):
    if poblacion_nuevo_ingreso > 0:
        tasa_permanencia = Decimal((poblacion*100)/poblacion_nuevo_ingreso)
        tasa_permanencia = round(tasa_permanencia, 1)
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

def obtenerEgresadosAcumulados(alumnos, periodos, periodo_actual=None):
    """
    Obtiene total de egresados en un rango de periodos hasta el periodo anterior al actual
    Args:
        alumnos: QuerySet con los alumnos a considerar
        periodos: Lista de periodos a considerar
        periodo_actual: Periodo actual hasta donde NO contar (exclusive)
    """
    if periodo_actual:
        # Filtrar periodos hasta el anterior al actual
        periodos_validos = [p for p in periodos if p < periodo_actual]
    else:
        periodos_validos = periodos

    acumulados = Egreso.objects.filter(
        alumno_id__in=alumnos.values('clave'),
        periodo__in=periodos_validos
    ).aggregate(
        total=Count('pk'),
        hombres=Count('pk', filter=Q(alumno__curp__genero='H')),
        mujeres=Count('pk', filter=Q(alumno__curp__genero='M'))
    )

    return acumulados

from abc import ABC, abstractmethod

class IndicesBase(APIView, ABC):
    """Clase base abstracta para todos los índices"""
    permission_classes = [permissions.IsAuthenticated]

    def get_params(self, request):
        """Obtiene y valida parámetros comunes"""
        
        return {
            'nuevo_ingreso': request.GET.get('nuevo-ingreso'),
            'traslado_equivalencia': request.GET.get('traslado-equivalencia'),
            'cohorte': request.GET.get('cohorte', '20241'),
            'semestres': request.GET.get('semestres', '9'),
            'carrera': request.GET.get('carrera')
        }
    
    
    def get_base_data(self, tipos, cohorte, periodos, carrera):
        """Obtiene datos base comunes"""
        temp_data = {}
        poblacion_nuevo_ingreso = 0
        
        # Obtener alumnos iniciales
        alumnos = Ingreso.objects.filter(
            tipo__in=tipos,
            periodo=cohorte,
            alumno__plan__carrera__pk=carrera
        ).annotate(
            clave=F("alumno_id")
        ).values("clave")

        alumnos_periodo_anterior = alumnos
        periodo_anterior = cohorte

        # Recolección de datos por periodo
        for periodo in periodos:
            # Obtener población activa
            if periodo == cohorte:
                poblacion_act = obtenerPoblacionActiva(tipos, alumnos, periodo, carrera)
                poblacion_nuevo_ingreso = poblacion_act['poblacion']
                alumnos_periodo = Ingreso.objects.filter(
                    tipo__in=tipos,
                    periodo=periodo,
                    alumno_id__in=alumnos,
                    alumno__plan__carrera__pk=carrera
                ).annotate(
                    clave=F("alumno_id")
                ).values("clave")
            else:
                poblacion_act = obtenerPoblacionActiva(['RE'], alumnos, periodo, carrera)
                alumnos_periodo = Ingreso.objects.filter(
                    tipo='RE',
                    periodo=periodo,
                    alumno_id__in=alumnos,
                    alumno__plan__carrera__pk=carrera
                ).annotate(
                    clave=F("alumno_id")
                ).values("clave")
            
            # Obtener datos de inactivos
            poblacion_inactiva = obtenerPoblacionInactiva(alumnos, periodo)
            
            # Calcular deserción
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
            
            # Guardar datos en temp_data
            temp_data[periodo] = {
                'hombres': poblacion_act['hombres'],
                'mujeres': poblacion_act['mujeres'],
                'hombres_egresados': poblacion_inactiva['egreso']['hombres'],
                'mujeres_egresadas': poblacion_inactiva['egreso']['mujeres'],
                'hombres_titulados': poblacion_inactiva['titulacion']['hombres'],
                'mujeres_tituladas': poblacion_inactiva['titulacion']['mujeres'],
                'hombres_desertores': desercion['hombres'],
                'mujeres_desertoras': desercion['mujeres']
            }

            # Actualizar valores para siguiente iteración
            alumnos_periodo_anterior = alumnos_periodo
            periodo_anterior = periodo

        return {
            'temp_data': temp_data,
            'poblacion_nuevo_ingreso': poblacion_nuevo_ingreso,
            'alumnos': alumnos
        }

    def get_base_data_global(self, tipos, cohorte, periodos):
        """Obtiene datos base para todas las carreras combinadas"""
        temp_data = {}
        
        # Obtener todos los alumnos del cohorte sin filtrar por carrera
        alumnos = Ingreso.objects.filter(
            tipo__in=tipos,
            periodo=cohorte
        ).annotate(
            clave=F("alumno_id")
        ).values("clave")

        # Obtener población inicial total
        poblacion_inicial = Ingreso.objects.filter(
            tipo__in=tipos,
            periodo=cohorte
        ).aggregate(
            poblacion=Count('alumno_id', distinct=True),
            hombres=Count('alumno_id', distinct=True, 
                         filter=Q(alumno__curp__genero='H')),
            mujeres=Count('alumno_id', distinct=True, 
                         filter=Q(alumno__curp__genero='M'))
        )

        alumnos_periodo_anterior = alumnos
        periodo_anterior = cohorte

        # Recolección de datos por periodo
        for periodo in periodos:
            if periodo == cohorte:
                # Para el periodo inicial
                poblacion_act = poblacion_inicial
                alumnos_periodo = alumnos
            else:
                # Para periodos posteriores
                poblacion_act = Ingreso.objects.filter(
                    tipo='RE',
                    periodo=periodo,
                    alumno_id__in=alumnos.values('clave')
                ).aggregate(
                    poblacion=Count('alumno_id', distinct=True),
                    hombres=Count('alumno_id', distinct=True, 
                                 filter=Q(alumno__curp__genero='H')),
                    mujeres=Count('alumno_id', distinct=True, 
                                 filter=Q(alumno__curp__genero='M'))
                )
                alumnos_periodo = Ingreso.objects.filter(
                    tipo='RE',
                    periodo=periodo,
                    alumno_id__in=alumnos.values('clave')
                ).values('clave')

            # Obtener datos de inactivos globales
            poblacion_inactiva = obtenerPoblacionInactiva(alumnos, periodo)
            
            # Calcular deserción global
            egresados_periodo = Egreso.objects.filter(
                periodo=periodo_anterior,
                alumno_id__in=alumnos
            ).values('clave')

            desercion = calcularDesercion(
                alumnos_periodo_anterior,
                alumnos_periodo,
                egresados_periodo
            )

            # Guardar datos en temp_data
            temp_data[periodo] = {
                'hombres': poblacion_act['hombres'],
                'mujeres': poblacion_act['mujeres'],
                'hombres_egresados': poblacion_inactiva['egreso']['hombres'],
                'mujeres_egresadas': poblacion_inactiva['egreso']['mujeres'],
                'hombres_titulados': poblacion_inactiva['titulacion']['hombres'],
                'mujeres_tituladas': poblacion_inactiva['titulacion']['mujeres'],
                'hombres_desertores': desercion['hombres'],
                'mujeres_desertoras': desercion['mujeres']
            }

            alumnos_periodo_anterior = alumnos_periodo
            periodo_anterior = periodo

        return {
            'temp_data': temp_data,
            'poblacion_nuevo_ingreso': poblacion_inicial['poblacion'],
            'alumnos': alumnos
        }

    @abstractmethod
    def process_response(self, base_data, periodos):
        """Cada subclase implementa su procesamiento específico"""
        pass

    @abstractmethod
    def calculate_rate(self, *args, **kwargs):
        """Cada subclase implementa su cálculo de tasa"""
        pass

# APIView para obtener la cantidad de alumnos por carrera
class IndicesPermanencia(IndicesBase):
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
        try:
            # Obtener parámetros usando método de clase base
            params = self.get_params(request)
            logger.info(f"Parámetros recibidos: {params}")
            tipos = calcularTipos(params['nuevo_ingreso'], params['traslado_equivalencia'])
            periodos = calcularPeriodos(params['cohorte'], int(params['semestres']) + 1)
            
            # Determinar si es consulta global o por carrera
            if params['carrera'] == 'TODAS':
                base_data = self.get_base_data_global(tipos, params['cohorte'], periodos)
            else:
                base_data = self.get_base_data(tipos, params['cohorte'], periodos, params['carrera'])
            
            # Obtener datos base
            response_data = self.process_response(base_data, periodos)
            
            return Response(response_data)
            
        except Exception as ex:
            logger.error(f"Error en índices permanencia: {str(ex)}")
            return Response({'error': str(ex)}, status=500)

    def process_response(self, base_data, periodos):
        """Procesa response con tasas de permanencia"""
        response_data = {}
        temp_data = base_data['temp_data']
        poblacion_nuevo_ingreso = base_data['poblacion_nuevo_ingreso']
        egresados_acumuladosHombres = 0
        egresados_acumuladosMujeres = 0
        egresados_acumulados = 0

        for i in range(len(periodos) - 1):
            periodo_actual = periodos[i]
            periodo_siguiente = periodos[i + 1]
            
            if periodo_siguiente in temp_data:
                response_data[periodo_actual] = temp_data[periodo_actual].copy()
                
                # Obtener activos del periodo actual
                activosHombres = temp_data[periodo_actual]['hombres']
                activosMujeres = temp_data[periodo_actual]['mujeres']
                activosGeneral = activosHombres + activosMujeres
                
                # Obtener desertores del periodo siguiente
                desertoresHombres = temp_data[periodo_siguiente]['hombres_desertores']
                desertoresMujeres = temp_data[periodo_siguiente]['mujeres_desertoras']
                desertoresGeneral = desertoresHombres + desertoresMujeres
                
                # Sobrescribir datos de deserción
                response_data[periodo_actual]['hombres_desertores'] = temp_data[periodo_siguiente]['hombres_desertores']
                response_data[periodo_actual]['mujeres_desertoras'] = temp_data[periodo_siguiente]['mujeres_desertoras']
                
                # Calcular tasa con egresados acumulados hasta el periodo anterior
                tasaHombres = self.calculate_rate(activosHombres, egresados_acumuladosHombres, desertoresHombres, poblacion_nuevo_ingreso)
                tasaMujeres = self.calculate_rate(activosMujeres, egresados_acumuladosMujeres, desertoresMujeres, poblacion_nuevo_ingreso)
                tasaGeneral = self.calculate_rate(activosGeneral, egresados_acumulados, desertoresGeneral, poblacion_nuevo_ingreso)
                response_data[periodo_actual]['tasa_permanencia_Hombres'] = tasaHombres
                response_data[periodo_actual]['tasa_permanencia_Mujeres'] = tasaMujeres
                response_data[periodo_actual]['tasa_permanencia'] = tasaGeneral

                # Actualizar egresados acumulados después de calcular la tasa
                egresados_acumuladosHombres += (temp_data[periodo_actual]['hombres_egresados'])
                egresados_acumuladosMujeres += (temp_data[periodo_actual]['mujeres_egresadas'])
                egresados_acumulados = egresados_acumuladosHombres + egresados_acumuladosMujeres

        logger.info(f"response_data: {response_data}")
        return response_data

    def calculate_rate(self, activos, egresados, desertores, poblacion_nuevo_ingreso):
        """Calcula tasa de permanencia"""
        activos_menos_bajas = activos + egresados - desertores
        return calcularTasa(activos_menos_bajas, poblacion_nuevo_ingreso)

# APIView para obtener la cantidad de alumnos por carrera
class IndicesEgreso(IndicesBase):
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
        try:
            params = self.get_params(request)
            tipos = calcularTipos(params['nuevo_ingreso'], params['traslado_equivalencia'])
            periodos = calcularPeriodos(params['cohorte'], int(params['semestres']))
            base_data = self.get_base_data(tipos, params['cohorte'], periodos, params['carrera'])
            response_data = self.process_response(base_data, periodos)
            return Response(response_data)
        except Exception as ex:
            logger.error(f"Error en índices generacionales: {str(ex)}")
            return Response({'error': str(ex)}, status=500)

    def process_response(self, base_data, periodos):
        temp_data = base_data['temp_data']
        poblacion_nuevo_ingreso = base_data['poblacion_nuevo_ingreso']
        response_data = {}

        # Obtener población de nuevo ingreso por sexo
        hombres_nuevo_ingreso = 0
        mujeres_nuevo_ingreso = 0
        if periodos:
            primer_periodo = periodos[0]
            if primer_periodo in temp_data:
                hombres_nuevo_ingreso = temp_data[primer_periodo]['hombres']
                mujeres_nuevo_ingreso = temp_data[primer_periodo]['mujeres']

        tasa_egreso = 0
        tasa_egreso_hombres = 0
        tasa_egreso_mujeres = 0

        for periodo in periodos:
            poblacion_inactiva = obtenerPoblacionInactiva(base_data['alumnos'], periodo)
            egresados_total = poblacion_inactiva['egreso']['egresados']
            egresados_hombres = poblacion_inactiva['egreso']['hombres']
            egresados_mujeres = poblacion_inactiva['egreso']['mujeres']

            tasa_egreso += self.calculate_rate(egresados_total, poblacion_nuevo_ingreso)
            tasa_egreso_hombres += self.calculate_rate(egresados_hombres, poblacion_nuevo_ingreso)
            tasa_egreso_mujeres += self.calculate_rate(egresados_mujeres, poblacion_nuevo_ingreso)

            response_data[periodo] = dict(
                hombres=temp_data[periodo]['hombres'], 
                mujeres=temp_data[periodo]['mujeres'], 
                hombres_egresados=temp_data[periodo]['hombres_egresados'], 
                mujeres_egresadas=temp_data[periodo]['mujeres_egresadas'], 
                tasa_egreso=tasa_egreso,
                tasa_egreso_hombres=tasa_egreso_hombres,
                tasa_egreso_mujeres=tasa_egreso_mujeres
            )

        return response_data

    def calculate_rate(self, egresados, poblacion_nuevo_ingreso):
        return calcularTasa(egresados, poblacion_nuevo_ingreso)
    
class IndicesTitulacion(IndicesBase):
    """
    Vista para listar la cantidad de alumnos por carrera.
    * Requiere autenticación por token.
    """
    permission_classes = [permissions.IsAuthenticated]

    def get(self, request, format=None):
        try:
            params = self.get_params(request)
            tipos = calcularTipos(params['nuevo_ingreso'], params['traslado_equivalencia'])
            periodos = calcularPeriodos(params['cohorte'], int(params['semestres']))
            base_data = self.get_base_data(tipos, params['cohorte'], periodos, params['carrera'])
            response_data = self.process_response(base_data, periodos)
            return Response(response_data)
        except Exception as ex:
            logger.error(f"Error en índices generacionales: {str(ex)}")
            return Response({'error': str(ex)}, status=500)

    def process_response(self, base_data, periodos):
        temp_data = base_data['temp_data']
        poblacion_nuevo_ingreso = base_data['poblacion_nuevo_ingreso']
        response_data = {}

        # Obtener población de nuevo ingreso por sexo
        hombres_nuevo_ingreso = 0
        mujeres_nuevo_ingreso = 0
        if periodos:
            primer_periodo = periodos[0]
            if primer_periodo in temp_data:
                hombres_nuevo_ingreso = temp_data[primer_periodo]['hombres']
                mujeres_nuevo_ingreso = temp_data[primer_periodo]['mujeres']

        tasa_titulacion = 0
        tasa_titulacion_hombres = 0
        tasa_titulacion_mujeres = 0

        for periodo in periodos:
            poblacion_inactiva = obtenerPoblacionInactiva(base_data['alumnos'], periodo)
            titulados_total = poblacion_inactiva['titulacion']['titulados']
            titulados_hombres = poblacion_inactiva['titulacion']['hombres']
            titulados_mujeres = poblacion_inactiva['titulacion']['mujeres']

            tasa_titulacion += self.calculate_rate(titulados_total, poblacion_nuevo_ingreso)
            tasa_titulacion_hombres += self.calculate_rate(titulados_hombres, poblacion_nuevo_ingreso)
            tasa_titulacion_mujeres += self.calculate_rate(titulados_mujeres, poblacion_nuevo_ingreso)

            response_data[periodo] = dict(
                hombres=temp_data[periodo]['hombres'],
                mujeres=temp_data[periodo]['mujeres'],
                hombres_egresados=temp_data[periodo]['hombres_egresados'],
                mujeres_egresadas=temp_data[periodo]['mujeres_egresadas'],
                hombres_titulados=temp_data[periodo]['hombres_titulados'],
                mujeres_tituladas=temp_data[periodo]['mujeres_tituladas'],
                tasa_titulacion=tasa_titulacion,
                tasa_titulacion_hombres=tasa_titulacion_hombres,
                tasa_titulacion_mujeres=tasa_titulacion_mujeres
            )

        return response_data

    def calculate_rate(self, titulados, poblacion_nuevo_ingreso):
        return calcularTasa(titulados, poblacion_nuevo_ingreso)

class IndicesDesercion(IndicesBase):
    """
    Vista para listar los índices de deserción.
    * Requiere autenticación por token.
    """
    def get(self, request, format=None):
        try:
            # Obtener parámetros usando método de clase base
            params = self.get_params(request)
            tipos = calcularTipos(params['nuevo_ingreso'], params['traslado_equivalencia'])
            periodos = calcularPeriodos(params['cohorte'], int(params['semestres']) + 1)
            
            # Obtener datos base
            base_data = self.get_base_data(tipos, params['cohorte'], periodos, params['carrera'])
            
            # Procesar respuesta
            response_data = self.process_response(base_data, periodos)
            
            return Response(response_data)
            
        except Exception as ex:
            logger.error(f"Error en índices deserción: {str(ex)}")
            return Response({'error': str(ex)}, status=500)

    def process_response(self, base_data, periodos):
        """Procesa response con tasas de deserción"""
        response_data = {}
        temp_data = base_data['temp_data']
        poblacion_nuevo_ingreso = base_data['poblacion_nuevo_ingreso']
        desercion_acumulada = 0
        desercion_acumulada_Hombres = 0
        desercion_acumulada_Mujeres = 0

        for i in range(len(periodos) - 1):
            periodo_actual = periodos[i]
            periodo_siguiente = periodos[i + 1]
            
            if periodo_siguiente in temp_data:
                # Copiar datos base del periodo actual
                response_data[periodo_actual] = temp_data[periodo_actual].copy()
                
                # Sobrescribir datos de deserción con los del periodo siguiente
                response_data[periodo_actual]['hombres_desertores'] = temp_data[periodo_siguiente]['hombres_desertores']
                response_data[periodo_actual]['mujeres_desertoras'] = temp_data[periodo_siguiente]['mujeres_desertoras']
                desertores_Hombres = temp_data[periodo_siguiente]['hombres_desertores']
                desertores_Mujeres = temp_data[periodo_siguiente]['mujeres_desertoras']
                
                # Calcular deserción del periodo
                desertores = (temp_data[periodo_siguiente]['hombres_desertores'] + 
                            temp_data[periodo_siguiente]['mujeres_desertoras'])
                
                # Actualizar desercion acumulada
                desercion_acumulada += desertores
                desercion_acumulada_Hombres += desertores_Hombres
                desercion_acumulada_Mujeres += desertores_Mujeres
                
                # Calcular tasa de deserción
                tasa = self.calculate_rate(desercion_acumulada, poblacion_nuevo_ingreso)
                tasa_desercion_Hombres = self.calculate_rate(desercion_acumulada_Hombres, poblacion_nuevo_ingreso)
                tasa_desercion_Mujeres = self.calculate_rate(desercion_acumulada_Mujeres, poblacion_nuevo_ingreso)
                response_data[periodo_actual]['tasa_desercion'] = tasa
                response_data[periodo_actual]['tasa_desercion_Hombres'] = tasa_desercion_Hombres
                response_data[periodo_actual]['tasa_desercion_Mujeres'] = tasa_desercion_Mujeres

        logger.info(f"response_data: {response_data}")
        return response_data

    def calculate_rate(self, desercion_total, poblacion_nuevo_ingreso):
        """Calcula tasa de deserción"""
        return calcularTasa(desercion_total, poblacion_nuevo_ingreso)
    
class IndicesGeneracionalBase(APIView):
    """Clase base para índices generacionales"""
    permission_classes = [permissions.IsAuthenticated]

    def get_generaciones(self, cohorte, num_generaciones=9):
        """Obtiene las generaciones a analizar"""
        cohorte_actual = int(cohorte)
        año_base = cohorte_actual // 10
        semestre_actual = cohorte_actual % 10
        generaciones = []
        
        for i in range(9):
            año = año_base + (i // 2)
            semestre = 1 if i % 2 == 0 else 3
            generacion = str(año * 10 + semestre)
            generaciones.append(generacion)
        
        # Generaciones a analizar
        logger.info(f"Generaciones obtenidas: {generaciones}")

        return generaciones

    def get_base_data(self, tipos, generacion, carrera, periodos):
        """Obtiene datos base comunes"""
        alumnos = Ingreso.objects.filter(
            tipo__in=tipos,
            periodo=generacion,
            alumno__plan__carrera__pk=carrera
        ).annotate(
            clave=F("alumno_id")
        ).values("clave")


        poblacion_inicial = obtenerPoblacionActiva(tipos, alumnos, generacion, carrera)
        poblacion_inicial_hombres = poblacion_inicial['hombres']
        poblacion_inicial_mujeres = poblacion_inicial['mujeres']
        logger.info(f"Población inicial: {poblacion_inicial}, Hombres: {poblacion_inicial_hombres}, Mujeres: {poblacion_inicial_mujeres}")
        return alumnos, poblacion_inicial['poblacion'], poblacion_inicial_hombres, poblacion_inicial_mujeres

    def get(self, request, format=None):
        """Método GET común"""
        try:
            cohorte = request.GET.get('cohorte', '20241')
            num_semestres = int(request.GET.get('semestres', '9'))
            carrera = request.GET.get('carrera')
            
            if not carrera:
                return Response({'error': 'Carrera es requerida'}, status=400)

            tipos = calcularTipos(
                request.GET.get('nuevo-ingreso'),
                request.GET.get('traslado-equivalencia')
            )

            response_data = {}
            generaciones = self.get_generaciones(cohorte, num_semestres)

            for gen in generaciones:
                periodos = calcularPeriodos(gen, num_semestres + 1)
                response_data[gen] = self.process_generation(tipos, gen, carrera, periodos)

            return Response(response_data)

        except Exception as ex:
            logger.error(f"Error en {self.__class__.__name__}: {str(ex)}")
            return Response({'error': str(ex)}, status=500)

    @abstractmethod
    def process_generation(self, tipos, generacion, carrera, periodos):
        """Cada subclase implementa su procesamiento específico"""
        pass

class IndicesGeneracionalDesercion(IndicesGeneracionalBase):
    """
    Vista para listar los índices de deserción por generación.

    * Requiere autenticación por token.
    """
    def process_generation(self, tipos, generacion, carrera, periodos):
        """Procesa datos de deserción para una generación"""
        alumnos, total_inicial, total_inicial_hombres, total_inicial_mujeres = self.get_base_data(tipos, generacion, carrera, periodos)
        desercion_total = 0
        desercion_hombres = 0
        desercion_mujeres = 0
        alumnos_periodo_anterior = alumnos
        periodo_anterior = generacion
        ultimo_periodo = periodos[-1]

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
            
            desercion_hombres += desercion['hombres']
            desercion_mujeres += desercion['mujeres']
            desercion_total += desercion['hombres'] + desercion['mujeres']
            alumnos_periodo_anterior = alumnos_periodo
            periodo_anterior = periodo

        poblacion_actual = obtenerPoblacionActiva(['RE'], alumnos, ultimo_periodo, carrera)
        total_actual = poblacion_actual['poblacion']
        tasa_desercion = calcularTasa(desercion_total, total_inicial)
        tasa_desercion_hombres = calcularTasa(desercion_hombres, total_inicial)
        tasa_desercion_mujeres = calcularTasa(desercion_mujeres, total_inicial)
        
        return {
            'total_inicial': total_inicial,
            'total_inicial_hombres': total_inicial_hombres,
            'total_inicial_mujeres': total_inicial_mujeres,
            'total_actual': total_actual,
            'total_actual_hombres': poblacion_actual['hombres'],
            'total_actual_mujeres': poblacion_actual['mujeres'],
            'desercion_total': desercion_total,
            'tasa_desercion': tasa_desercion,
            'tasa_desercion_hombres': tasa_desercion_hombres,
            'tasa_desercion_mujeres': tasa_desercion_mujeres,
            'ultimo_periodo': ultimo_periodo
        }

class IndicesGeneracionalPermanencia(IndicesGeneracionalBase):
    """Vista para listar los índices de permanencia por generación."""
    def process_generation(self, tipos, generacion, carrera, periodos):
        """Procesa datos de permanencia para una generación"""
        # Obtener datos base
        alumnos, total_inicial, total_inicial_hombres, total_inicial_mujeres = self.get_base_data(tipos, generacion, carrera, periodos)
        ultimo_periodo = periodos[-1]

        # Obtener egresados acumulados hasta el periodo anterior al último
        egresados_acumulados = obtenerEgresadosAcumulados(alumnos, periodos, ultimo_periodo)
        egresados_acumulados_total = egresados_acumulados['total']
        egresados_acumulados_hombres = egresados_acumulados['hombres']
        egresados_acumulados_mujeres = egresados_acumulados['mujeres']
        
        logger.info(f"""
            Egresados acumulados hasta {ultimo_periodo}:
            Total egresados: {egresados_acumulados}
            ------------------------
        """)

        # Obtener población actual
        poblacion_actual = obtenerPoblacionActiva(['RE'], alumnos, ultimo_periodo, carrera)
        total_actual = poblacion_actual['poblacion']
        total_actual_hombres = poblacion_actual['hombres']
        total_actual_mujeres = poblacion_actual['mujeres']

        # Calcular total actual incluyendo egresados
        total_actual_con_egresados = total_actual + egresados_acumulados_total
        total_actual_con_egresados_hombres = total_actual_hombres + egresados_acumulados_hombres
        total_actual_con_egresados_mujeres = total_actual_mujeres + egresados_acumulados_mujeres
        logger.info(f"""
            Cálculo final:
            Total actual ({total_actual}) + Egresados acumulados ({egresados_acumulados}) = {total_actual_con_egresados}
            Total actual hombres ({total_actual_hombres}) + Egresados acumulados hombres ({egresados_acumulados_hombres}) = {total_actual_con_egresados_hombres}
            Total actual mujeres ({total_actual_mujeres}) + Egresados acumulados mujeres ({egresados_acumulados_mujeres}) = {total_actual_con_egresados_mujeres}
            ------------------------
        """)

        # Calcular tasa de permanencia
        tasa_permanencia = calcularTasa(total_actual_con_egresados, total_inicial)
        tasa_permanencia_hombres = calcularTasa(total_actual_con_egresados_hombres, total_inicial)
        tasa_permanencia_mujeres = calcularTasa(total_actual_con_egresados_mujeres, total_inicial)

        # Retornar solo los datos necesarios
        return {
            'total_inicial': total_inicial,
            'total_inicial_hombres': total_inicial_hombres,
            'total_inicial_mujeres': total_inicial_mujeres,
            'total_actual_hombres': total_actual_con_egresados_hombres,
            'total_actual_mujeres': total_actual_con_egresados_mujeres,
            'total_actual': total_actual_con_egresados,
            'tasa_permanencia': tasa_permanencia,
            'tasa_permanencia_hombres': tasa_permanencia_hombres,
            'tasa_permanencia_mujeres': tasa_permanencia_mujeres,
        }

class IndicesGeneracionalEgreso(IndicesGeneracionalBase):
    def process_generation(self, tipos, generacion, carrera, periodos):
        """Procesa datos de egreso para una generación"""
        alumnos, total_inicial, total_inicial_hombres, total_inicial_mujeres = self.get_base_data(tipos, generacion, carrera, periodos)
        total_egresados = 0
        total_egresados_hombres = 0
        total_egresados_mujeres = 0
        # Procesar cada periodo igual que IndicesEgreso
        for periodo in periodos[:-1]:
            # Obtener población inactiva del periodo
            poblacion_inactiva = obtenerPoblacionInactiva(alumnos, periodo)
            
            # Sumar egresados del periodo
            egresados_periodo = poblacion_inactiva['egreso']['egresados']
            egresados_hombres_periodo = poblacion_inactiva['egreso']['hombres']
            egresados_mujeres_periodo = poblacion_inactiva['egreso']['mujeres']
            total_egresados += egresados_periodo
            total_egresados_hombres += egresados_hombres_periodo
            total_egresados_mujeres += egresados_mujeres_periodo

        # Calcular tasa final
        tasa_egreso = calcularTasa(total_egresados, total_inicial)
        tasa_egreso_hombres = calcularTasa(total_egresados_hombres, total_inicial)
        tasa_egreso_mujeres = calcularTasa(total_egresados_mujeres, total_inicial)

        logger.info(f"""
            Cálculo de egreso generacional:
            Generación: {generacion}
            Total inicial: {total_inicial}
            Total egresados acumulados: {total_egresados}
            Tasa de egresados hombres: {tasa_egreso_hombres}
            Tasa de egresados mujeres: {tasa_egreso_mujeres}
            Tasa de egreso: {tasa_egreso}
            ------------------------
        """)

        return {
            'total_inicial': total_inicial,
            'total_inicial_hombres': total_inicial_hombres,
            'total_inicial_mujeres': total_inicial_mujeres,
            'total_actual': total_egresados,
            'total_actual_hombres': total_egresados_hombres,
            'total_actual_mujeres': total_egresados_mujeres,
            'tasa_egreso': tasa_egreso,
            'tasa_egreso_hombres': tasa_egreso_hombres,
            'tasa_egreso_mujeres': tasa_egreso_mujeres
        }
class IndicesGeneracionalTitulacion(IndicesGeneracionalBase):
    def process_generation(self, tipos, generacion, carrera, periodos):
        """Procesa datos de egreso para una generación"""
        alumnos, total_inicial, total_inicial_hombres, total_inicial_mujeres = self.get_base_data(tipos, generacion, carrera, periodos)
        total_titulados = 0
        total_titulados_hombres = 0
        total_titulados_mujeres = 0

        # Procesar cada periodo igual que IndicesEgreso
        for periodo in periodos:
            # Obtener población inactiva del periodo
            poblacion_inactiva = obtenerPoblacionInactiva(alumnos, periodo)
            
            # Sumar egresados del periodo
            titulados_periodo = poblacion_inactiva['titulacion']['titulados']
            titulados_hombres_periodo = poblacion_inactiva['titulacion']['hombres']
            titulados_mujeres_periodo = poblacion_inactiva['titulacion']['mujeres']
            total_titulados += titulados_periodo
            total_titulados_hombres += titulados_hombres_periodo
            total_titulados_mujeres += titulados_mujeres_periodo

        # Calcular tasa final
        tasa_titulacion = calcularTasa(total_titulados, total_inicial)
        tasa_titulacion_hombres = calcularTasa(total_titulados_hombres, total_inicial)
        tasa_titulacion_mujeres = calcularTasa(total_titulados_mujeres, total_inicial)

        logger.info(f"""
            Cálculo de titulacion generacional:
            Generación: {generacion}
            Total inicial: {total_inicial}
            Total titulados acumulados: {total_titulados}
            Tasa de titulados hombres: {tasa_titulacion_hombres}
            Tasa de titulados mujeres: {tasa_titulacion_mujeres}
            Tasa de titulacion: {tasa_titulacion}
            ------------------------
        """)

        return {
            'total_inicial': total_inicial,
            'total_inicial_hombres': total_inicial_hombres,
            'total_inicial_mujeres': total_inicial_mujeres,
            'total_actual': total_titulados,
            'total_actual_hombres': total_titulados_hombres,
            'total_actual_mujeres': total_titulados_mujeres,
            'tasa_titulacion_mujeres': tasa_titulacion_mujeres,
            'tasa_titulacion_hombres': tasa_titulacion_hombres,
            'tasa_titulacion': tasa_titulacion
        }

# Función para calcular los desertores
def calcularDesercion(lista_alumnos_periodo_anterior, lista_alumnos_periodo_actual, lista_alumnos_egresados):
    desertores = {'hombres': 0, 'mujeres': 0}
    alumnos_actuales = {alumno['clave'] for alumno in lista_alumnos_periodo_actual}
    alumnos_anteriores = {alumno['clave'] for alumno in lista_alumnos_periodo_anterior}
    egresados = {alumno['clave'] for alumno in lista_alumnos_egresados}
    
    # Encontrar alumnos que desertaron (estaban antes pero ya no están y no egresaron)
    for alumno_clave in alumnos_anteriores - alumnos_actuales - egresados:
        datos_alumno = Personal.objects.get(alumno__no_control=alumno_clave)
        if datos_alumno.genero == 'H':
            desertores['hombres'] += 1
        elif datos_alumno.genero == 'M':
            desertores['mujeres'] += 1

    # Encontrar alumnos que reingresaron (no estaban antes pero ahora sí)
    for alumno_clave in alumnos_actuales - alumnos_anteriores:
        datos_alumno = Personal.objects.get(alumno__no_control=alumno_clave)
        if datos_alumno.genero == 'H':
            desertores['hombres'] -= 1  # Restar para indicar reingreso
        elif datos_alumno.genero == 'M':
            desertores['mujeres'] -= 1  # Restar para indicar reingreso
            
    return desertores
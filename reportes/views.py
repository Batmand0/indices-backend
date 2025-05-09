from django.http import JsonResponse
# Create your views here.
from django.db.models import Count, F, Q, Value
from django.db.models.functions import Coalesce
from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import permissions

from registros.models import Ingreso, Egreso, Titulacion
from planes.models import Plan
from carreras.models import Carrera
from registros.periodos import calcularPeriodos, getPeriodoActual

from decimal import Decimal
from indices.views import obtenerPoblacionEgreso, obtenerPoblacionTitulada, obtenerPoblacionActiva, calcularTasa, calcularTipos
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

def obtenerPoblacionNuevoIngreso(tipos_ingreso, periodos, carreras):
    """Obtiene población de nuevo ingreso para múltiples periodos y carreras en una sola consulta"""
    return Ingreso.objects.filter(
        tipo__in=tipos_ingreso,
        periodo__in=periodos,
        alumno__plan__carrera__pk__in=carreras
    ).values(
        'periodo', 
        'alumno__plan__carrera__pk'
    ).annotate(
        hombres=Count('pk', filter=Q(alumno__curp__genero='H')),
        mujeres=Count('pk', filter=Q(alumno__curp__genero='M')),
        total=Count('pk')
    )

def obtenerPoblacionEgresoMultiple(tipos, cohorte, periodos, carrera_pk):
    """Obtiene población de egreso para múltiples periodos"""
    # Primero obtenemos los alumnos de nuevo ingreso
    alumnos = Ingreso.objects.filter(
        tipo__in=tipos,
        periodo=cohorte,
        alumno__plan__carrera__pk=carrera_pk
    ).values_list('alumno_id', flat=True)

    # Luego obtenemos los egresos de esos alumnos
    return Egreso.objects.filter(
        alumno_id__in=alumnos,
        periodo__in=periodos
    ).values(
        'periodo'
    ).annotate(
        hombres=Count('pk', filter=Q(alumno__curp__genero='H')),
        mujeres=Count('pk', filter=Q(alumno__curp__genero='M')),
        total=Count('pk')
    )

class ReportesBase(APIView):
    """Clase base para todos los reportes"""
    permission_classes = [permissions.IsAuthenticated]

    def get_base_params(self, request):
        """Obtiene parámetros base comunes para todos los reportes"""
        return {
            'nuevo_ingreso': request.query_params.get('nuevo-ingreso'),
            'traslado_equivalencia': request.query_params.get('traslado-equivalencia'),
            'cohorte': request.query_params.get('cohorte') if request.query_params.get('cohorte') else getPeriodoActual(),
            'semestres': request.query_params.get('semestres') if request.query_params.get('semestres') else '9'
        }

    def get_base_data(self, params):
        """Obtiene datos base comunes para todos los reportes"""
        return {
            'tipos': calcularTipos(params['nuevo_ingreso'], params['traslado_equivalencia']),
            'periodos': calcularPeriodos(params['cohorte'], int(params['semestres'])),
            'carreras': Carrera.objects.values('clave', 'nombre'),
            'cohorte': params['cohorte'],  # Agregar cohorte
            'semestres': params['semestres']  # Agregar semestres
        }

    def process_response(self, data):
        """Método abstracto para procesar la respuesta específica de cada reporte"""
        raise NotImplementedError("Las subclases deben implementar process_response")

    def get(self, request, format=None):
        """Método GET común para todos los reportes"""
        try:
            params = self.get_base_params(request)
            base_data = self.get_base_data(params)
            response_data = self.process_response(base_data)
            return Response(response_data)
        except Exception as e:
            logger.error(f"Error en {self.__class__.__name__}: {str(e)}")
            return Response({'error': str(e)}, status=500)

class ReportesNuevoIngreso(ReportesBase):
    """
    Vista para listar la cantidad de alumnos de nuevo ingreso por carrera.

    * Requiere autenticación por token.

    ** nuevo-ingreso: Alumnos ingresando en 1er por examen o convalidacion
    ** traslado-equivalencia: Alumnos ingresando de otro TEC u otra escuela
    ** cohorte: El periodo donde empezara el calculo
    ** semestres: Cuantos semestres seran calculados desde el cohorte
    """
    def process_response(self, data):
        response_data = {}
        poblacion_data = obtenerPoblacionNuevoIngreso(
            data['tipos'], 
            data['periodos'],
            [plan['clave'] for plan in data['carreras']]
        )

        for plan in data['carreras']:
            plan_regs = {}
            for periodo in data['periodos']:
                datos = next(
                    (item for item in poblacion_data 
                     if item['periodo'] == periodo and 
                     item['alumno__plan__carrera__pk'] == plan['clave']),
                    {'hombres': 0, 'mujeres': 0}
                )
                
                plan_regs[periodo] = {
                    'periodo': periodo,
                    'hombres': datos['hombres'],
                    'mujeres': datos['mujeres']
                }
                
            response_data[plan['nombre']] = plan_regs
        
        return response_data

class ReportesEgreso(ReportesBase):
    def process_response(self, data):
        response_data = {}
        
        # Obtener alumnos de nuevo ingreso por carrera
        for carrera in data['carreras']:
            poblacion_nuevo_ingreso = Ingreso.objects.filter(
                tipo__in=data['tipos'],
                periodo=data['cohorte'],
                alumno__plan__carrera__pk=carrera['clave']
            ).count()

            # Calcular periodos de egreso
            periodos_egreso = []
            semestres = int(data['semestres'])
            if semestres >= 8:
                periodos_egreso = data['periodos'][8:min(12, semestres)]
                if semestres > 12:
                    periodos_egreso.extend(data['periodos'][12:semestres])

            # Obtener datos de egreso
            egreso_data = obtenerPoblacionEgresoMultiple(
                data['tipos'],
                data['cohorte'],
                periodos_egreso,
                carrera['clave']
            )

            registros__semestres = {}
            
            # Procesar primeros 12 semestres
            egreso_total = {'total': 0, 'hombres': 0, 'mujeres': 0}
            for sem in range(8, min(semestres, 12)):
                datos_periodo = next(
                    (item for item in egreso_data 
                     if item['periodo'] == data['periodos'][sem]),
                    {'hombres': 0, 'mujeres': 0, 'total': 0}
                )
                
                egreso_total['total'] += datos_periodo['total']
                egreso_total['hombres'] += datos_periodo['hombres']
                egreso_total['mujeres'] += datos_periodo['mujeres']
                
                registros__semestres[sem + 1] = {
                    'hombres': datos_periodo['hombres'],
                    'mujeres': datos_periodo['mujeres']
                }

            registros__semestres['total_1'] = {'valor': egreso_total['total']}
            tasa_egreso = calcularTasa(egreso_total['total'], poblacion_nuevo_ingreso)
            registros__semestres['tasa_egreso_1'] = {'valor': f"{tasa_egreso} %"}

            # ... resto del código para semestres > 12 ...

            response_data[carrera['nombre']] = {
                'carrera': carrera['nombre'],
                'poblacion_nuevo_ingreso': {'poblacion': poblacion_nuevo_ingreso},
                'registros': registros__semestres
            }

        return response_data

class ReportesTitulacion(ReportesBase):
    """
    Vista para listar la cantidad de alumnos de nuevo ingreso por carrera.

    * Requiere autenticación por token.

    ** nuevo-ingreso: Alumnos ingresando en 1er por examen o convalidacion
    ** traslado-equivalencia: Alumnos ingresando de otro TEC u otra escuela
    ** cohorte: El periodo donde empezara el calculo
    ** semestres: Cuantos semestres seran calculados desde el cohorte
    """
    def process_response(self, data):
        response_data = {}
        carreras = Carrera.objects.select_related('clave__plan').values_list('clave', 'plan', 'nombre')

        for carrera in carreras:
            alumnos = (Ingreso.objects
                .filter(tipo__in=data['tipos'], 
                       periodo=data['cohorte'],
                       alumno__plan__carrera__pk=carrera[0])
                .annotate(clave=F("alumno_id"))
                .values("clave"))

            registros__semestres = {}
            poblacion_nuevo_ingreso = obtenerPoblacionActiva(data['tipos'], alumnos, data['cohorte'], carrera[0])
            
            titulados_total = crearTotales()
            for i in range(8, int(data['semestres']) if int(data['semestres']) <= 12 else 12):
                titulados_periodo = obtenerPoblacionTitulada(alumnos, data['periodos'][i])
                titulados_total = actualizarTotales(titulados_total, titulados_periodo)
                registros__semestres[i+1] = dict(hombres=titulados_periodo['hombres'], mujeres=titulados_periodo['mujeres'])

            registros__semestres['total_1'] = dict(valor=titulados_total['total'])
            tasa_titulados = calcularTasa(titulados_total['total'], poblacion_nuevo_ingreso['poblacion'])
            registros__semestres['tasa_titulacion_1'] = dict(valor="{tasa_titulados} %".format(tasa_titulados=tasa_titulados))

            titulados_total = crearTotales()
            if int(data['semestres']) > 12:
                for i in range(12, int(data['semestres'])):
                    titulados_periodo = obtenerPoblacionTitulada(alumnos, data['periodos'][i])
                    titulados_total = actualizarTotales(titulados_total, titulados_periodo)
                registros__semestres[13] = dict(hombres=titulados_total['hombres'], mujeres=titulados_total['mujeres'])
                tasa_titulados = calcularTasa(titulados_total['total'], poblacion_nuevo_ingreso['poblacion'])
                registros__semestres['tasa_titulacion_2'] = dict(valor="{tasa_titulados} %".format(tasa_titulados=tasa_titulados))
            response_data[carrera[2]] = dict(poblacion_nuevo_ingreso=poblacion_nuevo_ingreso , registros=registros__semestres)

        return response_data

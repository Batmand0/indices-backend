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
from indices.views import obtenerPoblacionEgreso, obtenerPoblacionInactiva, obtenerPoblacionTitulada, obtenerPoblacionActiva, calcularTasa, calcularTipos
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

def obtenerPoblacionNuevoIngresoCarrera(tipos, cohorte, carrera_pk):
    """Obtiene población de nuevo ingreso para una carrera en su cohorte"""
    return Ingreso.objects.filter(
        tipo__in=tipos,
        periodo=cohorte,
        alumno__plan__carrera__pk=carrera_pk
    ).aggregate(
        poblacion=Count('pk'),
        hombres=Count('pk', filter=Q(alumno__curp__genero='H')),
        mujeres=Count('pk', filter=Q(alumno__curp__genero='M'))
    )

def obtenerPoblacionEgresoMultiple(tipos, cohorte, periodos, carrera_pk):
    """Obtiene población de egreso acumulada para los periodos"""
    alumnos = Ingreso.objects.filter(
        tipo__in=tipos,
        periodo=cohorte,
        alumno__plan__carrera__pk=carrera_pk
    ).values_list('alumno_id', flat=True)

    # Obtener egresos acumulados
    return Egreso.objects.filter(
        alumno_id__in=alumnos,
        periodo__in=periodos
    ).values(
        'periodo'
    ).annotate(
        hombres=Count('pk', filter=Q(alumno__curp__genero='H')),
        mujeres=Count('pk', filter=Q(alumno__curp__genero='M')),
        total=Count('pk')
    ).order_by('periodo')  # Ordenar por periodo para acumulación correcta

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
                    {'hombres': 0, 'mujeres': 0, 'total': 0}
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
        semestres = int(data['semestres'])
        
        for carrera in data['carreras']:
            # Obtener nuevo ingreso y alumnos del cohorte
            alumnos = Ingreso.objects.filter(
                tipo__in=data['tipos'],
                periodo=data['cohorte'],
                alumno__plan__carrera__pk=carrera['clave']
            ).values('alumno_id')

            poblacion_inicial = obtenerPoblacionNuevoIngresoCarrera(
                data['tipos'], 
                data['cohorte'], 
                carrera['clave']
            )

            registros__semestres = {}
            egreso_acumulado = {'total': 0, 'hombres': 0, 'mujeres': 0}
            
            # Un solo bucle para procesar todos los periodos
            for sem in range(7, min(semestres, 12)):
                periodo = data['periodos'][sem]
                poblacion_inactiva = obtenerPoblacionInactiva(alumnos, periodo)
                
                # Acumular egresados
                egreso_acumulado['total'] += poblacion_inactiva['egreso']['egresados']
                egreso_acumulado['hombres'] += poblacion_inactiva['egreso']['hombres']
                egreso_acumulado['mujeres'] += poblacion_inactiva['egreso']['mujeres']

                # Guardar en registros solo si es semestre 8 o mayor
                if sem >= 8:
                    registros__semestres[sem + 1] = {
                        'hombres': poblacion_inactiva['egreso']['hombres'],
                        'mujeres': poblacion_inactiva['egreso']['mujeres']
                    }

            # Calcular tasa con el acumulado total (7-12)
            registros__semestres['total_1'] = {'valor': egreso_acumulado['total']}
            tasa_egreso = calcularTasa(egreso_acumulado['total'], poblacion_inicial['poblacion'])
            registros__semestres['tasa_egreso_1'] = {'valor': f"{tasa_egreso} %"}
            
            # Agregar logging para verificar
            logger.info(f"""
                Egresados acumulados para carrera {carrera['nombre']}:
                Periodo inicial: {data['cohorte']}
                Total acumulado: {egreso_acumulado['total']}
                Hombres acumulados: {egreso_acumulado['hombres']}
                Mujeres acumuladas: {egreso_acumulado['mujeres']}
                ------------------------
            """)

            if semestres > 12:
                egreso_total_2 = {'total': 0, 'hombres': 0, 'mujeres': 0}
                for i in range(12, semestres):  # Cambiar 11 por 12
                    periodo = data['periodos'][i]
                    poblacion_inactiva = obtenerPoblacionInactiva(alumnos, periodo)
                    
                    # Sumar egresados del periodo
                    egreso_total_2['total'] += poblacion_inactiva['egreso']['egresados']
                    egreso_total_2['hombres'] += poblacion_inactiva['egreso']['hombres']
                    egreso_total_2['mujeres'] += poblacion_inactiva['egreso']['mujeres']

                # Guardar datos acumulados después del semestre 12
                registros__semestres[13] = {
                    'hombres': egreso_total_2['hombres'],
                    'mujeres': egreso_total_2['mujeres']
                }
                
                # Calcular tasa total acumulada (primeros 12 + posteriores)
                tasa_egreso_2 = calcularTasa(
                    egreso_acumulado['total'] + egreso_total_2['total'], 
                    poblacion_inicial['poblacion']
                )
                registros__semestres['tasa_egreso_2'] = {'valor': f"{tasa_egreso_2} %"}

            response_data[carrera['nombre']] = {
                'carrera': carrera['nombre'],
                'poblacion_nuevo_ingreso': {
                    'poblacion': poblacion_inicial['poblacion'],
                    'hombres': poblacion_inicial['hombres'],
                    'mujeres': poblacion_inicial['mujeres']
                },
                'registros': registros__semestres
            }

        return response_data

class ReportesTitulacion(ReportesBase):
    def process_response(self, data):
        response_data = {}
        
        for carrera in data['carreras']:
            # Obtener nuevo ingreso del cohorte
            poblacion_inicial = obtenerPoblacionNuevoIngresoCarrera(
                data['tipos'], 
                data['cohorte'], 
                carrera['clave']
            )

            alumnos = (Ingreso.objects
                .filter(tipo__in=data['tipos'], 
                       periodo=data['cohorte'],
                       alumno__plan__carrera__pk=carrera['clave'])
                .values('alumno_id'))

            registros__semestres = {}
            
            titulados_total = crearTotales()
            for i in range(8, int(data['semestres']) if int(data['semestres']) <= 12 else 12):
                titulados_periodo = obtenerPoblacionTitulada(alumnos, data['periodos'][i])
                titulados_total = actualizarTotales(titulados_total, titulados_periodo)
                registros__semestres[i+1] = {
                    'hombres': titulados_periodo['hombres'],
                    'mujeres': titulados_periodo['mujeres']
                }

            registros__semestres['total_1'] = {'valor': titulados_total['total']}
            tasa_titulados = calcularTasa(titulados_total['total'], poblacion_inicial['poblacion'])
            registros__semestres['tasa_titulacion_1'] = {'valor': f"{tasa_titulados} %"}

            if int(data['semestres']) > 12:
                titulados_total_2 = crearTotales()
                for i in range(12, int(data['semestres'])):
                    titulados_periodo = obtenerPoblacionTitulada(alumnos, data['periodos'][i])
                    titulados_total_2 = actualizarTotales(titulados_total_2, titulados_periodo)
                registros__semestres[13] = {
                    'hombres': titulados_total_2['hombres'],
                    'mujeres': titulados_total_2['mujeres']
                }
                tasa_titulados_2 = calcularTasa(titulados_total_2['total'], poblacion_inicial['poblacion'])
                registros__semestres['tasa_titulacion_2'] = {'valor': f"{tasa_titulados_2} %"}

            response_data[carrera['nombre']] = {
                'carrera': carrera['nombre'],
                'poblacion_nuevo_ingreso': {
                    'poblacion': poblacion_inicial['poblacion'],
                    'hombres': poblacion_inicial['hombres'],
                    'mujeres': poblacion_inicial['mujeres']
                },
                'registros': registros__semestres
            }

        return response_data

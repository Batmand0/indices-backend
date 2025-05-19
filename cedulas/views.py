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
    """Vista para generar la tabla de cédulas CACEI."""
    permission_classes = [permissions.IsAuthenticated]

    def get_population_data(self, tipos, periodo, carrera):
        """Obtiene datos de población en una sola consulta"""
        return Ingreso.objects.filter(
            periodo=periodo
        ).aggregate(
            poblacion_total=Count('pk', filter=Q(tipo__in=tipos)),
            poblacion_carrera=Count('pk', filter=Q(
                tipo__in=tipos, 
                alumno__plan__carrera__pk=carrera
            ))
        )

    def get_alumni_data(self, alumnos_carrera, periodo_inicio, periodo_fin):
        """Obtiene datos de egreso y titulación en una sola consulta"""
        return {
            'egresados': Egreso.objects.filter(
                alumno_id__in=alumnos_carrera,
                periodo__gte=periodo_inicio,
                periodo__lte=periodo_fin
            ).count(),
            'titulados': Titulacion.objects.filter(
                alumno_id__in=alumnos_carrera,
                periodo__gte=periodo_inicio,
                periodo__lte=periodo_fin
            ).count()
        }

    def format_period(self, periodo):
        """Formatea el periodo para mostrar"""
        if periodo.endswith('1'):
            return f"2/{periodo}"
        return f"8/{periodo}"

    def get(self, request, format=None):
        try:
            # Obtener parámetros
            tipos = []
            if request.GET.get('nuevo-ingreso'):
                tipos.extend(['EX', 'CO'])
            if request.GET.get('traslado-equivalencia'):
                tipos.extend(['TR', 'EQ'])
            
            cohorte = request.GET.get('cohorte', getPeriodoActual())
            carrera = request.GET.get('carrera')

            # Calcular períodos una sola vez
            periodos = calcularPeriodos(cohorte, 24)
            response_data = {}

            # Procesar generaciones
            for gen in range(10):
                periodo_inicial = periodos[gen]
                periodo_final = periodos[gen + 8]

                # Obtener alumnos de la carrera una sola vez
                alumnos_carrera = list(Ingreso.objects.filter(
                    tipo__in=tipos,
                    periodo=periodo_inicial,
                    alumno__plan__carrera__pk=carrera
                ).values_list('alumno_id', flat=True))

                # Obtener datos de población
                poblacion_data = self.get_population_data(tipos, periodo_inicial, carrera)
                poblacion_total = poblacion_data['poblacion_total']
                poblacion_nuevo_ingreso = poblacion_data['poblacion_carrera']

                # Obtener datos de egreso y titulación
                alumni_data = self.get_alumni_data(
                    alumnos_carrera, 
                    periodo_inicial, 
                    periodo_final
                )

                # Calcular tasas
                tasa_egreso = round(
                    (alumni_data['egresados'] * 100 / poblacion_nuevo_ingreso)
                    if poblacion_nuevo_ingreso > 0 else 0, 
                    2
                )
                tasa_titulo = round(
                    (alumni_data['titulados'] * 100 / poblacion_nuevo_ingreso)
                    if poblacion_nuevo_ingreso > 0 else 0, 
                    2
                )
                porcentaje_alumnos = round(
                    (poblacion_nuevo_ingreso * 100 / poblacion_total)
                    if poblacion_total > 0 else 0,
                    2
                )

                # Formar generación
                generacion = f"{self.format_period(periodo_inicial)} - {self.format_period(periodo_final)}"

                # Guardar resultados
                response_data[generacion] = {
                    'poblacion_total': poblacion_total,
                    'poblacion': poblacion_nuevo_ingreso,
                    'porcentaje_alumnos_carrera': porcentaje_alumnos,
                    'egresados': alumni_data['egresados'],
                    'tasa_egreso': tasa_egreso,
                    'titulados': alumni_data['titulados'],
                    'tasa_titulacion': tasa_titulo
                }

            return Response(response_data)

        except Exception as e:
            return Response({'error': str(e)}, status=500)

class CedulasCACECA(APIView):
    """Vista para generar la tabla de cédulas CACECA."""
    permission_classes = [permissions.IsAuthenticated]

    def get_population_data(self, tipos, periodo, carrera):
        """Obtiene datos de población inicial en una sola consulta"""
        alumnos = Ingreso.objects.filter(
            tipo__in=tipos,
            periodo=periodo,
            alumno__plan__carrera__pk=carrera
        ).values_list('alumno_id', flat=True)

        poblacion = Ingreso.objects.filter(
            tipo__in=tipos,
            periodo=periodo,
            alumno_id__in=alumnos,
            alumno__plan__carrera__pk=carrera
        ).count()

        return {
            'alumnos': list(alumnos),
            'poblacion': poblacion
        }

    def get_tracking_data(self, alumnos, periodo_inicio, periodo_fin, carrera):
        """Obtiene datos de seguimiento en una sola consulta"""
        egresados = Egreso.objects.filter(
            alumno_id__in=alumnos,
            periodo__gte=periodo_inicio,
            periodo__lte=periodo_fin
        ).count()

        titulados = Titulacion.objects.filter(
            alumno_id__in=alumnos,
            periodo__gte=periodo_inicio,
            periodo__lte=periodo_fin
        ).count()

        activos = Ingreso.objects.filter(
            tipo='RE',
            alumno_id__in=alumnos,
            periodo=periodo_fin,
            alumno__plan__carrera__pk=carrera
        ).count()

        return {
            'egresados': egresados,
            'titulados': titulados,
            'activos': activos
        }

    def calculate_rates(self, data):
        """Calcula tasas a partir de los datos"""
        if data['poblacion_inicial'] == 0:
            return {
                'tasa_desercion': 0,
                'tasa_egreso': 0,
                'tasa_titulacion': 0,
                'tasa_reprobacion': 0,
                'desercion': 0,
                'reprobacion': 0
            }

        desercion = max(0, data['poblacion_inicial'] - data['activos'] - data['egresados'])
        reprobacion = data['poblacion_inicial'] - data['egresados'] - desercion

        return {
            'desercion': desercion,
            'reprobacion': reprobacion,
            'tasa_desercion': round(Decimal(desercion * 100) / data['poblacion_inicial'], 2),
            'tasa_egreso': round(Decimal(data['egresados'] * 100) / data['poblacion_inicial'], 2),
            'tasa_titulacion': round(Decimal(data['titulados'] * 100) / data['poblacion_inicial'], 2),
            'tasa_reprobacion': round(Decimal(reprobacion * 100) / data['poblacion_inicial'], 2)
        }

    def get(self, request, format=None):
        try:
            # Obtener parámetros
            tipos = []
            if request.GET.get('nuevo-ingreso'):
                tipos.extend(['EX', 'CO'])
            if request.GET.get('traslado-equivalencia'):
                tipos.extend(['TR', 'EQ'])
            
            cohorte = request.GET.get('cohorte', getPeriodoActual())
            carrera = request.GET.get('carrera')
            response_data = {}
            periodo_inicial = cohorte

            # Procesar generaciones
            for gen in range(3):
                periodos = calcularPeriodos(periodo_inicial, 9)
                periodo_final = periodos[8]

                # Obtener población inicial
                pop_data = self.get_population_data(tipos, periodo_inicial, carrera)
                
                # Obtener datos de seguimiento
                tracking_data = self.get_tracking_data(
                    pop_data['alumnos'],
                    periodo_inicial,
                    periodo_final,
                    carrera
                )

                # Calcular tasas
                rates = self.calculate_rates({
                    'poblacion_inicial': pop_data['poblacion'],
                    'activos': tracking_data['activos'],
                    'egresados': tracking_data['egresados'],
                    'titulados': tracking_data['titulados']
                })

                # Guardar resultados
                generacion = f"{periodos[0]} - {periodos[8]}"
                response_data[generacion] = {
                    'poblacion': pop_data['poblacion'],
                    'desercion': rates['desercion'],
                    'tasa_desercion': rates['tasa_desercion'],
                    'reprobacion': rates['reprobacion'],
                    'tasa_reprobacion': rates['tasa_reprobacion'],
                    'egresados': tracking_data['egresados'],
                    'titulados': tracking_data['titulados'],
                    'tasa_titulacion': rates['tasa_titulacion'],
                    'tasa_egreso': rates['tasa_egreso']
                }

                periodo_inicial = periodos[1]

            return Response(response_data)

        except Exception as e:
            return Response({'error': str(e)}, status=500)
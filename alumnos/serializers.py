from rest_framework import serializers
from rest_framework.validators import UniqueTogetherValidator

from registros.models import Ingreso, Egreso, Titulacion, LiberacionIngles
from registros.periodos import getPeriodoActual, calcularPeriodos
from .models import Alumno

class AlumnoSerializer(serializers.ModelSerializer):
    class Meta:
        model = Alumno
        fields = '__all__'
        validators = [
            UniqueTogetherValidator(
                queryset = Alumno.objects.all(),
                fields = ['no_control', 'curp']
            )
        ]

class HistorialSerializer(serializers.ModelSerializer):
    estatus = serializers.SerializerMethodField()
    registros = serializers.SerializerMethodField()

    class Meta:
        model = Alumno
        fields = '__all__'
        depth = 1

    def get_estatus(self, obj):
        titulado = Titulacion.objects.filter(alumno=obj.pk).exists()
        if titulado:
            return "Titulado"
        egresado = Egreso.objects.filter(alumno=obj.pk).exists()
        if egresado:
            return "Egresado"
        periodo_actual = getPeriodoActual()
        inscrito = Ingreso.objects.filter(alumno=obj.pk, periodo=periodo_actual).exists()
        if inscrito:
            return "Inscrito"
        else:
            return "Baja"

    def get_registros(self, obj):
        request = self.context['request']
        semestres = request.query_params.get('semestres')
        cohorte = request.query_params.get('cohorte')
        periodos = []
        registros = {}
        if semestres is not None and cohorte is not None:
            periodos = calcularPeriodos(cohorte, int(semestres))
            ingresos = Ingreso.objects.filter(alumno=obj.pk, periodo__in=periodos).values()
            registros["ingresos"] = ingresos
        else:
            ingresos = Ingreso.objects.filter(alumno=obj.pk).values()
            registros["ingresos"] = ingresos
        egreso = Egreso.objects.filter(alumno=obj.pk).values()
        registros["egreso"] = egreso

        titulacion = Titulacion.objects.filter(alumno=obj.pk).values()
        registros["titulacion"] = titulacion

        liberacion_ingles = LiberacionIngles.objects.filter(alumno=obj.pk).values()
        registros["liberacion_ingles"] = liberacion_ingles
        return registros
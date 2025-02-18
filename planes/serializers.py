from rest_framework import serializers
from rest_framework.validators import UniqueValidator
from .models import Plan

class PlanGETSerializer(serializers.ModelSerializer):
    class Meta:
        model = Plan
        fields = '__all__'
        depth = 1

    def validate(self, data):
        """
        Validar que la fecha final sea despues de la fecha de inicio
        """
        if data['fecha_final'] is not None:
            if data['fecha_inicio'] >= data['fecha_final']:
                raise serializers.ValidationError('La fecha final debe ser después de la fecha de inicio')
        return data


class PlanPOSTSerializer(serializers.ModelSerializer):
    clave = serializers.CharField(max_length=150, validators=[UniqueValidator(queryset=Plan.objects.all(), message='La clave debe de ser única.', lookup='iexact')])

    class Meta:
        model = Plan
        fields = '__all__'

    def validate(self, data):
        """
        Validar que la fecha final sea despues de la fecha de inicio
        """
        if data['fecha_final'] is not None:
            if data['fecha_inicio'] >= data['fecha_final']:
                raise serializers.ValidationError('La fecha final debe ser después de la fecha de inicio')
        return data
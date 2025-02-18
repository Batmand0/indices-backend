from rest_framework import serializers
from .models import Ingreso, Egreso, Titulacion, LiberacionIngles

class IngresoSerializer(serializers.ModelSerializer):
    class Meta:
        model = Ingreso
        fields = '__all__'

class EgresoSerializer(serializers.ModelSerializer):
    class Meta:
        model = Egreso
        fields = '__all__'

class TitulacionSerializer(serializers.ModelSerializer):
    class Meta:
        model = Titulacion
        fields = '__all__'

class LiberacionInglesSerializer(serializers.ModelSerializer):
    class Meta:
        model = LiberacionIngles
        fields = '__all__'

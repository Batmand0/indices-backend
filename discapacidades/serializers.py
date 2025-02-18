from rest_framework import serializers
from rest_framework.validators import UniqueValidator
from .models import Discapacidad

class DiscapacidadSerializer(serializers.ModelSerializer):
    nombre = serializers.CharField(max_length=150, validators=[UniqueValidator(queryset=Discapacidad.objects.all(), message='El nombre debe de ser único.', lookup='iexact')])

    class Meta:
        model = Discapacidad
        fields = '__all__'

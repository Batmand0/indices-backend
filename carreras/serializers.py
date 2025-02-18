from rest_framework import serializers
from rest_framework.validators import UniqueValidator
from .models import Carrera

class CarreraSerializer(serializers.ModelSerializer):
    clave = serializers.CharField(max_length=50, validators=[UniqueValidator(queryset=Carrera.objects.all(), message='La clave debe de ser única.', lookup='iexact')])

    class Meta:
        model = Carrera
        fields = '__all__'
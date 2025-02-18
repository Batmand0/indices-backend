from rest_framework import serializers
from .models import Personal
import datetime

class PersonalSerializer(serializers.ModelSerializer):
    class Meta:
        model = Personal
        fields = '__all__'

    def validate_fecha_nacimiento(self, value):
        if value > datetime.date.today():
            raise serializers.ValidationError('Fecha de nacimiento no puede ser una fecha futura')
        return value
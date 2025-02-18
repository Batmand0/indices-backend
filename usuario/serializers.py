from .models import Usuario
from django.contrib.auth.password_validation import validate_password
from django.utils.translation import gettext_lazy as _
from rest_framework_simplejwt.serializers import TokenObtainPairSerializer
from rest_framework import serializers

class CustomTokenObtainPairSerializer(TokenObtainPairSerializer):
    @classmethod
    def get_token(cls, user):
        token = super().get_token(user)

        token['username'] = user.username
        token['first_name'] = user.first_name
        token['paternal_surname'] = user.paternal_surname
        token['maternal_surname'] = user.maternal_surname
        token['email'] = user.email
        token['gender'] = user.gender
        token['is_staff'] = user.is_staff
        token['is_superuser'] = user.is_superuser

        return token

class RegisterSerializer(serializers.ModelSerializer):
    password = serializers.CharField(write_only=True, required=True, style={'input_type': 'password'}, validators=[validate_password])
    password2 = serializers.CharField(write_only=True, required=True, style={'input_type': 'password'})

    class Meta:
        model = Usuario
        fields = [
            'username',
            'first_name',
            'paternal_surname',
            'maternal_surname',
            'email',
            'gender',
            'password',
            'password2'
        ]

    def validate(self, attrs):
        if attrs['password'] != attrs['password2']:
            raise serializers.ValidationError(
                {'password': _('Los campos de contraseñas no son iguales')}
            )
        return attrs

    def create(self, validated_data):
        usuario = Usuario.objects.create(
            username=validated_data['username'],
            first_name=validated_data['first_name'],
            paternal_surname=validated_data['paternal_surname'],
            maternal_surname=validated_data['maternal_surname'],
            email=validated_data['email'],
            gender=validated_data['gender']
        )

        usuario.set_password(validated_data['password'])
        usuario.save()

        return usuario

class UserSerializer(serializers.ModelSerializer):
    class Meta:
        model = Usuario
        fields = ['id', 'username', 'email']

class UserListSerializer(serializers.ModelSerializer):
    # career_permissions = serializers.SerializerMethodField()
    # def get_career_permissions(self, value):
    #     return value['career_permissions']
    class Meta:
        model = Usuario
        fields = ['id', 'username', 'email']
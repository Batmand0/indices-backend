from django.db import models
from django.utils.translation import gettext_lazy as _
from django.core.exceptions import ValidationError
import re

class Discapacidad(models.Model):
    def validate_solo_letras(value):
        match = re.search(r'^[A-ZÁÉÍÓÚÑÄËÏÖÜ ]+$', value.upper())
        if not match:
            raise ValidationError(
                _('Enter a value formed only by letters'),
                params={'value': value},
            )

    nombre = models.CharField(max_length=150, null=False, blank=False, unique=True, validators=[validate_solo_letras])
    descripcion = models.TextField(null=True, blank=False)

    def save(self, *args, **kwargs):
        self.nombre = self.nombre.upper()
        super().save(*args, **kwargs)

    def __str__(self):
        return f'[{self.pk}] {self.nombre}'

    class Meta:
        verbose_name_plural = 'discapacidades'

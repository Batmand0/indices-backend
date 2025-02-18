from django.db import models
from django.utils.translation import gettext_lazy as _
from django.core.exceptions import ValidationError
import re

class Carrera(models.Model):
    def validate_nombre(value):
        match = re.search(r'^[A-ZÁÉÍÓÚÑÄËÏÖÜ. ]+$', value.upper())
        if not match:
            raise ValidationError(
                _('Enter a value formed only by letters'),
                params={'value': value},
            )

    clave = models.CharField(max_length=50, primary_key=True, blank=False)
    nombre = models.CharField(max_length=150, null=False, blank=False, validators=[validate_nombre])

    def __str__(self):
        return f'[{self.pk}] {self.clave} - {self.nombre}'

    def save(self, *args, **kwargs):
        self.clave = self.clave.upper()
        self.nombre = self.nombre.upper()
        super().save(*args, **kwargs)

    class Meta:
        permissions = [("ver_carrera", "puede ver la carrera")]

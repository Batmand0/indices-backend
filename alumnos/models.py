from django.db import models
from django.core.exceptions import ValidationError
import re

class Alumno(models.Model):
    def validate_nocontrol(value):
        match = re.match(r'^C?(0\d|[1-9]\d)(0[1-9]|[1-9]\d)(000[1-9]|00[1-9]\d|0[1-9]\d\d|[1-9]\d\d\d)$', value.upper())
        if match is None:
            raise ValidationError(
                'Numero de control invalido',
                params={'value': value},
            )

    no_control = models.CharField(primary_key=True, max_length=9, null=False, blank=False, verbose_name='número de control', validators=[validate_nocontrol])
    curp = models.ForeignKey('personal.Personal', on_delete=models.PROTECT, verbose_name='curp')
    plan = models.ForeignKey('planes.Plan', on_delete=models.PROTECT, verbose_name='plan de estudios')

    REQUIRED_FIELDS = [
        'no_control',
        'curp',
        'plan',
    ]

    def save(self, *args, **kwargs):
        self.no_control = self.no_control.upper()
        super().save(*args, **kwargs)

    def __str__(self):
        return f'[{self.pk}] {self.no_control}'

    class Meta:
        constraints = [
            models.UniqueConstraint(fields=['no_control', 'curp'], name='unique_alumno')
        ]

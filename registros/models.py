from django.core.exceptions import ValidationError
from django.core.validators import MinValueValidator, MaxValueValidator
from django.db import models
from .periodos import getPeriodoActual, getNumSemestre
import re

class RegistroManager(models.Manager):
    def contiene_corte(self, periodo):
        return self.filter(periodo=periodo, es_corte=True).exists()

    def realizar_corte(self, periodo):
        registros = self.filter(periodo=periodo, es_corte=False)
        for registro in registros:
            registro.es_corte = True
        return self.bulk_update(registros, ['es_corte'])

class BaseRegistro(models.Model):
    def validate_registro(value):
        match = re.search(r'^[0-9]{4}[13]$', value)
        if not match:
            raise ValidationError(
                'Formato inválido para periodo',
                params={'value': value}
            )
        periodo_actual = getPeriodoActual()
        if value > periodo_actual:
            raise ValidationError(
                'Periodo inválido, debe ser del año presente o anterior',
                params={'value': value}
            )

    periodo = models.CharField(max_length=5, null=False, blank=False, validators=[validate_registro])
    alumno = models.ForeignKey('alumnos.Alumno', on_delete=models.CASCADE, verbose_name='alumno')
    es_corte = models.BooleanField(default=False, null=False, blank=False)
    objects = RegistroManager()

    def save(self, *args, **kwargs):
        if self.pk and self.es_corte:
            raise ValidationError('Este registro ya no puede ser modificado')
        super(BaseRegistro, self).save(*args, **kwargs)

    def delete(self, *args, **kwargs):
        if self.es_corte:
            raise ValidationError('Este registro ya no puede ser eliminado')
        super(BaseRegistro, self).save(*args, **kwargs)

    def __str__(self):
        return f'[{self.pk}] {self.alumno.no_control} en {self.periodo}'

    class Meta:
        abstract = True
        constraints = [
            models.UniqueConstraint(fields=['alumno', 'periodo'], name='unique_%(class)s', violation_error_message='Ya existe un registro con este periodo para el alumno')
        ]

class Ingreso(BaseRegistro):
    class TiposIngresos(models.TextChoices):
        EXAMEN = 'EX', 'Examen'
        EQUIVALENCIA = 'EQ', 'Equivalencia'
        TRASLADO = 'TR', 'Traslado'
        CONVALIDACION = 'CO', 'Convalidación'
        REINGRESO = 'RE', 'Reingreso'

    tipo = models.CharField(max_length=2, choices=TiposIngresos.choices, default=TiposIngresos.EXAMEN, null=False, blank=False)
    num_semestre = models.PositiveIntegerField(null=False, blank=False, validators=[MinValueValidator(1), MaxValueValidator(16)])

    def clean(self):
        exclusive_tipos = [
            Ingreso.TiposIngresos.EXAMEN.value,
            Ingreso.TiposIngresos.EQUIVALENCIA.value,
            Ingreso.TiposIngresos.TRASLADO.value,
            Ingreso.TiposIngresos.CONVALIDACION.value
        ]
        if self.tipo in exclusive_tipos and Ingreso.objects.filter(alumno=self.alumno, tipo__in=exclusive_tipos).count() >= 1:
            raise ValidationError({'tipo': 'Solo puede existir un ingreso de EXAMEN, EQUIVALENCIA, TRASLADO o CONVALIDACION'})
        if Egreso.objects.filter(alumno=self.alumno).exists():
            raise ValidationError('No se puede registrar un ingreso para un alumno egresado')

    def calcular_num_semestre(self):
        if self.num_semestre is None:
            # asignar numero de semestre por calculo de periodo
            primer_ingreso = Ingreso.objects.filter(alumno=self.alumno).order_by('periodo').first()
            if primer_ingreso is not None:
                self.num_semestre = getNumSemestre(primer_ingreso.periodo, primer_ingreso.num_semestre, self.periodo)
            else:
                self.num_semestre = 1

    def save(self, *args, **kwargs):
        super(Ingreso, self).save(*args, **kwargs)

    class Meta(BaseRegistro.Meta):
        constraints = BaseRegistro.Meta.constraints.copy()
        constraints += [
            models.UniqueConstraint(
                fields=['alumno', 'num_semestre'], 
                name='unique_num_semestre', 
                violation_error_message='Ya se tiene un ingreso con este número de semestre'
            )
        ]
        # Añadir índices manteniendo las restricciones existentes
        indexes = [
            models.Index(fields=['alumno', 'periodo'], name='idx_ingreso_alumno_periodo'),
            models.Index(fields=['periodo', 'tipo'], name='idx_ingreso_periodo_tipo'),
            models.Index(fields=['tipo'], name='idx_ingreso_tipo')
        ]


class Egreso(BaseRegistro):
    def clean(self):
        if not self.pk and Egreso.objects.filter(alumno=self.alumno).exists():
            raise ValidationError('Solo puede existir un egreso por alumno')

        ultimo_ingreso = Ingreso.objects.filter(alumno=self.alumno).order_by('periodo').last()
        if ultimo_ingreso is None or ultimo_ingreso.periodo > self.periodo:
            raise ValidationError({'periodo': 'El periodo de egreso debe ser igual o mayor al del último ingreso'})

    def save(self, *args, **kwargs):
        self.clean()
        super(Egreso, self).save(*args, **kwargs)

class Titulacion(BaseRegistro):
    class TiposTitulaciones(models.TextChoices):
        TESIS = 'TE', 'Tesis'
        RESIDENCIA = 'RE', 'Residencia'
        DUAL = 'DU', 'Modelo Dual'

    tipo = models.CharField(max_length=2, choices=TiposTitulaciones.choices, default=TiposTitulaciones.RESIDENCIA, null=False, blank=False)

    def clean(self):
        if not self.pk and Titulacion.objects.filter(alumno=self.alumno).exists():
            raise ValidationError('Solo puede existir una titulacion por alumno')
        try:
            egreso = Egreso.objects.get(alumno=self.alumno)
            if egreso.periodo > self.periodo:
                raise ValidationError({'periodo': 'El periodo de titulación debe ser igual o mayor al de egreso'})
        except Egreso.DoesNotExist:
            raise ValidationError('No se puede crear una titulacion sin un egreso existente')

    def save(self, *args, **kwargs):
        self.clean()
        super(Titulacion, self).save(*args, **kwargs)

    class Meta(BaseRegistro.Meta):
        verbose_name = 'titulación'
        verbose_name_plural = 'titulaciones'

class LiberacionIngles(BaseRegistro):
    def clean(self):
        if not self.pk and LiberacionIngles.objects.filter(alumno=self.alumno).exists():
            raise ValidationError('Solo puede existir una liberación de inglés por alumno')

    def save(self, *args, **kwargs):
        self.clean()
        super(LiberacionIngles, self).save(*args, **kwargs)

    class Meta(BaseRegistro.Meta):
        verbose_name = 'liberación de inglés'
        verbose_name_plural = 'liberaciones de inglés'

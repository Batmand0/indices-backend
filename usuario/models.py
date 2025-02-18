from django.contrib.auth.models import AbstractUser
from django.core.exceptions import ValidationError
from django.utils.translation import gettext_lazy as _
from django.db import models
import re
from django.dispatch import receiver
from django.urls import reverse
from django_rest_passwordreset.signals import reset_password_token_created
from django.core.mail import send_mail, EmailMessage

class Usuario(AbstractUser):
    class Gender(models.TextChoices):
        MALE = 'H', _('Hombre')
        FEMALE = 'M', _('Mujer')
        OTHER = 'X', _('Otro')

    def validate_name(value):
        match = re.search(r'^[A-ZÁÉÍÓÚÑÄËÏÖÜ ]+$', value.upper())
        if not match:
            raise ValidationError(
                _('Enter a valid name'),
                params={'value': value},
            )

    first_name = models.CharField(max_length=150, null=False, blank=False, validators=[validate_name])
    paternal_surname = models.CharField(max_length=150, null=False, blank=False, validators=[validate_name])
    maternal_surname = models.CharField(max_length=150, null=True, blank=True, validators=[validate_name])
    email = models.EmailField(null=False, blank=False, unique=True)
    gender = models.CharField(choices=Gender.choices, default=Gender.OTHER, max_length=1, null=False)

    REQUIRED_FIELDS = [
        'first_name',
        'paternal_surname',
        'email',
    ]

    def save(self, *args, **kwargs):
        self.first_name = self.first_name.upper()
        self.paternal_surname = self.paternal_surname.upper()
        self.maternal_surname = self.maternal_surname.upper() if self.maternal_surname else None
        self.email = self.email.lower()
        super().save(*args, **kwargs)

    def __str__(self):
        return f'{self.username}'

    class Meta:
        permissions = [
            ('view_tablas_poblacion', 'Puede consultar Tablas de Poblacion'),
            ('view_tablas_crecimiento', 'Puede consultar Tablas de Crecimiento'),
            ('view_indices_permanencia', 'Puede consultar Indices de Permanencia'),
            ('view_indices_egreso', 'Puede consultar Indices de Egreso'),
            ('view_indices_titulacion', 'Puede consultar Indices de Titulacion'),
            ('view_indices_desercion', 'Puede consultar Indices de Desercion'),
            ('view_reportes_nuevo_ingreso', 'Puede consultar Reportes de Nuevo Ingreso'),
            ('view_reportes_egresados', 'Puede consultar Reportes de Egresados'),
            ('view_reportes_titulados', 'Puede consultar Reportes de Titulados'),
            ('view_cedulas_cacei', 'Puede consultar Cedulas de CACEI'),
            ('view_cedulas_caceca', 'Puede consultar Cedulas de CACECA'),
        ]

    @receiver(reset_password_token_created)
    def password_reset_token_created(sender, instance, reset_password_token, *args, **kwargs):
        # the below like concatinates your websites reset password url and the reset email token which will be required at a later stage
        email_plaintext_message = "Da clic en el enlace para restablecer tu contraseña" + " " + "{}{}".format(instance.request.build_absolute_uri("http://localhost:3000/restablecer-contrasena/"), reset_password_token.key)
        """
            this below line is the django default sending email function,
            takes up some parameter (title(email title), message(email body), from(email sender), to(recipient(s))
        """
        send_mail(
            # title:
            "Restablecer contraseña para {title}".format(title="Crediation portal account"),
            # message:
            email_plaintext_message,
            # from:
            "1202julianlopez@gmail.com",
            # to:
            [reset_password_token.user.email],
            fail_silently=False,
        )
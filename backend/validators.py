from django.core.exceptions import ValidationError
import re

class LowercaseUppercasePasswordValidator:
    def validate(self, password, user=None):
        match = re.search("^(?=.*[a-z])(?=.*[A-Z])(?=.*[a-zA-Z]).*$", password)
        if not match:
            raise ValidationError(message="La contraseña debe contener 1 letra minúscula y 1 letra mayúscula.")

    def get_help_text(self):
        return "Debe contener 1 letra minúscula y 1 letra mayúscula."

class ContainsNumberPasswordValidator:
    def validate(self, password, user=None):
        match = re.search("^(?=.*\d).*$", password)
        if not match:
            raise ValidationError(message="La contraseña debe contener 1 número.")

    def get_help_text(self):
        return "Debe contener 1 número."

class SpecialCharacterValidator:
    def validate(self, password, user=None):
        match = re.search("^(?=.*[¡!@#$%^&*\(\)\-_=+¿?<>,.:;'\"\[\]{}/\\`~]).*$", password)
        if not match:
            raise ValidationError(message="La contraseña debe contener 1 carácter especial.")

    def get_help_text(self):
        return "Debe contener 1 carácter especial."
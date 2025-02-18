from django.utils.timezone import now
import math

# Calcula el periodo correspondiente a un cohorte tras una cantidad de semestres
# Devuelve un string en formato [0-9]{4}[13]
def calcularPeriodo(cohorte: str, semestres: int):
    if semestres == 1:
        return cohorte
    else:
        anualidad = int(cohorte[:4])
        semestre_inicial = int(cohorte[4:])

        if semestre_inicial == 1:
            ajuste = (semestres/2) - semestre_inicial
            anualidad = anualidad + math.ceil(ajuste)
            semestre = 3 if (ajuste - math.floor(ajuste)) == 0 else 1
        else:
            ajuste = (semestres/2)
            anualidad = anualidad + math.floor(ajuste)
            semestre = 1 if (ajuste - math.floor(ajuste)) == 0 else 3

        periodo = str(anualidad) + str(semestre)
        return periodo

# Calcula todos los periodos de un cohorte
# Devuelve una lista de strings
def calcularPeriodos(cohorte: str, semestres: int):
    periodos = []
    for i in range(semestres):
        periodo = calcularPeriodo(cohorte, i+1)
        periodos.append(periodo)
    return periodos

# Devuelve el periodo correspondiente a la fecha actual
# Ene-Jul ----> XXXX1
# Ago-Dic ----> XXXX3
def getPeriodoActual():
    date = now().today()
    semestre = 1 if date.month < 8 else 3
    return f"{date.year}{semestre}"

# Calcula la diferencia entre dos periodos, se ajusta de acuerdo a un numero de semestre inicial
def getNumSemestre(primer_periodo: str, primer_num_semestre: int, nuevo_periodo: str):
    # divide la anualidad y semestre
    primer_tuple = (int(primer_periodo[:4]), int(primer_periodo[4:]))
    nuevo_tuple = (int(nuevo_periodo[:4]), int(nuevo_periodo[4:]))
    if nuevo_tuple[0] < primer_tuple[0]:
        return None

    dif_anualidad = nuevo_tuple[0] - primer_tuple[0]
    # cada año es una diferencia de dos semestres
    num_semestre = primer_num_semestre + (dif_anualidad * 2)
    # ajustar por diferencia en semestre
    if nuevo_tuple[1] > primer_tuple[1]:
        num_semestre += 1
    elif nuevo_tuple[1] < primer_tuple[1]:
        num_semestre -= 1
    return num_semestre
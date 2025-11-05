"""
Utilidades para cálculo de semáforos
"""
from config import SEMAFORO_VERDE, SEMAFORO_AMARILLO


def calcular_estado_semaforo(porcentaje: float) -> str:
    """
    Calcula el estado del semáforo según el porcentaje de cobertura.
    """
    if porcentaje >= SEMAFORO_VERDE * 100:
        return "VERDE"
    elif porcentaje >= SEMAFORO_AMARILLO * 100:
        return "AMARILLO"
    else:
        return "ROJO"


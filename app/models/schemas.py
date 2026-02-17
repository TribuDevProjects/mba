from pydantic import BaseModel


class ClientMBARequest(BaseModel):
    """Request para endpoints por cliente.
    
    Cada cliente tiene su endpoint dedicado (/mba/{customer}/).
    Búsqueda parcial case-insensitive por defecto.
    Soporta múltiples productos separados por coma.
    
    Examples:
        {"product": "diablo"}
        {"product": "papas, burger, malteada"}
    """
    product: str


class AssociationRule(BaseModel):
    lhs: list[str]
    rhs: list[str]
    support: float
    confidence: float
    lift: float
    leverage: float
    conviction: float
    antecedent_support: float
    consequent_support: float
    zhangs_metric: float

from pydantic import BaseModel


class MBARequest(BaseModel):
    customer: str
    product: str


class ClientMBARequest(BaseModel):
    """Request para endpoints por cliente (no necesita campo customer)."""
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

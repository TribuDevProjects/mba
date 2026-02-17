import logging

from fastapi import APIRouter, HTTPException, status, Depends

from app.dependencies import verify_token
from app.clients import ALL_CLIENTS

logger = logging.getLogger(__name__)
router = APIRouter()

_CLIENT_MAP = {c.CUSTOMER_NAME: c for c in ALL_CLIENTS}


@router.get("/mba")
def mba_info(_: None = Depends(verify_token)):
    """Información sobre clientes disponibles y cómo usar el API."""
    return {
        "message": "Market Basket Analysis API",
        "available_customers": list(_CLIENT_MAP.keys()),
        "endpoints": {
            client_name: f"/mba/{client_name}/" 
            for client_name in _CLIENT_MAP.keys()
        },
        "usage": {
            "method": "POST",
            "endpoint": "/mba/{customer}/",
            "headers": {"Authorization": "Bearer {token}"},
            "body": {"product": "product_name or partial_name"},
            "examples": [
                {"product": "diablo"},
                {"product": "papas, burger"},
            ]
        },
        "features": [
            "Búsqueda parcial case-insensitive",
            "Soporte para múltiples productos (separados por coma)",
            "Limpieza automática de datos nulos",
        ]
    }

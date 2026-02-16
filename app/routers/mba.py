import logging

from fastapi import APIRouter, HTTPException, status, Depends

from app.dependencies import verify_token
from app.models.schemas import MBARequest
from app.clients import ALL_CLIENTS
from app.services.market_basket import run_mba_pipeline

logger = logging.getLogger(__name__)
router = APIRouter()

_CLIENT_MAP = {c.CUSTOMER_NAME: c for c in ALL_CLIENTS}


@router.get("/mba")
def mba_info(_: None = Depends(verify_token)):
    return {
        "message": "La solicitud POST que deberias enviar es:",
        "single_product": {"customer": "customer", "product": "product_name"},
        "multi_product": {"customer": "customer", "product": "product_name_1, product_name_2"},
        "available_customers": list(_CLIENT_MAP.keys()),
        "note": "Prefiere usar /mba/{customer} directamente. Autenticación requerida: Bearer token en header.",
    }


@router.post("/mba")
def mba_analysis(request: MBARequest, _: None = Depends(verify_token)):
    client = _CLIENT_MAP.get(request.customer.lower())
    if client is None:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"Customer '{request.customer}' no encontrado. Disponibles: {list(_CLIENT_MAP.keys())}",
        )

    product_names = [p.strip() for p in request.product.split(",")]
    logger.info("El basket a buscar es: %s", product_names)

    config = client.get_config()
    rules = run_mba_pipeline(
        product_names=product_names,
        query=config.query,
        db_url=config.db_url,
        min_support=config.min_support,
        transform_fn=client.transform_data,
    )

    if rules is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Algunos productos no se encontraron en la base de datos",
        )
    return rules

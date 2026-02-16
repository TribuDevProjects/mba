import logging
from dataclasses import dataclass

import pandas as pd
from fastapi import APIRouter, HTTPException, status, Depends

from app.dependencies import verify_token
from app.models.schemas import ClientMBARequest
from app.services.market_basket import run_mba_pipeline
from app.logger import get_client_logger, set_client_context, clear_client_context

logger = logging.getLogger(__name__)


@dataclass
class ClientConfig:
    name: str
    db_url: str
    min_support: float
    query: str


class BaseClient:
    """
    Base para adaptadores de clientes.
    Subclases deben sobreescribir get_config() para definir credenciales y query.
    Opcionalmente sobreescriben transform_data() para limpieza custom.
    """

    CUSTOMER_NAME: str = ""

    def __init__(self):
        self.router = APIRouter(
            prefix=f"/mba/{self.CUSTOMER_NAME}", tags=[self.CUSTOMER_NAME]
        )
        self._register_routes()

    def get_config(self) -> ClientConfig:
        """Cada cliente sobreescribe este metodo con sus credenciales y query."""
        raise NotImplementedError("Cada cliente debe implementar get_config()")

    def transform_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Sobreescribir para aplicar limpieza/transformacion antes del MBA."""
        return df

    def _register_routes(self):
        client = self

        @self.router.get("/")
        def info(_: None = Depends(verify_token)):
            return {
                "message": "La solicitud POST que deberias enviar es:",
                "body": {"product": "product_name_1, product_name_2"},
                "customer": client.CUSTOMER_NAME,
                "auth": "Bearer token en Authorization header",
            }

        @self.router.post("/")
        def analyze(request: ClientMBARequest, _: None = Depends(verify_token)):
            # Establecer el contexto del cliente para todos los logs
            set_client_context(client.CUSTOMER_NAME)
            
            try:
                product_names = [p.strip() for p in request.product.split(",")]
                
                client_logger = get_client_logger(client.CUSTOMER_NAME)
                client_logger.info("Basket a buscar: %s", product_names)

                config = client.get_config()
                rules = run_mba_pipeline(
                    product_names=product_names,
                    query=config.query,
                    db_url=config.db_url,
                    min_support=config.min_support,
                    transform_fn=client.transform_data,
                    client_logger=client_logger,
                )

                if rules is None:
                    raise HTTPException(
                        status_code=status.HTTP_404_NOT_FOUND,
                        detail="Algunos productos no se encontraron en la base de datos",
                    )
                return rules
            finally:
                # Limpiar el contexto del cliente
                clear_client_context()

import logging

from fastapi import FastAPI

from app.routers import mba
from app.clients import ALL_CLIENTS
from app.logger import ClientFormatter

# Configurar el formatter personalizado
handler = logging.StreamHandler()
handler.setFormatter(ClientFormatter())

logging.basicConfig(
    level=logging.INFO,
    handlers=[handler],
)


def create_app() -> FastAPI:
    application = FastAPI(title="MBA API", version="2.0.0")

    # Backward-compatible /mba endpoint
    application.include_router(mba.router)

    # Per-client endpoints: /mba/carlsjr, /mba/karzo, etc.
    for client in ALL_CLIENTS:
        application.include_router(client.router)

    @application.get("/health")
    def health():
        return {"status": "ok"}

    return application


app = create_app()

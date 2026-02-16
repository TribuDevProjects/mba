import logging
from contextvars import ContextVar

# Variable de contexto para mantener el cliente actual
current_client: ContextVar[str] = ContextVar("current_client", default="")

# Códigos de color ANSI
COLORS = {
    "carlsjr": "\033[96m",      # Cyan
    "multicarnes": "\033[95m",  # Magenta
    "karzo": "\033[93m",        # Yellow
    "cypress": "\033[92m",      # Green
    "reset": "\033[0m",         # Reset
}

# Colores para niveles de log
LEVEL_COLORS = {
    "WARNING": "\033[93m",  # Yellow
    "ERROR": "\033[91m",    # Red
    "CRITICAL": "\033[91m", # Red
    "DEBUG": "\033[90m",    # Gray
}


class ClientFormatter(logging.Formatter):
    """Formatter personalizado que agrega el prefijo del cliente a todos los logs."""
    
    def format(self, record):
        msg = record.getMessage()
        client_name = current_client.get("")
        reset = COLORS["reset"]
        
        if client_name:
            client_upper = client_name.upper()
            color = COLORS.get(client_name.lower(), "")
            
            # Detectar si el mensaje ya tiene el prefijo del cliente (con color)
            # Esto evita duplicación cuando viene de ClientLoggerAdapter
            if msg.startswith(f"{color}{client_upper}:{reset}"):
                # Ya tiene el prefijo, no duplicar
                if record.levelno == logging.INFO:
                    return msg
                else:
                    # Para otros niveles, agregar nivel con color antes
                    level_color = LEVEL_COLORS.get(record.levelname, "")
                    return f"{level_color}{record.levelname}{reset} - {msg}"
            
            # Para nivel INFO, solo mostrar CLIENTE: mensaje
            if record.levelno == logging.INFO:
                return f"{color}{client_upper}:{reset} {msg}"
            # Para otros niveles, incluir el nivel con color: NIVEL - CLIENTE: mensaje
            else:
                level_color = LEVEL_COLORS.get(record.levelname, "")
                return f"{level_color}{record.levelname}{reset} - {color}{client_upper}:{reset} {msg}"
        
        # Si no hay cliente en contexto, usar formato estándar
        if record.levelno == logging.INFO:
            return f"INFO: {msg}"
        else:
            level_color = LEVEL_COLORS.get(record.levelname, "")
            return f"{level_color}{record.levelname}{reset}: {msg}"


class ClientLoggerAdapter(logging.LoggerAdapter):
    """Adapter que agrega el prefijo del cliente con color a todos los mensajes."""

    def process(self, msg, kwargs):
        client_name = self.extra.get("client_name", "").lower()
        client_upper = client_name.upper()
        color = COLORS.get(client_name, "")
        reset = COLORS["reset"]
        
        return f"{color}{client_upper}:{reset} {msg}", kwargs


def get_client_logger(client_name: str) -> ClientLoggerAdapter:
    """Crea un logger con prefijo de cliente (ej: 'CARLSJR:', 'MULTICARNES:')"""
    base_logger = logging.getLogger("mba.client")
    return ClientLoggerAdapter(base_logger, {"client_name": client_name})


def set_client_context(client_name: str):
    """Establece el contexto del cliente para todos los logs subsecuentes."""
    current_client.set(client_name)


def clear_client_context():
    """Limpia el contexto del cliente."""
    current_client.set("")

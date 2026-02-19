from prefect.variables import Variable

from app.clients.base import BaseClient, ClientConfig


class MulticarnesClient(BaseClient):
    CUSTOMER_NAME = "multicarnes"

    QUERY = """
        SELECT id_ecommerce_user as customer_id, eo.id as order_id, eop.name as product_name
        FROM multicarnes.ecommerce_orders AS eo
        LEFT JOIN multicarnes.ecommerce_order_products AS eop on eo.id = eop.id_ecommerce_order
    """

    def get_config(self) -> ClientConfig:
        credentials = Variable.get("multicarnes_warehouse")
        return ClientConfig(
            name=self.CUSTOMER_NAME,
            db_url=(
                f"mysql+mysqlconnector://"
                f"{credentials['DB_USER']}:{credentials['DB_PASS']}@"
                f"{credentials['DB_HOST']}/{credentials['DB_DATABASE']}"
            ),
            min_support=0.08,
            query=self.QUERY,
        )


client = MulticarnesClient()

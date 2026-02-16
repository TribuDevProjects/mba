from prefect.variables import Variable

from app.clients.base import BaseClient, ClientConfig


class KarzoClient(BaseClient):
    CUSTOMER_NAME = "karzo"

    QUERY = """
        SELECT o.client_id as customer_id, o.id as order_id, p.name as product_name
        FROM karzo.orders as o
        LEFT JOIN karzo.order_products op on o.id = op.order_id
        LEFT JOIN karzo.products as p on p.id = op.product_id
    """

    def get_config(self) -> ClientConfig:
        credentials = Variable.get("karzo")
        return ClientConfig(
            name=self.CUSTOMER_NAME,
            db_url=(
                f"mysql+mysqlconnector://"
                f"{credentials['DB_USER']}:{credentials['DB_PASS']}@"
                f"{credentials['DB_HOST']}/{credentials['DB_DATABASE']}"
            ),
            min_support=0.001,
            query=self.QUERY,
        )


client = KarzoClient()

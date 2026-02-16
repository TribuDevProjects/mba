from prefect.variables import Variable

from app.clients.base import BaseClient, ClientConfig


class CarlsJrClient(BaseClient):
    CUSTOMER_NAME = "carlsjr"

    QUERY = """
        SELECT customer_id, order_id, pd.product_name as product_name
        FROM carlsjr_warehouse.warehouse_orders AS wo
        LEFT JOIN carlsjr_warehouse.warehouse_products AS wp ON wo.upc = wp.upc
        LEFT JOIN carlsjr_warehouse.products_dimension AS pd ON wp.id = pd.id
    """

    def get_config(self) -> ClientConfig:
        credentials = Variable.get("carlsjr_warehouse")
        return ClientConfig(
            name=self.CUSTOMER_NAME,
            db_url=(
                f"mysql+mysqlconnector://"
                f"{credentials['DB_USER']}:{credentials['DB_PASS']}@"
                f"{credentials['DB_HOST']}/{credentials['DB_DATABASE']}"
            ),
            min_support=0.01,
            query=self.QUERY,
        )

client = CarlsJrClient()
from prefect.variables import Variable

from app.clients.base import BaseClient, ClientConfig


class CypressClient(BaseClient):
    CUSTOMER_NAME = "cypress"

    QUERY = """
        SELECT do.customer_id, do.id as order_id, dp.name as product_name
        FROM deliverect_orders as do
        LEFT JOIN deliverect_order_products as dop on do.id = dop.deliverect_order_id
        LEFT JOIN deliverect_menu_location_products as dmlp on dmlp.id = dop.deliverect_menu_location_product_id
        LEFT JOIN deliverect_products as dp on dmlp.deliverect_product_id = dp.id
        WHERE dp.name IS NOT NULL
    """

    def get_config(self) -> ClientConfig:
        credentials = Variable.get("cypress")
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


client = CypressClient()

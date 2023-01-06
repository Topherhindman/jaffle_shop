def model(dbt, session):

    orders_renames = {"id": "order_id", "user_id": "customer_id"}

    raw_orders = dbt.ref("raw_orders").to_pandas()

    orders = raw_orders.rename(columns=orders_renames)

    return orders
def model(dbt, session):

    customers_renames = {"total_amount": "customer_lifetime_value"}

    stg_orders = dbt.ref("stg_orders").to_pandas()
    stg_payments = dbt.ref("stg_payments").to_pandas()
    stg_customers = dbt.ref("stg_customers").to_pandas()

    customer_orders = (
        stg_orders.groupby("customer_id")
        .agg(
            first_order=("order_date", "min"),
            most_recent_order=("order_date", "max"),
            number_of_orders=("order_id", "count"),
        )
        .reset_index()
    )

    customer_payments = (
        stg_payments.merge(stg_orders, on="order_id", how="left")
        .groupby("customer_id")
        .agg(total_amount=("amount", "sum"))
        .reset_index()
    )

    customers = (
        stg_customers.merge(customer_orders, on="customer_id", how="left")
        .merge(customer_payments, on="customer_id", how="left")
        .rename(columns=customers_renames)
    )

    return customers
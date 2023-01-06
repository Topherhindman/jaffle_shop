def model(dbt, session):

    payment_methods = ["credit_card", "coupon", "bank_transfer", "gift_card"]

    orders_renames = {"total_amount": "amount"}
    orders_payments_renames = {
        f"{payment_method}": f"{payment_method}_amount"
        for payment_method in payment_methods
    }

    stg_orders = dbt.ref("stg_orders").to_pandas()
    stg_payments = dbt.ref("stg_payments").to_pandas()

    orders_payments_totals = stg_payments.groupby("order_id").agg(
        amount=("amount", "sum")
    )

    orders_payments = (
        stg_payments.groupby(["order_id", "payment_method"])
        .agg(payment_method_amount=("amount", "sum"))
        .reset_index()
        .pivot(
            index="order_id",
            columns="payment_method",
            values="payment_method_amount",
        )
        .rename(columns=orders_payments_renames)
        .merge(orders_payments_totals, on="order_id", how="left")
        .reset_index()
    )

    orders = stg_orders.merge(orders_payments, on="order_id", how="left").rename(
        columns=orders_renames
    )
    orders = orders.fillna(0)  # hacked the mainframe (fixes tests)

    return orders
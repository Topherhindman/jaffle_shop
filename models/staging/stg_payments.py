def model(dbt, session):

    payments_renames = {"id": "payment_id"}

    raw_payments = dbt.ref("raw_payments").to_pandas()

    payments = raw_payments.rename(columns=payments_renames)
    # -- `amount` is currently stored in cents, so we convert it to dollars
    payments["amount"] /= 100

    return payments
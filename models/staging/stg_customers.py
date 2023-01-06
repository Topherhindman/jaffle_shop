def model(dbt, session):

    customers_renames = {"id": "customer_id"}

    stg_customers = dbt.ref("raw_customers").to_pandas()

    stg_customers = stg_customers.rename(columns=customers_renames)

    return stg_customers
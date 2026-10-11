from pyspark.sql.functions import *
import pyspark.pipelines as dp

# ------------------------------ Silver Cleaning -------------------------------

# ==============================================================================
# 1. CUSTOMERS
# ==============================================================================
@dp.view(name="silver_customers_validated")
def silver_customers_validated():
    df_customers = spark.readStream.table("dev.bronze.bronze_customers")
    
    return (
        df_customers
        .dropna(how='all')
        .drop("file_name", "file_path", "ingestion_timestamp", "_rescued_data")
        .withColumns({
            "customer_id": trim(col("customer_id")),
            "first_name": trim(col("first_name")),
            "last_name": trim(col("last_name")),
            "email": trim(col("email")),
            "address": trim(col("address")),
            "city": trim(col("city")),
            "state": trim(col("state")),
            "customer_segment": trim(col("customer_segment")),
            "customer_status": trim(col("customer_status")),
            "registration_date": coalesce(
                try_to_date(col("registration_date"), "yyyy-MM-dd"),
                try_to_date(col("registration_date"), "MM/dd/yyyy"),
                try_to_date(col("registration_date"), "yyyy/MM/dd"),
                try_to_date(col("registration_date"), "MM-dd-yyyy"),
                try_to_date(col("registration_date"), "MMM d, yyyy"),
                try_to_date(col("registration_date"), "dd-MM-yy")
            ),
            "updated_at": coalesce(
                try_to_date(col("updated_at"), "yyyy-MM-dd"),
                try_to_date(col("updated_at"), "MM/dd/yyyy"),
                try_to_date(col("updated_at"), "yyyy/MM/dd"),
                try_to_date(col("updated_at"), "MM-dd-yyyy"),
                try_to_date(col("updated_at"), "MMM d, yyyy"),
                try_to_date(col("updated_at"), "dd-MM-yy")
            ),
            "date_of_birth": coalesce(
                try_to_date(col("date_of_birth"), "yyyy-MM-dd"),
                try_to_date(col("date_of_birth"), "MM/dd/yyyy"),
                try_to_date(col("date_of_birth"), "yyyy/MM/dd"),
                try_to_date(col("date_of_birth"), "MM-dd-yyyy"),
                try_to_date(col("date_of_birth"), "MMM d, yyyy"),
                try_to_date(col("date_of_birth"), "dd-MM-yy")
            )
        })
        .withColumn(
            "quarantine_reason",
            when(
                ~col("email").rlike(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,10}$"), 
                "Invalid email format"
            ).otherwise(lit(None))
        )
    )

# --- Silver Staging (Valid Data) ---
@dp.view(name="silver_customers_staging")
def silver_customers_staging():
    return (
        spark.readStream.table("silver_customers_validated")
        .filter(col("quarantine_reason").isNull())
        .drop("quarantine_reason")
        .dropDuplicates()
    )

# --- Quarantine Table (Invalid Data) ---
@dp.table(name="dev.silver.quarantine_customers")
def quarantine_customers():
    return (
        spark.readStream.table("silver_customers_validated")
        .filter(col("quarantine_reason").isNotNull())
    )

# --- SCD Type 2 Auto CDC Flow ---
dp.create_streaming_table(name="dev.silver.silver_customers")
dp.create_auto_cdc_flow(
    target="dev.silver.silver_customers",
    source="silver_customers_staging",
    keys=["customer_id"],
    sequence_by=col("updated_at"),
    stored_as_scd_type=2
)


# ==============================================================================
# 2. BRANCHES
# ==============================================================================
@dp.view(name="silver_branches_validated")
def silver_branches_validated():
    df_branches = spark.readStream.table("dev.bronze.bronze_branches")
    
    return (
        df_branches
        .dropna(how='all')
        .drop("file_name", "file_path", "ingestion_timestamp", "_rescued_data")
        .withColumns({
            "branch_id": trim(col("branch_id")),
            "branch_code": trim(col("branch_code")),
            "branch_name": trim(col("branch_name")),
            "city": trim(col("city")),
            "state": trim(col("state")),
            "region": lower(trim(col("region"))),
            "branch_type": lower(trim(col("branch_type"))),
            "manager_name": trim(col("manager_name")),
            "branch_status": lower(trim(col("branch_status"))),
            "opening_date": coalesce(
                try_to_date(col("opening_date"), "yyyy-MM-dd"),
                try_to_date(col("opening_date"), "MM/dd/yyyy"),
                try_to_date(col("opening_date"), "yyyy/MM/dd"),
                try_to_date(col("opening_date"), "MM-dd-yyyy"),
                try_to_date(col("opening_date"), "MMM d, yyyy"),
                try_to_date(col("opening_date"), "dd-MM-yy")
            )
        })
        .withColumn(
            "quarantine_reason",
            when(~col("region").isin(["east", "west", "north", "south", "central"]), "Invalid region")
            .when(~col("branch_status").isin(["active", "closed"]), "Invalid branch_status")
            .when(~col("branch_type").isin(["metro", "urban", "semi-urban", "rural"]), "Invalid branch_type")
            .otherwise(lit(None))
        )
    )

# --- Silver Table (Valid Data) ---
@dp.table(name="dev.silver.silver_branches")
def silver_branches():
    return (
        spark.readStream.table("silver_branches_validated")
        .filter(col("quarantine_reason").isNull())
        .drop("quarantine_reason")
        .dropDuplicates(["branch_id"])
    )

# --- Quarantine Table (Invalid Data) ---
@dp.table(name="dev.silver.quarantine_branches")
def quarantine_branches():
    return (
        spark.readStream.table("silver_branches_validated")
        .filter(col("quarantine_reason").isNotNull())
    )


# ==============================================================================
# 3. ACCOUNTS
# ==============================================================================
@dp.view(name="silver_accounts_validated")
def silver_accounts_validated():
    df_accounts = spark.readStream.table("dev.bronze.bronze_accounts")
    
    return (
        df_accounts
        .dropna(how="all")
        .drop("file_name", "file_path", "ingestion_timestamp", "_rescued_data")
        .withColumns({
            "account_id": trim(col("account_id")),
            "customer_id": trim(col("customer_id")),
            "branch_id": trim(col("branch_id")),
            "account_type": lower(trim(col("account_type"))),
            "account_status": lower(trim(col("account_status"))),
            "currency": trim(col("currency")),
            "account_tier": trim(col("account_tier")),
            "opening_date": coalesce(
                try_to_date(col("opening_date"), "yyyy-MM-dd"),
                try_to_date(col("opening_date"), "MM/dd/yyyy"),
                try_to_date(col("opening_date"), "yyyy/MM/dd"),
                try_to_date(col("opening_date"), "MM-dd-yyyy"),
                try_to_date(col("opening_date"), "MMM d, yyyy"),
                try_to_date(col("opening_date"), "dd-MM-yy")
            ),
            "closing_date": coalesce(
                try_to_date(col("closing_date"), "yyyy-MM-dd"),
                try_to_date(col("closing_date"), "MM/dd/yyyy"),
                try_to_date(col("closing_date"), "yyyy/MM/dd"),
                try_to_date(col("closing_date"), "MM-dd-yyyy"),
                try_to_date(col("closing_date"), "MMM d, yyyy"),
                try_to_date(col("closing_date"), "dd-MM-yy")
            ),          
            "updated_at": coalesce(
                try_to_date(col("updated_at"), "yyyy-MM-dd"),
                try_to_date(col("updated_at"), "MM/dd/yyyy"),
                try_to_date(col("updated_at"), "yyyy/MM/dd"),
                try_to_date(col("updated_at"), "MM-dd-yyyy"),
                try_to_date(col("updated_at"), "MMM d, yyyy"),
                try_to_date(col("updated_at"), "dd-MM-yy")
            ) 
        })
        .withColumn(
            "quarantine_reason",
            when(~col("account_status").isin(["active", "dormant", "closed"]), "Invalid account_status")
            .when(~col("account_type").isin(["savings", "business", "current", "salary"]), "Invalid account_type")
            .otherwise(lit(None))
        )
    )

# --- Silver Table (Valid Data + Lookups) ---
@dp.table(name="dev.silver.silver_accounts")
def silver_accounts():
    df_silver_customers = spark.readStream.table("dev.silver.silver_customers")
    df_silver_branches = spark.readStream.table("dev.silver.silver_branches")

    current_silver_customer = df_silver_customers.filter(col("__END_AT").isNull())

    return (
        spark.readStream.table("silver_accounts_validated")
        .filter(col("quarantine_reason").isNull())
        .drop("quarantine_reason")
        .dropDuplicates(["account_id"])
        # Inner joins act as a referential integrity check. Missing FKs will drop here.
        .join(current_silver_customer.select("customer_id"), on="customer_id", how="inner")
        .join(df_silver_branches.select("branch_id"), on="branch_id", how="inner")
    )

# --- Quarantine Table (Invalid Data) ---
@dp.table(name="dev.silver.quarantine_accounts")
def quarantine_accounts():
    return (
        spark.readStream.table("silver_accounts_validated")
        .filter(col("quarantine_reason").isNotNull())
    )


# ==============================================================================
# 4. TRANSACTIONS
# ==============================================================================
@dp.view(name="silver_transactions_validated")
def silver_transactions_validated():
    df_transactions = spark.readStream.table("dev.bronze.bronze_transactions")
    
    return (
        df_transactions
        .dropna(how="all")
        .drop("file_name", "file_path", "ingestion_timestamp", "_rescued_data")
        .withColumns({
            "transaction_id": trim(col("transaction_id")),
            "account_id": trim(col("account_id")),
            "transaction_type": trim(col("transaction_type")),
            "transaction_channel": trim(col("transaction_channel")),
            "currency": trim(col("currency")),
            "merchant_name": trim(col("merchant_name")),
            "merchant_category": trim(col("merchant_category")),
            "transaction_status": lower(trim(col("transaction_status"))),
            "reference_number": trim(col("reference_number")),
            "transaction_timestamp": coalesce(
                try_to_timestamp(col("transaction_timestamp"), lit("yyyy-MM-dd HH:mm:ss")),
                try_to_timestamp(col("transaction_timestamp"), lit("MM/dd/yyyy HH:mm:ss")),
                try_to_timestamp(col("transaction_timestamp"), lit("yyyy/MM/dd HH:mm:ss")),
                try_to_timestamp(col("transaction_timestamp"), lit("MM-dd-yyyy HH:mm:ss")),
                try_to_timestamp(col("transaction_timestamp"), lit("MMM d, yyyy HH:mm:ss")),
                try_to_timestamp(col("transaction_timestamp"), lit("dd-MM-yy HH:mm:ss"))
            ),
            "created_at": coalesce(
                try_to_timestamp(col("created_at"), lit("yyyy-MM-dd HH:mm:ss")),
                try_to_timestamp(col("created_at"), lit("MM/dd/yyyy HH:mm:ss")),
                try_to_timestamp(col("created_at"), lit("yyyy/MM/dd HH:mm:ss")),
                try_to_timestamp(col("created_at"), lit("MM-dd-yyyy HH:mm:ss")),
                try_to_timestamp(col("created_at"), lit("MMM d, yyyy HH:mm:ss")),
                try_to_timestamp(col("created_at"), lit("dd-MM-yy HH:mm:ss"))
            )
        })
        .withColumn(
            "quarantine_reason",
            when(~col("transaction_status").isin(["reversed", "completed", "failed"]), "Invalid transaction_status")
            .when(col("amount") < 0, "Negative amount")
            .otherwise(lit(None))
        )
    )

# --- Silver Table (Valid Data + Lookups) ---
@dp.table(name="dev.silver.silver_transactions")
def silver_transactions():
    df_silver_accounts = spark.readStream.table("dev.silver.silver_accounts")

    return (
        spark.readStream.table("silver_transactions_validated")
        .filter(col("quarantine_reason").isNull())
        .drop("quarantine_reason")
        .dropDuplicates(["transaction_id"])
        .join(df_silver_accounts.select("account_id"), on="account_id", how="inner")
    )

# --- Quarantine Table (Invalid Data) ---
@dp.table(name="dev.silver.quarantine_transactions")
def quarantine_transactions():
    return (
        spark.readStream.table("silver_transactions_validated")
        .filter(col("quarantine_reason").isNotNull())
    )
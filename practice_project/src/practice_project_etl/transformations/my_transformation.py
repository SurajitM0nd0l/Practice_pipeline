from pyspark import pipelines as dp
from pyspark.sql.functions import *


# --------------------------- Auto-loader pipeline read ----------------------------

# ------------------- Customer Ingestion -------------------
@dp.table(name="dev.bronze.bronze_customers")
def bronze_customers():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/Volumes/dev/bronze/raw/customers/")
        .select(
            col("*"),
            col("_metadata.file_name").alias("file_name"),
            col("_metadata.file_path").alias("file_path"),
            current_timestamp().alias("ingestion_timestamp")
        )
    )

# ------------------- Accounts Ingestion -------------------
@dp.table(name="dev.bronze.bronze_accounts")
def bronze_accounts():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/Volumes/dev/bronze/raw/accounts/")
        .select(
            col("*"),
            col("_metadata.file_name").alias("file_name"),
            col("_metadata.file_path").alias("file_path"),
            current_timestamp().alias("ingestion_timestamp")
        )
    )

# ------------------- Transactions Ingestion -------------------
@dp.table(name="dev.bronze.bronze_transactions")
def bronze_transactions():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/Volumes/dev/bronze/raw/transactions/")
        .select(
            col("*"),
            col("_metadata.file_name").alias("file_name"),
            col("_metadata.file_path").alias("file_path"),
            current_timestamp().alias("ingestion_timestamp")
        )
    )

# ------------------- Branches Ingestion -------------------
@dp.table(name="dev.bronze.bronze_branches")
def bronze_branches():
    return (
        spark.readStream
        .format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("cloudFiles.inferColumnTypes", "true")
        .load("/Volumes/dev/bronze/raw/branches/")
        .select(
            col("*"),
            col("_metadata.file_name").alias("file_name"),
            col("_metadata.file_path").alias("file_path"),
            current_timestamp().alias("ingestion_timestamp")
        )
    )


# ------------------------------ Silver Cleaning -------------------------------

# -------------- Silver Customers Clean ---------------
@dp.view(name="silver_customers_staging")
def silver_customers():
    df_customers = spark.readStream.table("dev.bronze.bronze_customers")
    cleaned_df_customers = (
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

        .dropDuplicates()

        .filter(col("email").rlike(r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,10}$"))
    )

    return cleaned_df_customers

# ---------------- SCD type - 2 on Customers -----------------
dp.create_streaming_table(
    name="dev.silver.silver_customers"
)

dp.create_auto_cdc_flow(
    target="dev.silver.silver_customers",
    source="silver_customers_staging",

    keys=["customer_id"],

    sequence_by=col("updated_at"),

    stored_as_scd_type=2
)

# ------------------- Silver Branches Clean --------------------
@dp.table(name="dev.silver.silver_branches")
def silver_branches():
    df_branches = spark.readStream.table("dev.bronze.bronze_branches")
    cleaned_df_branches = (
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

        .dropDuplicates(["branch_id"])

        .filter(col("region").isin(["east", "west", "north", "south", "central"]))
        .filter(col("branch_status").isin(["active", "closed"]))
        .filter(col("branch_type").isin(["metro", "urban", "semi-urban", "rural"]))
    )

    return cleaned_df_branches


# ------------------- Silver Accounts Clean --------------------
@dp.table(name="dev.silver.silver_accounts")
def silver_accounts():
    df_accounts = spark.readStream.table("dev.bronze.bronze_accounts")
    df_silver_customers = spark.readStream.table("dev.silver.silver_customers")
    df_silver_branches = spark.readStream.table("dev.silver.silver_branches")

    current_silver_customer = df_silver_customers.filter(
        col("__END_AT").isNull()
    )

    cleaned_df_accounts = (
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

        .filter(col("account_status").isin(["active", "dormant", "closed"]))
        .filter(col("account_type").isin(["savings", "business", "current", "salary"]))

        .dropDuplicates(["account_id"])

        .join(current_silver_customer.select("customer_id"), on="customer_id", how="inner")
        .join(df_silver_branches.select("branch_id"), on="branch_id", how="inner")
    )

    return cleaned_df_accounts


# ------------------- Silver Transactions Clean --------------------
@dp.table(name="dev.silver.silver_transactions")
def silver_transactions():
    df_transactions = spark.readStream.table("dev.bronze.bronze_transactions")
    df_silver_accounts = spark.readStream.table("dev.silver.silver_accounts")

    cleaned_df_transactions = (
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

        .filter(col("transaction_status").isin(["reversed", "completed", "failed"]))
        .filter(col("amount") >= 0)

        .dropDuplicates(["transaction_id"])

        .join(df_silver_accounts.select("account_id"), on="account_id", how="inner")
    )

    return cleaned_df_transactions


# ------------------------------- Gold Layer -------------------------------
@dp.table(name="dev.gold.dim_customers")
def dim_customers():
    df_gold_customers = spark.read.table("dev.silver.silver_customers")
    return df_gold_customers

@dp.table(name="dev.gold.dim_branches")
def dim_branches():
    df_gold_branches = spark.read.table("dev.silver.silver_branches")
    return df_gold_branches

@dp.table(name="dev.gold.dim_accounts")
def dim_accounts():
    df_gold_accounts = spark.read.table("dev.silver.silver_accounts")
    return df_gold_accounts

@dp.table(name="dev.gold.fact_transactions")
def fact_transactions():

    transactions = spark.read.table(
        "dev.silver.silver_transactions"
    )

    accounts = spark.read.table(
        "dev.gold.dim_accounts"
    )

    return (
        transactions.alias("t")
        .join(
            accounts.alias("a"),
            col("t.account_id") == col("a.account_id"),
            "left"
        )
        .select(
            col("t.transaction_id"),
            col("t.account_id"),
            col("a.customer_id"),
            col("a.branch_id"),

            to_date(col("t.transaction_timestamp")).alias(
                "transaction_date"
            ),

            col("t.transaction_timestamp"),
            col("t.amount"),
            col("t.transaction_type"),
            col("t.transaction_channel"),
            col("t.currency"),
            col("t.merchant_name"),
            col("t.merchant_category"),
            col("t.transaction_status"),
            col("t.reference_number")
        )
    )
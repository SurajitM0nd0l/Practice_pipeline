from pyspark.sql.functions import expr
import pyspark.pipelines as dp

@dp.view(name="dev.gold.vw_dim_customers")
def vw_dim_customers():
    # Read the full unmasked Gold table
    df_customers = spark.read.table("dev.gold.dim_customers")
    
    # Overwrite ONLY the email and number columns with dynamic masking logic.
    # All other columns (customer_id, address, etc.) remain completely untouched.
    masked_df = df_customers.withColumns({
        "email": expr("""
            CASE 
                WHEN current_user() IN (SELECT user_email FROM dev.config.authorized_pii_users) 
                THEN email 
                
                -- Masking format: jo***@***.com
                ELSE CONCAT(LEFT(email, 2), '***@***.com') 
            END
        """),
        
        "number": expr("""
            CASE 
                WHEN current_user() IN (SELECT user_email FROM dev.config.authorized_pii_users) 
                THEN number 
                
                -- Masking format: Keep first 3 and last 2 digits (e.g., 555****99)
                ELSE CONCAT(LEFT(number, 3), '****', RIGHT(number, 2)) 
            END
        """)
    })
    
    return masked_df

# by creating table
INSERT INTO dev.config.authorized_pii_users 
VALUES 
    ('hr_admin@yourcompany.com', 'HR Admin', 'system', current_timestamp()),
    ('compliance_officer@yourcompany.com', 'Compliance', 'system', current_timestamp()),
    ('pipeline_service_principal@yourcompany.com', 'ETL', 'system', current_timestamp())

# by group adding
# The PySpark expr() would look like this instead of doing a SELECT from a table:
"email": expr("""
    CASE 
        WHEN is_account_group_member('pii_readers') 
        THEN email 
        ELSE CONCAT(LEFT(email, 2), '***@***.com') 
    END
""")
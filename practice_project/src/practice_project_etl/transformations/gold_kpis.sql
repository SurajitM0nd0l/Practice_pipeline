-- Calculate the total value of successful transactions
CREATE OR REPLACE VIEW dev.gold.total_successful_transactions AS
SELECT ROUND(SUM(amount), 2) AS total_value FROM dev.gold.fact_transactions WHERE transaction_status = 'completed';

-- calculate the number of successful transaction
CREATE OR REPLACE VIEW dev.gold.number_of_successful_transactions AS
SELECT COUNT(*) AS successful_transaction_count FROM dev.gold.fact_transactions WHERE transaction_status = 'completed';

-- calculate the total value of deposits and withdrawals
CREATE OR REPLACE VIEW dev.gold.total_deposit_withdrawal_value AS
SELECT 
  ROUND(SUM(CASE WHEN transaction_type = 'Deposit' THEN amount ELSE 0 END), 2) AS total_deposit_value,
  ROUND(SUM(CASE WHEN transaction_type = 'Withdrawal' THEN amount ELSE 0 END), 2) AS total_withdrawal_value
FROM dev.gold.fact_transactions
WHERE transaction_type IN ('Deposit', 'Withdrawal');

-- monthly transaction value
CREATE OR REPLACE VIEW dev.gold.monthly_transaction_value AS
SELECT 
  DATE_TRUNC('month', transaction_date) AS transaction_month,
  ROUND(SUM(amount), 2) AS total_value
FROM dev.gold.fact_transactions
GROUP BY transaction_month
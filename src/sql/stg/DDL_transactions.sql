CREATE TABLE VT26052617E774__STAGING.transactions (
	operation_id UUID,
	account_number_from BIGINT,
	account_number_to BIGINT,
	currency_code VARCHAR(3),
	country VARCHAR(30),
	status VARCHAR(15),
	transaction_type VARCHAR(30),
	amount BIGINT,
	transaction_dt TIMESTAMP 
); 

CREATE PROJECTION VT26052617E774__STAGING.transactions_projection (
	operation_id,
	account_number_from,
	account_number_to,
	currency_code,
	country,
	status,
	transaction_type,
	amount,
	transaction_dt
)
AS 
	SELECT *
	FROM VT26052617E774__STAGING.transactions
	ORDER BY transaction_dt
SEGMENTED BY HASH(transaction_dt, operation_id) all nodes;
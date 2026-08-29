CREATE TABLE VT26052617E774__DWH.global_metrics(
	date_update DATE,
	currency_from VARCHAR(3),
	amount_total NUMERIC(18, 2),
	cnt_transactions INTEGER, 
	avg_transactions_per_account NUMERIC(10, 2),
	cnt_accounts_make_transactions INTEGER
);

CREATE PROJECTION VT26052617E774__DWH.global_metrics_projection (
    date_update,
    currency_from,
    amount_total,
    cnt_transactions,
    avg_transactions_per_account,
    cnt_accounts_make_transactions
)
AS
SELECT *
FROM VT26052617E774__DWH.global_metrics
ORDER BY date_update;
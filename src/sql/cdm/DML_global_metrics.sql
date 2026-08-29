INSERT INTO VT26052617E774__DWH.global_metrics(
	date_update,
	currency_from,
    amount_total,
    cnt_transactions,
    avg_transactions_per_account,
    cnt_accounts_make_transactions
)
SELECT
	DATE(t.transaction_dt) as date_update,
    t.currency_code as currency_from,
    SUM(
    	CASE
        	WHEN t.currency_code = 420 THEN t.amount
            ELSE t.amount * c.currency_with_div
        END
    ) as amount_total,
    COUNT(*) as cnt_transactions,
    ROUND(
    	COUNT(*)::NUMERIC
        / COUNT(DISTINCT t.account_number_from),
        2
    ) as avg_transactions_per_account,
    COUNT(DISTINCT t.account_number_from) as cnt_accounts_make_transactions
FROM VT26052617E774__STAGING.transactions t
LEFT JOIN VT26052617E774__STAGING.currencies c
	ON t.currency_code = c.currency_code
    AND c.currency_code_with = 420
    AND c.date_update = DATE(t.transaction_dt)
WHERE DATE(t.transaction_dt) = DATE '{load_date}'
	AND t.account_number_from >= 0
    AND t.status = 'done' 
GROUP BY
	DATE(t.transaction_dt),
	t.currency_code;
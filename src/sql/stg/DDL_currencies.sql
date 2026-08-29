CREATE TABLE VT26052617E774__STAGING.currencies (
	 currency_code VARCHAR(3),
	 currency_code_with VARCHAR(3),
	 date_update DATE,
	 currency_with_div NUMERIC(10, 2) 
);

CREATE PROJECTION VT26052617E774__STAGING.currencies_projection (
	currency_code,
	currency_code_with,
	date_update,
	currency_with_div
)
AS 
	SELECT *
	FROM VT26052617E774__STAGING.currencies
	ORDER BY date_update
SEGMENTED BY HASH(date_update) all nodes;
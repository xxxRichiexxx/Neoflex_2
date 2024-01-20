CREATE SCHEMA DM;

DROP TABLE IF EXISTS DM.DM_ACCOUNT_TURNOVER_F;
CREATE TABLE dm.dm_account_turnover_f (
	oper_date date NOT NULL,
	account_rk int NOT NULL,
	credit_amount_rub numeric(23,8) NULL,
	credit_amount_tousend_rub numeric(23,8) NULL,
	debet_amount_rub numeric(23,8) NULL,
	debet_amount_tousend_rub numeric(23,8) NULL
);


DROP FUNCTION IF EXISTS ACCOUNT_TURNOVER_F (date_from date, date_to date);
CREATE FUNCTION ACCOUNT_TURNOVER_F (date_from date, date_to date) 
RETURNS table(
	oper_date date,
	account_rk int4,
	credit_amount_rub numeric,
	credit_amount_tousend_rub numeric,
	debet_amount_rub numeric,
	debet_amount_tousend_rub numeric
) AS 
$$
	WITH sq AS(
		SELECT
			p.oper_date
			,p.credit_account_rk										AS account_rk	
			,COALESCE(reduced_cource, 1)*p.credit_amount				AS credit_amount_rub
			,ROUND(COALESCE(reduced_cource, 1)*p.credit_amount/1000, 0)	AS credit_amount_tousend_rub
			,CAST(NULL AS INT)											AS debet_amount_rub
			,CAST(NULL AS INT)											AS debet_amount_tousend_rub
		FROM ods.ft_posting_f p
		LEFT JOIN ods.md_account_d a 
			ON p.credit_account_rk = a.account_rk 
			AND p.oper_date BETWEEN a.data_actual_date AND a.data_actual_end_date 
		LEFT JOIN ods.md_exchange_rate_d r
			ON a.currency_rk = r.currency_rk 
			AND p.oper_date BETWEEN r.data_actual_date  AND r.data_actual_end_date
		WHERE p.oper_date BETWEEN date_from AND date_to
		UNION ALL	
		SELECT
			p.oper_date
			,p.debet_account_rk											AS account_rk											
			,CAST(NULL AS INT)											AS credit_amount_rub
			,CAST(NULL AS INT)											AS credit_amount_tousend_rub
			,COALESCE(reduced_cource, 1)*p.debet_amount					AS debet_amount_rub
			,ROUND(COALESCE(reduced_cource, 1)*p.debet_amount/1000, 0)	AS credit_amount_tousend_rub
		FROM ods.ft_posting_f p
		LEFT JOIN ods.md_account_d a 
			ON p.debet_account_rk = a.account_rk 
			AND p.oper_date BETWEEN a.data_actual_date AND a.data_actual_end_date 
		LEFT JOIN ods.md_exchange_rate_d r
			ON a.currency_rk = r.currency_rk 
			AND p.oper_date BETWEEN r.data_actual_date  AND r.data_actual_end_date
		WHERE p.oper_date BETWEEN date_from AND date_to
	)
	SELECT
		oper_date
		,account_rk
		,SUM(credit_amount_rub)											AS credit_amount_rub
		,SUM(credit_amount_tousend_rub)									AS credit_amount_tousend_rub
		,SUM(debet_amount_rub)											AS debet_amount_rub
		,SUM(debet_amount_tousend_rub)									AS debet_amount_tousend_rub
	FROM sq
	GROUP BY 
		oper_date
		,account_rk
	ORDER BY
		oper_date
		,account_rk
$$
LANGUAGE SQL;

INSERT INTO DM.DM_ACCOUNT_TURNOVER_F
SELECT * FROM ACCOUNT_TURNOVER_F('2018-01-01', '2018-01-31');




DROP TABLE IF EXISTS dm.dm_f101_round_f;
CREATE TABLE dm.dm_f101_round_f (
	"date_from" date NOT NULL,
	"date_to" date NOT NULL,
	plan char(1) NOT NULL,
	num_sc int NOT NULL,
	a_p int4 NOT NULL,
	balance_in_rub numeric(23,8) NULL,
	balance_in_curr numeric(23,8) NULL,
	balance_in_total numeric(23,8) NULL,
	turn_deb_rub numeric(23,8) NULL,
	turn_deb_curr numeric(23,8) NULL,
	turn_deb_total numeric(23,8) NULL,
	turn_cre_rub numeric(23,8) NULL,
	turn_cre_curr numeric(23,8) NULL,
	turn_cre_total numeric(23,8) NULL,
	balance_out_rub numeric(23,8) NULL,
	balance_out_curr numeric(23,8) NULL,
	balance_out_total numeric(23,8) NULL
);


DROP FUNCTION IF EXISTS F101_ROUND_F(date_from date, date_to date);
CREATE FUNCTION F101_ROUND_F(date_from date, date_to date)
RETURNS TABLE (
	"date_from" date,
	"date_to" date,
	plan char(1),
	num_sc int,
	a_p int4,
	balance_in_rub numeric(23,8),
	balance_in_curr numeric(23,8),
	balance_in_total numeric(23,8),
	turn_deb_rub numeric(23,8),
	turn_deb_curr numeric(23,8),
	turn_deb_total numeric(23,8),
	turn_cre_rub numeric(23,8),
	turn_cre_curr numeric(23,8),
	turn_cre_total numeric(23,8),
	balance_out_rub numeric(23,8),
	balance_out_curr numeric(23,8),
	balance_out_total numeric(23,8)
) AS 
$$
	SELECT
		date_from
		,date_to
		,l.chapter																		AS PLAN 
		,l.ledger_account																AS NUM_SC 
		,CASE WHEN a.char_type = 'A' THEN 1 ELSE 2 END									AS A_P
		,SUM(
			CASE
				WHEN a.currency_code IN (643, 810) THEN b.balance_out
				ELSE 0
			END
		)																				AS balance_in_rub
		,SUM(
			CASE
				WHEN a.currency_code NOT IN (643, 810) 
					THEN b.balance_out*COALESCE(r.reduced_cource, 1)
				ELSE 0
			END
		)																				AS balance_in_curr
		,SUM(
			CASE
				WHEN a.currency_code IN (643, 810) THEN b.balance_out 
				ELSE b.balance_out*COALESCE(r.reduced_cource, 1)
			END																				
		)																				AS balance_in_total
		,SUM(
			CASE 
				WHEN a.currency_code IN (643, 810) THEN t.debet_amount_rub
				ELSE 0
			END
		)																				AS turn_deb_rub 
		,SUM(
			CASE 
				WHEN a.currency_code NOT IN (643, 810) THEN t.debet_amount_rub
				ELSE 0
			END	
		) 																				AS turn_deb_curr
		,SUM(t.debet_amount_rub)														AS turn_deb_total
		,SUM(
			CASE 
				WHEN a.currency_code IN (643, 810) THEN t.credit_amount_rub
				ELSE 0
			END
		)																				AS turn_cre_rub
		,SUM(
			CASE 
				WHEN a.currency_code NOT IN (643, 810) THEN t.credit_amount_rub
				ELSE 0
			END
		)																				AS turn_cre_curr
		,SUM(t.credit_amount_rub)														AS turn_cre_total
		,SUM(
			CASE 
				WHEN a.char_type ='A' AND a.currency_code IN (643, 810)
					THEN b.balance_out - t.credit_amount_rub + t.debet_amount_rub 
				WHEN a.char_type ='P' AND a.currency_code IN (643, 810)
					THEN b.balance_out + t.credit_amount_rub - t.debet_amount_rub 
			END
		)																				AS "balance_out_rub"
		,SUM(
			CASE 
				WHEN a.char_type ='A' AND a.currency_code NOT IN (643, 810)
					THEN b.balance_out*COALESCE(r.reduced_cource, 1) 
						- t.credit_amount_rub + t.debet_amount_rub
				WHEN a.char_type ='P' AND a.currency_code NOT IN (643, 810)
					THEN b.balance_out*COALESCE(r.reduced_cource, 1) 
						+ t.credit_amount_rub - t.debet_amount_rub
			END
		)																				AS "balance_out_curr"
		,SUM(
			CASE 
				WHEN a.char_type ='A' AND a.currency_code IN (643, 810)
					THEN b.balance_out - t.credit_amount_rub + t.debet_amount_rub 
				WHEN a.char_type ='P' AND a.currency_code IN (643, 810)
					THEN b.balance_out + t.credit_amount_rub - t.debet_amount_rub 
			END
			+
			CASE 
				WHEN a.char_type ='A' AND a.currency_code NOT IN (643, 810)
					THEN b.balance_out*COALESCE(r.reduced_cource, 1) 
						- t.credit_amount_rub + t.debet_amount_rub
				WHEN a.char_type ='P' AND a.currency_code NOT IN (643, 810)
					THEN b.balance_out*COALESCE(r.reduced_cource, 1) 
						+ t.credit_amount_rub - t.debet_amount_rub
			END
		)																				AS "balance_out_total"
	FROM ods.ft_balance_f b
	RIGHT JOIN ods.md_account_d a 
		ON b.account_rk = a.account_rk 
		AND b.on_date BETWEEN a.data_actual_date AND a.data_actual_end_date 
	LEFT JOIN ods.md_exchange_rate_d r
		ON a.currency_rk = r.currency_rk 
		AND b.on_date BETWEEN r.data_actual_date AND r.data_actual_end_date
	LEFT JOIN ods.md_ledger_account_s l
		ON SUBSTRING(a.account_number::varchar, 1, 5)::INT = l.ledger_account
		AND b.on_date BETWEEN l.start_date  AND l.end_date
	LEFT JOIN DM.DM_ACCOUNT_TURNOVER_F t 
		ON a.account_rk = t.account_rk 
		AND b.on_date = t.oper_date
	WHERE b.on_date BETWEEN date_from AND date_to
	GROUP BY
		l.chapter																	
		,l.ledger_account																
		,CASE WHEN a.char_type = 'A' THEN 1 ELSE 2 END
$$
LANGUAGE SQL;							
		
		
INSERT INTO DM.DM_F101_ROUND_F
SELECT * FROM F101_ROUND_F('2018-01-01', '2018-01-31');
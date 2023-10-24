
{{ config(materialized='table') }}

select
	t.user_id,
	t.transaction_date,
	t.amount,
    t.transaction_type
FROM public.user_transactions t
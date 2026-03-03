CREATE VIEW datamart.vw_customer_summary AS
SELECT 
    customer_id,
    customer_name,
    email,
    city,
    total_orders,
    total_amount,
    customer_tier,
    record_eff_start_dt,
    btch_insert_id
FROM dwh.customer_orders
WHERE record_active_flag = 'Y';
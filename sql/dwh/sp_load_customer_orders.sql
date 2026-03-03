CREATE OR REPLACE PROCEDURE dwh.sp_load_customer_orders(p_batch_id INT)
AS $$
BEGIN
    CREATE TEMP TABLE tmp_customer_orders AS
    SELECT 
        c.customer_id,
        COALESCE(c.name, 'UNKNOWN')         AS customer_name,
        LOWER(c.email)                       AS email,
        COALESCE(c.city, 'NOT PROVIDED')     AS city,
        COUNT(o.order_id)                    AS total_orders,
        SUM(COALESCE(o.amount, 0))           AS total_amount,
        CASE 
            WHEN SUM(o.amount) >= 50000 THEN 'PLATINUM'
            WHEN SUM(o.amount) >= 20000 THEN 'GOLD'
            ELSE 'SILVER'
        END                                  AS customer_tier,
        c.created_date
    FROM staging.customers c
    LEFT JOIN staging.orders o ON c.customer_id = o.customer_id
    GROUP BY c.customer_id, c.name, c.email, c.city, c.created_date;

    UPDATE dwh.customer_orders dwh
    SET 
        record_eff_end_dt   = GETDATE(),
        record_active_flag  = 'N',
        btch_upd_id         = p_batch_id,
        lst_upd_dt          = GETDATE()
    FROM tmp_customer_orders tmp
    WHERE dwh.customer_id       = tmp.customer_id
    AND dwh.record_active_flag  = 'Y'
    AND (
        dwh.customer_name   <> tmp.customer_name OR
        dwh.email           <> tmp.email OR
        dwh.city            <> tmp.city OR
        dwh.total_orders    <> tmp.total_orders OR
        dwh.total_amount    <> tmp.total_amount OR
        dwh.customer_tier   <> tmp.customer_tier
    );

    INSERT INTO dwh.customer_orders (
        customer_id, customer_name, email, city,
        total_orders, total_amount, customer_tier, created_date,
        record_eff_start_dt, record_eff_end_dt, record_active_flag,
        record_processed_dt, btch_insert_id, lst_insrt_dt
    )
    SELECT 
        tmp.customer_id, tmp.customer_name, tmp.email, tmp.city,
        tmp.total_orders, tmp.total_amount, tmp.customer_tier, tmp.created_date,
        GETDATE(), '9999-12-31', 'Y', GETDATE(), p_batch_id, GETDATE()
    FROM tmp_customer_orders tmp
    WHERE NOT EXISTS (
        SELECT 1 FROM dwh.customer_orders dwh
        WHERE dwh.customer_id       = tmp.customer_id
        AND dwh.record_active_flag  = 'Y'
        AND dwh.customer_name       = tmp.customer_name
        AND dwh.email               = tmp.email
        AND dwh.city                = tmp.city
    );

    DROP TABLE tmp_customer_orders;
END;
$$ LANGUAGE plpgsql;
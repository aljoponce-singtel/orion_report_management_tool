SELECT
    DISTINCT order_code
FROM
    RestInterface_order
WHERE
    service_number IN ({});
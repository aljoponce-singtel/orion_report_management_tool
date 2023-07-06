SELECT
    DISTINCT ord.order_code,
    ord.service_number,
    ord.order_status,
    ord.order_type,
    prd.network_product_code,
    per.role,
    act.name,
    act.status,
    act.completed_date
FROM
    RestInterface_order ord
    JOIN RestInterface_activity act ON act.order_id = ord.id
    LEFT JOIN RestInterface_person per ON per.id = act.person_id
    LEFT JOIN RestInterface_npp npp ON npp.order_id = ord.ID
    AND npp.level = 'Mainline'
    AND npp.status <> 'Cancel'
    LEFT JOIN RestInterface_product prd ON prd.id = npp.product_id
WHERE
    ord.order_code IN (
        'ZDY5727008',
        'ZFA0859006',
        'ZEP1915005',
        'ZEP1915008',
        'ZDK2143002',
        'ZAO9908002',
        'ZFD0167001',
        'ZFC1506005',
        'ZEP9265003',
        'ZFG0917001',
        'ZER9798001'
    )
ORDER BY
    ord.order_code,
    act.name;
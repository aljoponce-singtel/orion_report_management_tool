SELECT
    DISTINCT ORD.order_code,
    ORD.service_number,
    -- ORD.order_status,
    ORD.order_type,
    CUS.name AS customer,
    PRD.network_product_code AS product_code,
    PER.role,
    ACT.name,
    ACT.status,
    ACT.completed_date,
    DATE_FORMAT(ACT.updated_at, '%Y-%m-%d %H:%i:%s') AS updated_at
FROM
    RestInterface_order ORD
    JOIN RestInterface_activity ACT ON ACT.order_id = ORD.id
    LEFT JOIN RestInterface_person PER ON PER.id = ACT.person_id
    LEFT JOIN RestInterface_npp NPP ON NPP.order_id = ORD.ID
    AND NPP.level = 'Mainline'
    AND NPP.status <> 'Cancel'
    LEFT JOIN RestInterface_product PRD ON PRD.id = NPP.product_id
    LEFT JOIN RestInterface_customer CUS ON CUS.id = ORD.customer_id
WHERE
    (
        PER.role LIKE 'ODC_%'
        OR PER.role LIKE 'RDC_%'
        OR PER.role LIKE 'GSPSG_%'
        OR PER.role IN ('GSPSG_ME', 'GSP_LTC_GW', 'GSDT31')
    )
    AND ORD.order_code IN (
        'ZEH0496004',
        'ZEH0498006',
        'ZEH0496007',
        'ZEH0496005',
        'ZEH0496008',
        'ZEH0496001',
        'ZEI2803002',
        'ZGC1789002',
        'ZFT7219001',
        'ZFW5610001',
        'ZGO5543001',
        'ZGB8026001',
        'ZFH8660003'
    )
ORDER BY
    PRD.network_product_code,
    ORD.order_code,
    PER.role,
    ACT.name;
SELECT
    DISTINCT ORD.order_code,
    ORD.service_number,
    ORD.order_status,
    ORD.order_type,
    NPP.level,
    NPP.status,
    PRD.network_product_code,
    PER.role,
    ACT.name,
    ACT.status,
    ACT.completed_date,
    DATE(ACT.updated_at) AS updated_at
FROM
    RestInterface_order ORD
    JOIN RestInterface_activity ACT ON ACT.order_id = ORD.id
    LEFT JOIN RestInterface_person PER ON PER.id = ACT.person_id
    LEFT JOIN RestInterface_npp NPP ON NPP.order_id = ORD.ID
    AND NPP.level = 'Mainline'
    AND NPP.status <> 'Cancel'
    LEFT JOIN RestInterface_product PRD ON PRD.id = NPP.product_id
WHERE
    PER.role IN (
        'RDC_ASSG',
        'RDC_ASSG1',
        'RDC_ASSG2',
        'RDC_ASSG3',
        'RDC_ASSG4',
        'RDC_GSP',
        'RDC_GSP1',
        'RDC_GSP2',
        'RDC_GSP3',
        'RDC_GSP4',
        'RDC_ILC',
        'RDC_ILC1',
        'RDC_ILC2',
        'RDC_ILC3',
        'RDC_ILC4'
    ) -- AND ACT.name       IN ( 'GSDT Co-ordination Work', 'GSDT Co-ordination OS LLC' )
    AND ORD.order_code IN ('ZBK5222012', 'ZFN2189001')
ORDER BY
    ORD.order_code,
    NPP.level,
    NPP.status,
    ACT.name;
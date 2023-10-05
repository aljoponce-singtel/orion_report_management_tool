SELECT
    DISTINCT ORD.order_code,
    ORD.current_crd,
    ORD.order_status,
    ORD.order_type,
    PRJ.project_code,
    PER.role AS group_id,
    CAST(ACT.activity_code AS SIGNED INTEGER) AS actstepno,
    ACT.name,
    ACT.status,
    ACT.due_date,
    ACT.completed_date,
    DATE(ACT.updated_at) AS act_updated_at
FROM
    RestInterface_order ORD
    INNER JOIN RestInterface_activity ACT ON ORD.id = ACT.order_id
    LEFT JOIN RestInterface_project PRJ ON ORD.project_id = PRJ.id
    LEFT JOIN RestInterface_customer CUS ON CUS.id = ORD.customer_id
    LEFT JOIN RestInterface_person PER ON ACT.person_id = PER.id
    LEFT JOIN RestInterface_user USR ON PER.user_id = USR.id
    LEFT JOIN RestInterface_npp NPP ON NPP.order_id = ORD.id
    AND NPP.level = 'Mainline'
    LEFT JOIN RestInterface_product PRD ON PRD.id = NPP.product_id
WHERE
    ORD.order_code IN (
        'YFZ9759001',
        'YFZ9759002',
        'YBB2461002',
        'YBB2461003'
    )
ORDER BY
    ORD.order_code,
    actstepno;
SELECT
    ORD.order_code,
    CUS.name,
    ORD.current_crd,
    ORD.service_number,
    ORD.order_status,
    ORD.order_type,
    PRD.network_product_code,
    PER.role AS GroupID,
    CAST(ACT.activity_code AS UNSIGNED) AS step_no,
    ACT.name,
    ACT.predecessor_list,
    ACT.status,
    ACT.due_date,
    ACT.completed_date
FROM
    RestInterface_activity ACT
    LEFT JOIN RestInterface_order ORD ON ACT.order_id = ORD.id
    LEFT JOIN RestInterface_project PRJ ON ORD.project_id = PRJ.id
    LEFT JOIN RestInterface_person PER ON ACT.person_id = PER.id
    LEFT JOIN RestInterface_customer CUS ON CUS.id = ORD.customer_id
    LEFT JOIN RestInterface_user USR ON PER.user_id = USR.id
    LEFT JOIN RestInterface_npp NPP ON NPP.order_id = ORD.id
    AND NPP.level = 'Mainline'
    LEFT JOIN RestInterface_product PRD ON PRD.id = NPP.product_id
WHERE
    ORD.service_number = 'M0684040'
ORDER BY
    ORD.order_code,
    activity_code;
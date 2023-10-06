SELECT
    DISTINCT ORD.order_code,
    ORD.service_number,
    PRD.network_product_code,
    PRD.network_product_desc,
    CUS.name AS customer_name,
    ORD.order_type,
    ORD.current_crd,
    ORD.taken_date,
    PER.role AS group_id,
    CAST(ACT.activity_code AS SIGNED INTEGER) AS act_step_no,
    ACT.name AS act_name,
    ACT.due_date AS act_due_date,
    ACT.ready_date AS act_rdy_date,
    DATE(ACT.exe_date) AS act_exe_date,
    DATE(ACT.dly_date) AS act_dly_date,
    ACT.completed_date AS act_com_date
FROM
    RestInterface_order ORD
    JOIN RestInterface_activity ACT ON ACT.order_id = ORD.id
    JOIN RestInterface_person PER ON PER.id = ACT.person_id
    LEFT JOIN RestInterface_customer CUS ON CUS.id = ORD.customer_id
    LEFT JOIN RestInterface_npp NPP ON ORD.id = NPP.order_id
    AND NPP.level = 'MainLine'
    AND NPP.status != 'Cancel'
    LEFT JOIN RestInterface_product PRD ON NPP.product_id = PRD.id
WHERE
    ORD.id IN (
        SELECT
            DISTINCT ORD.id
        FROM
            RestInterface_order ORD
            JOIN RestInterface_activity ACT ON ACT.order_id = ORD.id
            JOIN RestInterface_person PER ON PER.id = ACT.person_id
        WHERE
            PER.role IN (
                'GSP_SDN',
                'GSP_NFV'
            )
            AND ACT.completed_date BETWEEN '2023-07-01'
            AND '2023-07-31'
    )
    AND (
        (
            PER.role IN (
                'GSP_SDN',
                'GSP_NFV'
            )
            AND ACT.completed_date BETWEEN '2023-07-01'
            AND '2023-07-31'
        )
        OR PER.role IN (
            'GSDT_SDN',
            'GSDT_NFV'
        )
    )
ORDER BY
    ORD.order_code,
    act_step_no;
SELECT
    DISTINCT (
        CASE
            WHEN ORD.service_number REGEXP '.*LLC$' THEN 'OLLC'
            ELSE 'ILC'
        END
    ) AS 'Service',
    ORD.order_code AS 'OrderCode',
    CUS.name AS 'CustomerName',
    ORD.current_crd AS 'CRD',
    ORD.service_number AS 'ServiceNumber',
    ORD.order_status AS 'OrderStatus',
    ORD.order_type AS 'OrderType',
    PRD.network_product_code AS 'ProductCode',
    PER_PRECONFIG.role AS 'PreConfig_GroupID',
    (
        CASE
            WHEN PER_PRECONFIG.role REGEXP '^ODC_.*' THEN 'ODC'
            WHEN PER_PRECONFIG.role REGEXP '^RDC_.*' THEN 'RDC'
            WHEN PER_PRECONFIG.role IS NULL THEN NULL
            ELSE 'SGP'
        END
    ) AS 'PreConfig_Team',
    ACT_PRECONFIG.name AS 'PreConfig_ActName',
    ACT_PRECONFIG.status AS 'PreConfig_ActStatus',
    ACT_PRECONFIG.due_date AS 'PreConfig_ActDueDate',
    ACT_PRECONFIG.completed_date AS 'PreConfig_COM_Date',
    PER_COORDINATION.role AS 'Coordination_Group_ID',
    (
        CASE
            WHEN PER_COORDINATION.role REGEXP '^ODC_.*' THEN 'ODC'
            WHEN PER_COORDINATION.role REGEXP '^RDC_.*' THEN 'RDC'
            WHEN PER_COORDINATION.role IS NULL THEN NULL
            ELSE 'SGP'
        END
    ) AS 'Coordination_Team',
    ACT_COORDINATION.name AS 'Coordination_ActName',
    ACT_COORDINATION.status AS 'Coordination_ActStatus',
    ACT_COORDINATION.due_date AS 'Coordination_ActDueDate',
    ACT_COORDINATION.completed_date AS 'Coordination_COM_Date'
FROM
    (
        SELECT
            ORD.id AS order_id
        FROM
            RestInterface_order ORD
            JOIN RestInterface_activity ACT ON ACT.order_id = ORD.id
            JOIN RestInterface_person PER ON PER.id = ACT.person_id
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
            )
            AND ACT.name IN (
                'GSDT Co-ordination Work',
                'GSDT Co-ordination OS LLC'
            )
            AND ACT.completed_date BETWEEN '2023-06-01'
            AND '2023-06-30'
    ) ORD_COM
    JOIN RestInterface_order ORD ON ORD.id = ORD_COM.order_id
    JOIN (
        SELECT
            ACT.order_id,
            MAX(
                CASE
                    WHEN ACT.name NOT IN (
                        'GSDT Co-ordination Work',
                        'GSDT Co-ordination OS LLC'
                    ) THEN ACT.id
                END
            ) preconfig_id,
            MAX(
                CASE
                    WHEN ACT.name IN (
                        'GSDT Co-ordination Work',
                        'GSDT Co-ordination OS LLC'
                    ) THEN ACT.id
                END
            ) coordination_id
        FROM
            RestInterface_activity ACT
            JOIN RestInterface_person PER ON PER.id = ACT.person_id
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
                'RDC-GSP4',
                'RDC_ILC',
                'RDC_ILC1',
                'RDC_ILC2',
                'RDC_ILC3',
                'RDC_ILC4'
            )
        GROUP BY
            order_id
    ) ACT_MAX ON ACT_MAX.order_id = ORD.id
    LEFT JOIN RestInterface_activity ACT_PRECONFIG ON ACT_PRECONFIG.id = ACT_MAX.preconfig_id
    LEFT JOIN RestInterface_activity ACT_COORDINATION ON ACT_COORDINATION.id = ACT_MAX.coordination_id
    LEFT JOIN RestInterface_person PER_PRECONFIG ON PER_PRECONFIG.id = ACT_PRECONFIG.person_id
    LEFT JOIN RestInterface_person PER_COORDINATION ON PER_COORDINATION.id = ACT_COORDINATION.person_id
    LEFT JOIN RestInterface_customer CUS ON CUS.id = ORD.customer_id
    LEFT JOIN RestInterface_npp NPP ON NPP.order_id = ORD.id
    AND NPP.level = 'Mainline'
    AND NPP.status != 'Cancel'
    LEFT JOIN RestInterface_product PRD ON PRD.id = NPP.product_id
WHERE
    PRD.network_product_code IN (
        'BIC0022',
        'BIC0003',
        'CIC0001',
        'CIC0006',
        'CIC0010',
        'ILC0008'
    )
ORDER BY
    ORD.order_code;
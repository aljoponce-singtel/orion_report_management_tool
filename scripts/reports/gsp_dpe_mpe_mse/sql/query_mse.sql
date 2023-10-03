SELECT
    DISTINCT ORD.order_code AS 'Workorder',
    ORD.service_number AS 'Service No',
    CUS.name AS 'Customer Name',
    ACT.name AS 'Activity name',
    PER.role AS 'Group ID',
    ORD.current_crd AS 'CRD',
    ORD.order_type AS 'Order type',
    'TBD' AS 'GRS No',
    NPP.level AS 'NPP Level',
    PRD.network_product_code AS 'NPC',
    ORD.taken_date AS 'Order creation date',
    ACT.status AS 'Act Status',
    ACT.completed_date AS 'Comm date',
    'TBD' AS 'Purchasing Group',
    'TBD' AS 'Product Type',
    PAR.IMPGcode AS 'IMPG Code',
    PER.email AS 'Group Owner',
    ACT.performer_id AS 'Performer ID'
FROM
    RestInterface_order ORD
    JOIN RestInterface_activity ACT ON ORD.id = ACT.order_id
    JOIN RestInterface_person PER ON ACT.person_id = PER.id
    LEFT JOIN RestInterface_customer CUS ON CUS.id = ORD.customer_id
    LEFT JOIN RestInterface_user USR ON PER.user_id = USR.id
    LEFT JOIN RestInterface_npp NPP ON NPP.order_id = ORD.id
    LEFT JOIN RestInterface_product PRD ON PRD.id = NPP.product_id
    LEFT JOIN (
        SELECT
            NPP_INNER.id,
            MAX(
                CASE
                    WHEN PAR_INNER.parameter_name = 'IMPGcode' THEN PAR_INNER.parameter_value
                END
            ) IMPGcode
        FROM
            RestInterface_npp NPP_INNER
            JOIN RestInterface_parameter PAR_INNER ON PAR_INNER.npp_id = NPP_INNER.id
        WHERE
            NPP_INNER.status != 'Cancel'
        GROUP BY
            NPP_INNER.id
    ) PAR ON PAR.id = NPP.id
WHERE
    PER.role IN (
        'CPE',
        'CPE_CSE',
        'EWO',
        'EWO_CSE',
        'IMPACT_CSE',
        'LAN_CPE',
        'LAN_CPE_TR',
        'RLAN',
        'RLAN_CSE',
        'SDLAN',
        'SDWAN',
        'TOPS',
        'TOPS_CSE',
        'TSGRS',
        'TSGRS_CSE',
        'WIFI',
        'WIFI_CSE',
        'WLAN',
        'WLAN_CSE'
    )
    AND ACT.completed_date BETWEEN '2023-07-01'
    AND '2023-07-31'
ORDER BY
    ORD.order_code,
    ACT.name;
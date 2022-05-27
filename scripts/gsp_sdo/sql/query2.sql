SELECT
    DISTINCT ORD.id AS OrderId,
    ORD.order_code AS OrderCode,
    ORD.service_number AS ServiceNumber,
    REPLACE(
        REPLACE(
            REPLACE(ORD.service_number, 'ELITE', ''),
            'ETH',
            ''
        ),
        'GWL',
        ''
    ) AS ServiceNumberUpd,
    PRD.network_product_code AS ProductCode,
    ORD.current_crd AS CRD,
    CUS.name AS CustomerName,
    ORD.taken_date AS OrderCreated,
    ORD.order_type AS OrderType,
    (
        SELECT
            GROUP_CONCAT(
                CONCAT_WS(" ", family_name, given_name)
                ORDER BY
                    id DESC SEPARATOR " / "
            )
        FROM
            RestInterface_contactdetails
        WHERE
            order_id = ORD.id
            AND contact_type = "Project Manager"
        GROUP BY
            order_id
    ) AS ProjectManager
FROM
    RestInterface_order ORD
    LEFT JOIN RestInterface_npp NPP ON NPP.order_id = ORD.id
    AND NPP.level = 'Mainline'
    AND NPP.status <> 'Cancel'
    LEFT JOIN RestInterface_product PRD ON PRD.id = NPP.product_id
    LEFT JOIN RestInterface_customer CUS ON CUS.id = ORD.customer_id
WHERE
    ORD.order_code IN ({})
    AND ORD.order_type = 'Provide';
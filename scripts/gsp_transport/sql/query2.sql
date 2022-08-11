CREATE TEMPORARY TABLE COM_QUEUES
SELECT
    DISTINCT (
        CASE
            WHEN PRD.network_product_code LIKE 'DGN%' THEN 'Diginet'
            WHEN PRD.network_product_code LIKE 'DME%' THEN 'MetroE'
            WHEN PRD.network_product_code = 'ELK0052' THEN 'MegaPop (CE)'
            WHEN PRD.network_product_code LIKE 'GGW%' THEN 'Gigawave'
            ELSE 'Service'
        END
    ) AS Service,
    PRD.network_product_code,
    ORD.order_type,
    ORD.order_code,
    PER.role,
    ACT.name,
    ACT.status,
    ACT.completed_date,
    ORD.id
FROM
    RestInterface_order ORD
    JOIN RestInterface_activity ACT ON ACT.order_id = ORD.id
    LEFT JOIN RestInterface_person PER ON PER.id = ACT.person_id
    LEFT JOIN RestInterface_npp NPP ON NPP.order_id = ORD.ID
    AND NPP.level = 'Mainline'
    AND NPP.status <> 'Cancel'
    LEFT JOIN RestInterface_product PRD ON PRD.id = NPP.product_id
WHERE
    (
        (
            PRD.network_product_code LIKE 'DGN%'
            AND ORD.order_type IN ('Provide', 'Change', 'Cease')
            AND (
                PER.role LIKE 'ODC_%'
                OR PER.role LIKE 'RDC_%'
                OR PER.role LIKE 'GSPSG_%'
            )
            AND ACT.name = 'GSDT Co-ordination Wrk-BQ'
            AND ACT.status = 'COM'
        )
        OR (
            PRD.network_product_code LIKE 'DME%'
            AND ORD.order_type IN ('Provide', 'Change', 'Cease')
            AND (
                PER.role LIKE 'ODC_%'
                OR PER.role LIKE 'RDC_%'
                OR PER.role LIKE 'GSPSG_%'
            )
            AND ACT.name = 'GSDT Co-ordination Wrk-BQ'
            AND ACT.status = 'COM'
        )
        OR (
            PRD.network_product_code = 'ELK0052'
            AND (
                (
                    ORD.order_type IN ('Provide', 'Change')
                    AND (
                        PER.role LIKE 'ODC_%'
                        OR PER.role LIKE 'RDC_%'
                        OR PER.role LIKE 'GSPSG_%'
                    )
                    AND ACT.name = 'Circuit Creation'
                    AND ACT.status = 'COM'
                )
                OR (
                    ORD.order_type = 'Cease'
                    AND (
                        PER.role LIKE 'ODC_%'
                        OR PER.role LIKE 'RDC_%'
                        OR PER.role LIKE 'GSPSG_%'
                    )
                    AND ACT.name = 'Node & Circuit Deletion'
                    AND ACT.status = 'COM'
                )
            )
        )
        OR (
            PRD.network_product_code LIKE 'GGW%'
            AND (
                (
                    ORD.order_type = 'Provide'
                    AND PER.role = 'GSP_LTC_GW'
                    AND ACT.name = 'GSDT Co-ordination Work'
                    AND ACT.status = 'COM'
                )
                OR (
                    ORD.order_type = 'Cease'
                    AND PER.role = 'GSDT31'
                    AND ACT.name = 'GSDT Co-ordination Work'
                    AND ACT.status = 'COM'
                )
            )
        )
    )
    AND ACT.completed_date BETWEEN '2022-07-01'
    AND '2022-07-31'
ORDER BY
    Service,
    ORD.order_type DESC,
    PER.role,
    ORD.order_code,
    ACT.name;
SELECT
    DISTINCT ORD.order_code
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
            (
                (
                    PRD.network_product_code LIKE 'DGN%'
                    OR PRD.network_product_code LIKE 'DME%'
                )
                AND ACT.name = 'GSDT Co-ordination Wrk-BQ'
            )
            OR (
                PRD.network_product_code LIKE 'GGW%'
                AND ORD.order_type IN ('Provide', 'Cease')
                AND ACT.name = 'GSDT Co-ordination Work'
            )
            OR (
                PRD.network_product_code = 'ELK0052'
                AND (
                    (
                        ORD.order_type IN ('Provide')
                        AND ACT.name = 'Circuit Creation'
                    )
                    OR (
                        ORD.order_type IN ('Change')
                        AND ACT.name = 'Circuit Creation'
                    )
                    OR (
                        ORD.order_type IN ('Cease')
                        AND ACT.name = 'Node & Circuit Deletion'
                    )
                )
            )
        )
        AND (
            PER.role LIKE 'ODC_%'
            OR PER.role LIKE 'RDC_%'
            OR PER.role LIKE 'GSPSG_%'
        )
    )
    AND ACT.status = 'COM'
    AND ACT.completed_date BETWEEN '2022-07-01'
    AND '2022-07-31';
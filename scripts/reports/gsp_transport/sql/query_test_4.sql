SELECT
    DISTINCT ord.order_code
FROM
    RestInterface_order ord
    JOIN RestInterface_activity act ON act.order_id = ord.id
    LEFT JOIN RestInterface_person per ON per.id = act.person_id
    LEFT JOIN RestInterface_npp npp ON npp.order_id = ord.ID
    AND npp.level = 'Mainline'
    AND npp.status <> 'Cancel'
    LEFT JOIN RestInterface_product prd ON prd.id = npp.product_id
WHERE
    (
        (
            (
                prd.network_product_code LIKE 'DGN%'
                OR prd.network_product_code LIKE 'DEK%'
                OR prd.network_product_code LIKE 'DLC%'
            )
            AND ord.order_type IN ('Provide', 'Change', 'Cease')
            AND (
                per.role LIKE 'ODC_%'
                OR per.role LIKE 'RDC_%'
                OR per.role LIKE 'GSPSG_%'
            )
            AND act.name = 'GSDT Co-ordination Wrk-BQ'
        )
        OR (
            prd.network_product_code LIKE 'DME%'
            AND (
                per.role LIKE 'ODC_%'
                OR per.role LIKE 'RDC_%'
                OR per.role LIKE 'GSPSG_%'
            )
            AND (
                (
                    ord.order_type IN ('Provide', 'Change', 'Cease')
                    AND act.name IN (
                        'GSDT Co-ordination Wrk-BQ',
                        'GSDT Co-ordination Work'
                    )
                )
                OR (
                    ord.order_type = 'Provide'
                    AND act.name = 'Circuit creation'
                )
            )
        )
        OR (
            prd.network_product_code IN (
                'ELK0031',
                'ELK0052',
                'ELK0053',
                'ELK0055',
                'ELK0089',
                'ELK0090',
                'ELK0091',
                'ELK0092',
                'ELK0093',
                'ELK0094'
            )
            AND (
                (
                    (
                        per.role LIKE 'ODC_%'
                        OR per.role LIKE 'RDC_%'
                        OR per.role LIKE 'GSPSG_%'
                    )
                    AND (
                        (
                            ord.order_type = 'Provide'
                            AND act.name = 'Circuit Creation'
                        )
                        OR (
                            ord.order_type = 'Change'
                            AND act.name IN ('Circuit Creation', 'Reconfiguration')
                        )
                        OR (
                            ord.order_type = 'Cease'
                            AND act.name = 'Node & Circuit Deletion'
                        )
                    )
                )
                OR (
                    per.role = 'GSPSG_ME'
                    AND ord.order_type = 'Provide'
                    AND act.name = 'Circuit Configuration-STM'
                )
            )
        )
        OR (
            prd.network_product_code LIKE 'GGW%'
            AND act.name IN (
                'GSDT Co-ordination Wrk-BQ',
                'GSDT Co-ordination WK-BQ',
                'GSDT Co-ordination Work'
            )
            AND (
                (
                    ord.order_type = 'Provide'
                    AND (
                        per.role = 'GSP_LTC_GW'
                        OR per.role LIKE 'ODC_%'
                        OR per.role LIKE 'RDC_%'
                    )
                )
                OR (
                    ord.order_type = 'Cease'
                    AND (
                        per.role = 'GSDT31'
                        OR per.role LIKE 'ODC_%'
                        OR per.role LIKE 'RDC_%'
                    )
                )
            )
        )
    )
    AND act.status = 'COM'
    AND act.completed_date BETWEEN '2023-05-26'
    AND '2023-06-25'
    AND ord.order_code IN ('ZEP9265003');
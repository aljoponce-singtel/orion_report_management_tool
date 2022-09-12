SELECT
    DISTINCT ord.id
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
            prd.network_product_code LIKE 'DGN%'
            AND ord.order_type IN ('Provide', 'Change', 'Cease')
            AND (
                per.role LIKE 'ODC_%'
                OR per.role LIKE 'RDC_%'
                OR per.role LIKE 'GSPSG_%'
            )
            AND act.name = 'GSDT Co-ordination Wrk-BQ'
            AND act.status = 'COM'
        )
        OR (
            prd.network_product_code LIKE 'DME%'
            AND ord.order_type IN ('Provide', 'Change', 'Cease')
            AND (
                per.role LIKE 'ODC_%'
                OR per.role LIKE 'RDC_%'
                OR per.role LIKE 'GSPSG_%'
            )
            AND act.name = 'GSDT Co-ordination Wrk-BQ'
            AND act.status = 'COM'
        )
        OR (
            prd.network_product_code = 'ELK0052'
            AND (
                (
                    ord.order_type IN ('Provide', 'Change')
                    AND (
                        per.role LIKE 'ODC_%'
                        OR per.role LIKE 'RDC_%'
                        OR per.role LIKE 'GSPSG_%'
                    )
                    AND act.name = 'Circuit Creation'
                    AND act.status = 'COM'
                )
                OR (
                    ord.order_type = 'Cease'
                    AND (
                        per.role LIKE 'ODC_%'
                        OR per.role LIKE 'RDC_%'
                        OR per.role LIKE 'GSPSG_%'
                    )
                    AND act.name = 'Node & Circuit Deletion'
                    AND act.status = 'COM'
                )
            )
        )
        OR (
            prd.network_product_code LIKE 'GGW%'
            AND (
                (
                    ord.order_type = 'Provide'
                    AND per.role = 'GSP_LTC_GW'
                    AND act.name = 'GSDT Co-ordination Work'
                    AND act.status = 'COM'
                )
                OR (
                    ord.order_type = 'Cease'
                    AND per.role = 'GSDT31'
                    AND act.name = 'GSDT Co-ordination Work'
                    AND act.status = 'COM'
                )
            )
        )
    )
    AND act.completed_date BETWEEN '{}'
    AND '{}';
SELECT
    DISTINCT ord.order_code,
    ord.order_status,
    ord.order_type,
    prd.network_product_code,
    per.role,
    act.name,
    act.status,
    act.completed_date
FROM
    `RestInterface_order` AS ord
    INNER JOIN `RestInterface_activity` AS act ON act.order_id = ord.id
    LEFT OUTER JOIN `RestInterface_person` AS per ON per.id = act.person_id
    LEFT OUTER JOIN `RestInterface_npp` AS npp ON npp.order_id = ord.id
    AND npp.level = 'Mainline'
    AND npp.status != 'Cancel'
    LEFT OUTER JOIN `RestInterface_product` AS prd ON prd.id = npp.product_id
WHERE
    (
        (
            prd.network_product_code LIKE 'DGN%%'
            AND ord.order_type IN ('Provide', 'Change', 'Cease')
            AND (
                per.`role` LIKE 'ODC_%%'
                OR per.`role` LIKE 'RDC_%%'
                OR per.`role` LIKE 'GSPSG_%%'
            )
            AND act.name = 'GSDT Co-ordination Wrk-BQ'
            AND act.status = 'COM'
        )
        OR (
            prd.network_product_code LIKE 'DME%%'
            AND ord.order_type IN ('Provide', 'Change', 'Cease')
            AND (
                per.`role` LIKE 'ODC_%%'
                OR per.`role` LIKE 'RDC_%%'
                OR per.`role` LIKE 'GSPSG_%%'
            )
            AND act.name IN (
                'GSDT Co-ordination Wrk-BQ',
                'GSDT Co-ordination Work'
            )
            AND act.status = 'COM'
        )
        OR (
            prd.network_product_code IN (
                'ELK0052',
                'ELK0053',
                'ELK0089',
                'ELK0091',
                'ELK0092'
            )
            AND (
                (
                    ord.order_type IN ('Provide', 'Change')
                    AND (
                        per.`role` LIKE 'ODC_%%'
                        OR per.`role` LIKE 'RDC_%%'
                        OR per.`role` LIKE 'GSPSG_%%'
                    )
                    AND act.name = 'Circuit Creation'
                    AND act.status = 'COM'
                )
                OR (
                    ord.order_type = 'Cease'
                    AND (
                        per.`role` LIKE 'ODC_%%'
                        OR per.`role` LIKE 'RDC_%%'
                        OR per.`role` LIKE 'GSPSG_%%'
                    )
                    AND act.name = 'Node & Circuit Deletion'
                    AND act.status = 'COM'
                )
            )
        )
        OR (
            (
                ord.order_type = 'Provide'
                AND per.`role` = 'GSP_LTC_GW'
                AND act.name = 'GSDT Co-ordination Work'
                AND act.status = 'COM'
            )
            OR (
                ord.order_type = 'Cease'
                AND per.`role` = 'GSDT31'
                AND act.name = 'GSDT Co-ordination Work'
                AND act.status = 'COM'
            )
        )
    )
    AND ord.order_code IN ('ZCA8160001');
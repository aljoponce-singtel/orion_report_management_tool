SELECT
    DISTINCT (
        CASE
            WHEN prd.network_product_code LIKE 'DGN%' THEN 'Diginet'
            WHEN prd.network_product_code LIKE 'DME%' THEN 'MetroE'
            WHEN prd.network_product_code IN (
                'ELK0052',
                'ELK0053',
                'ELK0089',
                'ELK0091',
                'ELK0092'
            ) THEN 'MegaPop (CE)'
            WHEN prd.network_product_code LIKE 'GGW%' THEN 'Gigawave'
            ELSE NULL
        END
    ) AS service,
    ord.order_code,
    cus.name,
    ord.current_crd,
    ord.service_number,
    ord.order_status,
    ord.order_type,
    prd.network_product_code,
    per.role,
    CAST(act.activity_code AS SIGNED INTEGER) AS step_no,
    act.name,
    act.status,
    act.due_date,
    act.completed_date
FROM
    RestInterface_order ord
    JOIN RestInterface_activity act ON act.order_id = ord.id
    LEFT JOIN RestInterface_person per ON per.id = act.person_id
    LEFT JOIN RestInterface_customer cus ON cus.id = ord.customer_id
    LEFT JOIN RestInterface_npp npp ON npp.order_id = ord.ID
    AND npp.level = 'Mainline'
    AND npp.status <> 'Cancel'
    LEFT JOIN RestInterface_product prd ON prd.id = npp.product_id
WHERE
    ord.id IN ({ })
    AND (
        (
            prd.network_product_code LIKE 'DGN%'
            AND (
                (
                    ord.order_type IN ('Provide', 'Change')
                    AND (
                        per.role LIKE 'ODC_%'
                        OR per.role LIKE 'RDC_%'
                        OR per.role LIKE 'GSPSG_%'
                    )
                    AND (
                        (
                            act.name = 'GSDT Co-ordination Wrk-BQ'
                            AND act.status = 'COM'
                            AND act.completed_date BETWEEN '{}'
                            AND '{}'
                        )
                        OR act.name = 'Circuit Creation'
                    )
                )
                OR (
                    ord.order_type = 'Cease'
                    AND (
                        per.role LIKE 'ODC_%'
                        OR per.role LIKE 'RDC_%'
                        OR per.role LIKE 'GSPSG_%'
                    )
                    AND (
                        (
                            act.name = 'GSDT Co-ordination Wrk-BQ'
                            AND act.status = 'COM'
                            AND act.completed_date BETWEEN '{}'
                            AND '{}'
                        )
                        OR act.name IN (
                            'Node & Cct Del (DN-ISDN)',
                            'Node & Cct Deletion (DN)'
                        )
                    )
                )
            )
        )
        OR (
            prd.network_product_code LIKE 'DME%'
            AND (
                (
                    ord.order_type IN ('Provide')
                    AND (
                        per.role LIKE 'ODC_%'
                        OR per.role LIKE 'RDC_%'
                        OR per.role LIKE 'GSPSG_%'
                    )
                    AND (
                        (
                            act.name IN (
                                'GSDT Co-ordination Wrk-BQ',
                                'GSDT Co-ordination Work'
                            )
                            AND act.status = 'COM'
                            AND act.completed_date BETWEEN '{}'
                            AND '{}'
                        )
                        OR act.name = 'Circuit Creation'
                    )
                )
                OR (
                    ord.order_type IN ('Change')
                    AND (
                        (
                            (
                                per.role LIKE 'ODC_%'
                                OR per.role LIKE 'RDC_%'
                                OR per.role LIKE 'GSPSG_%'
                            )
                            AND act.name IN (
                                'GSDT Co-ordination Wrk-BQ',
                                'GSDT Co-ordination Work'
                            )
                            AND act.status = 'COM'
                            AND act.completed_date BETWEEN '{}'
                            AND '{}'
                        )
                        OR (
                            (
                                per.role LIKE 'ODC_%'
                                OR per.role LIKE 'RDC_%'
                                OR per.role LIKE 'GSP%'
                            )
                            AND act.name IN ('Circuit Creation', 'Change Speed Configure')
                        )
                    )
                )
                OR (
                    ord.order_type = 'Cease'
                    AND (
                        per.role LIKE 'ODC_%'
                        OR per.role LIKE 'RDC_%'
                        OR per.role LIKE 'GSPSG_%'
                    )
                    AND (
                        (
                            act.name IN (
                                'GSDT Co-ordination Wrk-BQ',
                                'GSDT Co-ordination Work'
                            )
                            AND act.status = 'COM'
                            AND act.completed_date BETWEEN '{}'
                            AND '{}'
                        )
                        OR act.name = 'Node & Circuit Deletion'
                    )
                )
            )
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
                        per.role LIKE 'ODC_%'
                        OR per.role LIKE 'RDC_%'
                        OR per.role LIKE 'GSPSG_%'
                    )
                    AND act.name = 'Circuit Creation'
                    AND act.status = 'COM'
                    AND act.completed_date BETWEEN '{}'
                    AND '{}'
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
                    AND act.completed_date BETWEEN '{}'
                    AND '{}'
                )
            )
        )
        OR (
            prd.network_product_code LIKE 'GGW%'
            AND (
                (
                    ord.order_type = 'Provide'
                    AND (
                        (
                            per.role = 'GSP_LTC_GW'
                            AND act.name = 'GSDT Co-ordination Work'
                            AND act.status = 'COM'
                            AND act.completed_date BETWEEN '{}'
                            AND '{}'
                        )
                        OR (
                            (
                                per.role LIKE 'ODC_%'
                                OR per.role LIKE 'RDC_%'
                                OR per.role LIKE 'GSPSG_%'
                            )
                            AND act.name = 'Circuit Creation'
                        )
                    )
                )
                OR (
                    ord.order_type = 'Cease'
                    AND (
                        (
                            per.role = 'GSDT31'
                            AND act.name = 'GSDT Co-ordination Work'
                            AND act.status = 'COM'
                            AND act.completed_date BETWEEN '{}'
                            AND '{}'
                        )
                        OR (
                            per.role = 'GSP_LTC_GW'
                            AND act.name = 'Circuit Removal from NMS'
                        )
                    )
                )
            )
        )
    )
ORDER BY
    service,
    ord.order_type DESC,
    act.name,
    step_no,
    per.role,
    ord.order_code;
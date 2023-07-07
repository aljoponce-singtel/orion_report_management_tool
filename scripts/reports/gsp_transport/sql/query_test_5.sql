SELECT
    DISTINCT (
        CASE
            WHEN (
                prd.network_product_code LIKE 'DGN%'
                OR prd.network_product_code LIKE 'DEK%'
                OR prd.network_product_code LIKE 'DLC%'
            ) THEN 'Diginet'
            WHEN prd.network_product_code LIKE 'DME%' THEN 'MetroE'
            WHEN prd.network_product_code IN (
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
    ord.order_code IN ('ZEP9265003')
    AND (
        (
            (
                prd.network_product_code LIKE 'DGN%'
                OR prd.network_product_code LIKE 'DEK%'
                OR prd.network_product_code LIKE 'DLC%'
            )
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
                            AND act.completed_date BETWEEN '2023-05-26'
                            AND '2023-06-25'
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
                            AND act.completed_date BETWEEN '2023-05-26'
                            AND '2023-06-25'
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
                            AND act.completed_date BETWEEN '2023-05-26'
                            AND '2023-06-25'
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
                            AND act.completed_date BETWEEN '2023-05-26'
                            AND '2023-06-25'
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
                            AND act.completed_date BETWEEN '2023-05-26'
                            AND '2023-06-25'
                        )
                        OR act.name = 'Node & Circuit Deletion'
                    )
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
                    ord.order_type = 'Provide'
                    AND (
                        (
                            (
                                per.role LIKE 'ODC_%'
                                OR per.role LIKE 'RDC_%'
                                OR per.role LIKE 'GSPSG_%'
                            )
                            AND act.name = 'Circuit Creation'
                        )
                        OR (
                            per.role = 'GSPSG_ME'
                            AND act.name = 'Circuit Configuration-STM'
                        )
                    )
                    AND act.status = 'COM'
                    AND act.completed_date BETWEEN '2023-05-26'
                    AND '2023-06-25'
                )
                OR (
                    ord.order_type = 'Change'
                    AND (
                        per.role LIKE 'ODC_%'
                        OR per.role LIKE 'RDC_%'
                        OR per.role LIKE 'GSPSG_%'
                    )
                    AND act.name IN ('Circuit Creation', 'Reconfiguration')
                    AND act.status = 'COM'
                    AND act.completed_date BETWEEN '2023-05-26'
                    AND '2023-06-25'
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
                    AND act.completed_date BETWEEN '2023-05-26'
                    AND '2023-06-25'
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
                            (
                                per.role = 'GSP_LTC_GW'
                                OR per.role LIKE 'ODC_%'
                                OR per.role LIKE 'RDC_%'
                            )
                            AND act.name IN (
                                'GSDT Co-ordination Wrk-BQ',
                                'GDST Co-ordination WK-BQ',
                                'GSDT Co-ordination Work'
                            )
                            AND act.status = 'COM'
                            AND act.completed_date BETWEEN '2023-05-26'
                            AND '2023-06-25'
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
                            (
                                per.role = 'GSDT31'
                                OR per.role LIKE 'ODC_%'
                                OR per.role LIKE 'RDC_%'
                            )
                            AND act.name IN (
                                'GSDT Co-ordination Wrk-BQ',
                                'GDST Co-ordination WK-BQ',
                                'GSDT Co-ordination Work'
                            )
                            AND act.status = 'COM'
                            AND act.completed_date BETWEEN '2023-05-26'
                            AND '2023-06-25'
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
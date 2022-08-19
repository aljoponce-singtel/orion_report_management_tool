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
    ORD.order_code,
    ORD.current_crd,
    ORD.service_number,
    ORD.order_status,
    ORD.order_type,
    PRD.network_product_code,
    PER.role,
    CAST(ACT.activity_code AS UNSIGNED) AS step_no,
    ACT.name,
    ACT.status,
    ACT.due_date,
    ACT.completed_date
FROM
    RestInterface_order ORD
    JOIN RestInterface_activity ACT ON ACT.order_id = ORD.id
    LEFT JOIN RestInterface_person PER ON PER.id = ACT.person_id
    LEFT JOIN RestInterface_npp NPP ON NPP.order_id = ORD.ID
    AND NPP.level = 'Mainline'
    AND NPP.status <> 'Cancel'
    LEFT JOIN RestInterface_product PRD ON PRD.id = NPP.product_id
WHERE
    ORD.id IN (
        SELECT
            id
        FROM
            COM_QUEUES
    )
    AND (
        (
            PRD.network_product_code LIKE 'DGN%'
            AND (
                (
                    ORD.order_type IN ('Provide', 'Change')
                    AND (
                        PER.role LIKE 'ODC_%'
                        OR PER.role LIKE 'RDC_%'
                        OR PER.role LIKE 'GSPSG_%'
                    )
                    AND (
                        (
                            ACT.name = 'GSDT Co-ordination Wrk-BQ'
                            AND ACT.status = 'COM'
                            AND ACT.completed_date BETWEEN '2022-07-01'
                            AND '2022-07-31'
                        )
                        OR ACT.name = 'Circuit Creation'
                    )
                )
                OR (
                    ORD.order_type = 'Cease'
                    AND (
                        PER.role LIKE 'ODC_%'
                        OR PER.role LIKE 'RDC_%'
                        OR PER.role LIKE 'GSPSG_%'
                    )
                    AND (
                        (
                            ACT.name = 'GSDT Co-ordination Wrk-BQ'
                            AND ACT.status = 'COM'
                            AND ACT.completed_date BETWEEN '2022-07-01'
                            AND '2022-07-31'
                        )
                        OR ACT.name IN (
                            'Node & Cct Del (DN-ISDN)',
                            'Node & Cct Deletion (DN)'
                        )
                    )
                )
            )
        )
        OR (
            PRD.network_product_code LIKE 'DME%'
            AND (
                (
                    ORD.order_type IN ('Provide')
                    AND (
                        PER.role LIKE 'ODC_%'
                        OR PER.role LIKE 'RDC_%'
                        OR PER.role LIKE 'GSPSG_%'
                    )
                    AND (
                        (
                            ACT.name = 'GSDT Co-ordination Wrk-BQ'
                            AND ACT.status = 'COM'
                            AND ACT.completed_date BETWEEN '2022-07-01'
                            AND '2022-07-31'
                        )
                        OR ACT.name = 'Circuit Creation'
                    )
                )
                OR (
                    ORD.order_type IN ('Change')
                    AND (
                        (
                            (
                                PER.role LIKE 'ODC_%'
                                OR PER.role LIKE 'RDC_%'
                                OR PER.role LIKE 'GSPSG_%'
                            )
                            AND ACT.name = 'GSDT Co-ordination Wrk-BQ'
                            AND ACT.status = 'COM'
                            AND ACT.completed_date BETWEEN '2022-07-01'
                            AND '2022-07-31'
                        )
                    )
                    OR (
                        (
                            PER.role LIKE 'ODC_%'
                            OR PER.role LIKE 'RDC_%'
                            OR PER.role LIKE 'GSP%'
                        )
                        AND ACT.name IN ('Circuit Creation', 'Change Speed Configure')
                    )
                )
                OR (
                    ORD.order_type = 'Cease'
                    AND (
                        PER.role LIKE 'ODC_%'
                        OR PER.role LIKE 'RDC_%'
                        OR PER.role LIKE 'GSPSG_%'
                    )
                    AND (
                        (
                            ACT.name = 'GSDT Co-ordination Wrk-BQ'
                            AND ACT.status = 'COM'
                            AND ACT.completed_date BETWEEN '2022-07-01'
                            AND '2022-07-31'
                        )
                        OR ACT.name = 'Node & Circuit Deletion'
                    )
                )
            )
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
                    AND ACT.completed_date BETWEEN '2022-07-01'
                    AND '2022-07-31'
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
                    AND ACT.completed_date BETWEEN '2022-07-01'
                    AND '2022-07-31'
                )
            )
        )
        OR (
            PRD.network_product_code LIKE 'GGW%'
            AND (
                (
                    ORD.order_type = 'Provide'
                    AND (
                        (
                            PER.role = 'GSP_LTC_GW'
                            AND ACT.name = 'GSDT Co-ordination Work'
                            AND ACT.status = 'COM'
                            AND ACT.completed_date BETWEEN '2022-07-01'
                            AND '2022-07-31'
                        )
                        OR (
                            (
                                PER.role LIKE 'ODC_%'
                                OR PER.role LIKE 'RDC_%'
                                OR PER.role LIKE 'GSPSG_%'
                            )
                            AND ACT.name = 'Circuit Creation'
                        )
                    )
                )
                OR (
                    ORD.order_type = 'Cease'
                    AND (
                        (
                            PER.role = 'GSDT31'
                            AND ACT.name = 'GSDT Co-ordination Work'
                            AND ACT.status = 'COM'
                            AND ACT.completed_date BETWEEN '2022-07-01'
                            AND '2022-07-31'
                        )
                        OR (
                            PER.role = 'GSP_LTC_GW'
                            AND ACT.name = 'Circuit Removal from NMS'
                        )
                    )
                )
            )
        )
    )
ORDER BY
    Service,
    ORD.order_type DESC,
    ACT.name,
    step_no,
    PER.role,
    ORD.order_code;
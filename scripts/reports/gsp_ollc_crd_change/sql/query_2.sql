SELECT
    ORD.arbor_disp,
    ORD.current_crd,
    ORD.order_type,
    ORD.order_status,
    ORD.taken_date,
    AAA.workorderno,
    BBB.last_crd,
    CCC.customerrequired AS today_crd,
    (
        SELECT
            value
        FROM
            pegasusmulesoft.RestInterface_metadata
        ORDER BY
            id DESC
        LIMIT
            1 OFFSET 1
    ) AS metadata_value,
    CCC.updated_at AS hist_updated_at
FROM
    (
        SELECT
            workorderno,
            MAX(HIST.customerrequired) last_crd
        FROM
            o2pprod.Order_CRD_History HIST
        WHERE
            HIST.updated_at < (
                SELECT
                    value
                FROM
                    pegasusmulesoft.RestInterface_metadata
                ORDER BY
                    id DESC
                LIMIT
                    1 OFFSET 1
            )
        GROUP BY
            workorderno
    ) BBB,
    (
        SELECT
            workorderno,
            COUNT(1)
        FROM
            o2pprod.Order_CRD_History
        GROUP BY
            workorderno
        having
            COUNT(1) > 1
    ) AAA,
    (
        SELECT
            workorderno,
            customerrequired,
            updated_at
        FROM
            o2pprod.Order_CRD_History HIST
        WHERE
            HIST.updated_at > (
                SELECT
                    value
                FROM
                    pegasusmulesoft.RestInterface_metadata
                ORDER BY
                    id DESC
                LIMIT
                    1 OFFSET 1
            )
    ) CCC,
    RestInterface_order ORD,
    RestInterface_npp NPP,
    RestInterface_product PROD
WHERE
    AAA.workorderno = BBB.workorderno
    AND AAA.workorderno = CCC.workorderno
    AND BBB.workorderno = CCC.workorderno
    AND ORD.order_code = CCC.workorderno
    AND ORD.id = NPP.order_id
    AND NPP.product_id = PROD.id
    AND ORD.order_type = 'Cease'
    AND ORD.order_status NOT IN (
        'Cancelled',
        'Closed',
        'Completed',
        'Pending Cancellation'
    )
    AND PROD.product_type_id = 1082;
SELECT
    DISTINCT ORD.arbor_disp,
    ORD.current_crd AS crd_changed_to,
    ORD.order_type,
    ORD.order_status,
    ORD.taken_date,
    AAA.workorderno,
    BBB.last_crd AS crd_changed_from,
    CCC.customerrequired AS today_crd,
    CCC.updated_at AS crd_updated_in_orion
FROM
    (
        SELECT
            workorderno,
            MAX(HIST.customerrequired) last_crd
        FROM
            o2pprod.Order_CRD_History HIST
        WHERE
            DATE_FORMAT(HIST.updated_at, '%y-%m-%d') < DATE_FORMAT(NOW(), '%y-%m-%d')
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
            DATE_FORMAT(HIST.updated_at, '%y-%m-%d') = DATE_FORMAT(NOW(), '%y-%m-%d')
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
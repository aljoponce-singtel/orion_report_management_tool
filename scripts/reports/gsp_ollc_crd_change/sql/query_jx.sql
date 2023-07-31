select
    ORD.arbor_disp,
    ORD.current_crd,
    ORD.order_type,
    ORD.order_status,
    ORD.taken_date,
    AAA.workorderno,
    BBB.last_crd,
    CCC.customerrequired today_crd,
    CCC.updated_at
from
    (
        select
            workorderno,
            max(hist.customerrequired) last_crd
        from
            o2pprod.Order_CRD_History hist
        where
            date_format(hist.updated_at, '%y-%m-%d') < date_format(now(), '%y-%m-%d')
        group by
            workorderno
    ) BBB,
    (
        select
            workorderno,
            count(1)
        from
            o2pprod.Order_CRD_History
        group by
            workorderno
        having
            count(1) > 1
    ) AAA,
    (
        select
            workorderno,
            customerrequired,
            updated_at
        from
            o2pprod.Order_CRD_History hist
        where
            date_format(hist.updated_at, '%y-%m-%d') = date_format(now(), '%y-%m-%d')
    ) CCC,
    RestInterface_order ORD,
    RestInterface_npp NPP,
    RestInterface_product PROD
where
    AAA.workorderno = BBB.workorderno
    and AAA.workorderno = CCC.workorderno
    and BBB.workorderno = CCC.workorderno
    and ORD.order_code = CCC.workorderno
    and ORD.id = NPP.order_id
    and NPP.product_id = PROD.id
    and ORD.order_type = 'Cease'
    and ORD.order_status not in (
        'Cancelled',
        'Closed',
        'Completed',
        'Pending Cancellation'
    )
    and PROD.product_type_id = 1082;
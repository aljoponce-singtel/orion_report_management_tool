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
    RestInterface_order ord
    JOIN RestInterface_activity act ON act.order_id = ord.id
    LEFT JOIN RestInterface_person per ON per.id = act.person_id
    LEFT JOIN RestInterface_npp npp ON npp.order_id = ord.ID
    AND npp.level = 'Mainline'
    AND npp.status <> 'Cancel'
    LEFT JOIN RestInterface_product prd ON prd.id = npp.product_id
WHERE
    (
        prd.network_product_code LIKE 'DGN%'
        OR prd.network_product_code LIKE 'DME%'
        OR prd.network_product_code IN (
            'ELK0052',
            'ELK0053',
            'ELK0089',
            'ELK0091',
            'ELK0092'
        )
    )
    AND act.name IN (
        'GSDT Co-ordination Wrk-BQ',
        'GSDT Co-ordination Work',
        'Circuit Creation',
        'Node & Circuit Deletion'
    )
    AND ord.order_code IN ('ZCP0828001');
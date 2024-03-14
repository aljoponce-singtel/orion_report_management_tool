-- Active: 1709279497631@@127.0.0.1@54406@o2puat
use o2puat;

SELECT DISTINCT
    PRJ.project_code,
    ORD.order_code,
    PRJTRK.product_category,
    PRJTRK.circuit_layer,
    PRJTRK.type_of_product,
    PRJTRK.type_of_work,
    ORD.current_crd,
    ORD.order_status,
    ORD.order_type,
    CAST(
        ACT.activity_code AS SIGNED INTEGER
    ) AS actstepno,
    ACT.name,
    ACT.status,
    ACT.due_date,
    ACT.completed_date
FROM
    o2puat.RestInterface_order ORD
    JOIN o2ptest.project_tracker_group PRJTRK ON PRJTRK.order_id = ORD.id
    JOIN o2puat.RestInterface_activity ACT ON ORD.id = ACT.order_id
    LEFT JOIN o2puat.RestInterface_project PRJ ON ORD.project_id = PRJ.id
WHERE
    ORD.project_tracker_group_name = (
        SELECT svc_ord_no
        FROM o2ptest.project_tracker_group
        LIMIT 1
    )
ORDER BY ORD.order_code, actstepno;
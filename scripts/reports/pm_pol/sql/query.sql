SELECT
    DISTINCT ORD.order_code AS OrderNumber,
    ORD.order_type AS OrderType,
    ORD.order_status AS OrderStatus,
    ORD.order_priority AS OrderPriority,
    ORD.current_crd AS CurrentCRD,
    ORD.initial_crd AS InitialCRD,
    ORD.taken_date AS TakenDate,
    ORD.sde_received_date AS SDERcvdDate,
    ORD.arbor_service_type AS ArborSvcType,
    CUS.name AS CustomerName,
    PRJ.project_code AS ProjectID,
    ORD.service_number AS SvcNumber,
    ORD.service_action_type AS SvcActionType,
    ORD.business_sector AS Sector,
    BRN.brn AS BRN,
    CAST(ACT.activity_code AS SIGNED INTEGER) AS ActivityCode,
    ACT.name AS ActivityName,
    ACT.status AS ActivityStatus,
    (
        CASE
            WHEN ORD.delivery_status = 1 THEN 'Wip on Track'
            WHEN ORD.delivery_status = 2 THEN 'Wip at Risk'
            WHEN ORD.delivery_status = 3 THEN 'Wip Delay'
            WHEN ORD.delivery_status = 4 THEN 'Delivered'
            WHEN ORD.delivery_status = 5 THEN 'Delivered Delay'
            ELSE 'N/A'
        END
    ) AS DeliveryStatus
FROM
    RestInterface_order ORD
    JOIN RestInterface_activity ACT ON ACT.order_id = ORD.id
    JOIN RestInterface_customerbrnmapping BRN ON BRN.id = ORD.customer_brn_id
    JOIN RestInterface_customer CUS ON CUS.id = BRN.customer_id
    JOIN RestInterface_project PRJ ON PRJ.id = ORD.project_id
WHERE
    ACT.tag_name = "Pegasus"
    AND ORD.business_sector NOT LIKE 'Enterprise Sales (Government%'
    AND ORD.assignee = 'PM'
    AND ORD.taken_date > '2018-09-31'
    AND PRJ.project_code REGEXP "^([a-zA-Z]{3}|[a-zA-Z]{5})[a-zA-Z0-9]{2}[0-9]{2}[A-Z]$"
ORDER BY
    OrderNumber,
    ActivityCode;
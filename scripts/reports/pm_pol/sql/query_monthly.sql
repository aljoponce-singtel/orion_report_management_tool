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
    ORD.arbor_disp AS ArborSvcDisp,
    ORD.ord_action_type AS OrderActionType,
    PRD.network_product_desc AS ProductDescription,
    (
        SELECT
            CONCAT(given_name, ", ", family_name)
        FROM
            RestInterface_contactdetails
        WHERE
            order_id = ORD.id
            AND contact_type = 'Project Manager'
        LIMIT
            1
    ) AS ProjectManager,
    CUS.name AS CustomerName,
    PRJ.project_code AS ProjectID,
    ORD.service_number AS SvcNumber,
    ORD.service_action_type AS SvcActionType
FROM
    RestInterface_order ORD
    LEFT JOIN RestInterface_customer CUS ON CUS.id = ORD.customer_id
    LEFT JOIN RestInterface_project PRJ ON PRJ.id = ORD.project_id
    LEFT JOIN RestInterface_npp NPP ON NPP.order_id = ORD.id
    AND NPP.level = 'Mainline'
    AND NPP.status <> 'Cancel'
    LEFT JOIN RestInterface_product PRD ON PRD.id = NPP.product_id
WHERE
    ORD.business_sector NOT LIKE 'Enterprise Sales (Government%'
    AND ORD.taken_date BETWEEN '{start_date}'
    AND '{end_date}'
ORDER BY
    ORD.order_code;
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
    CON.ProjectManager,
    CUS.name AS CustomerName,
    PRJ.project_code AS ProjectID,
    ORD.service_number AS SvcNumber,
    ORD.service_action_type AS SvcActionType
FROM
    RestInterface_order ORD
    JOIN RestInterface_project PRJ ON PRJ.id = ORD.project_id
    LEFT JOIN RestInterface_customer CUS ON CUS.id = ORD.customer_id
    LEFT JOIN (
        SELECT
            order_id,
            GROUP_CONCAT(
                DISTINCT TRIM(CONCAT(given_name, ", ", family_name))
                ORDER BY
                    given_name SEPARATOR '; '
            ) AS ProjectManager
        FROM
            RestInterface_contactdetails
        WHERE
            contact_type = 'Project Manager'
        GROUP BY
            order_id
    ) CON ON CON.order_id = ORD.id
    LEFT JOIN RestInterface_npp NPP ON NPP.order_id = ORD.id
    AND NPP.level = 'Mainline'
    AND NPP.status <> 'Cancel'
    LEFT JOIN RestInterface_product PRD ON PRD.id = NPP.product_id
WHERE
    ORD.business_sector NOT LIKE 'Enterprise Sales (Government%'
    AND ORD.taken_date BETWEEN '{start_date}'
    AND '{end_date}'
    AND PRJ.project_code REGEXP "^([a-zA-Z]{{3}}|[a-zA-Z]{{5}})[a-zA-Z0-9]{{2}}[0-9]{{2}}[A-Z]$"
ORDER BY
    ORD.order_code;
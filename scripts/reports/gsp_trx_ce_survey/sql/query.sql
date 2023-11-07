SELECT
    DISTINCT ORD.order_code AS OrderNo,
    CUS.name AS CustomerName,
    CUS_BRN.brn AS BRN,
    PRD.network_product_desc AS ProductDescription,
    ORD.order_type AS OrderType,
    ORD.ord_action_type AS OrderActionType,
    ORD.order_status AS OrderStatus,
    ORD.assignee AS Assignee,
    ORD.service_action_type AS ServiceActionType,
    ORD.arbor_disp AS ServiceType,
    ORD.business_sector AS Sector,
    ORD.initial_crd AS InitialCRD,
    ORD.close_date AS CloseDate,
    ORD.current_crd AS CommissionDate,
    ORD.taken_date AS OrdCreationDate,
    PRJ.project_code AS ProjectID,
    CON.contact_type AS ContactType,
    CON.family_name AS FirstName,
    CON.given_name AS LastName,
    CON.work_phone_no AS WorkPhoneNo,
    CON.mobile_no AS MobileNo,
    CON.email_address AS Email,
    SITE.location AS Address
FROM
    RestInterface_order ORD
    LEFT JOIN RestInterface_project PRJ ON ORD.project_id = PRJ.id
    LEFT JOIN RestInterface_customerbrnmapping CUS_BRN ON ORD.customer_brn_id = CUS_BRN.id
    LEFT JOIN RestInterface_customer CUS ON CUS_BRN.customer_id = CUS.id
    LEFT JOIN RestInterface_contactdetails CON ON ORD.id = CON.order_id
    AND CON.contact_type IN (
        'A-end-Cust',
        'Clarification-Cust',
        'Maintenance-Cust',
        'Technical-Cust'
    )
    LEFT JOIN RestInterface_npp NPP ON ORD.id = NPP.order_id
    AND NPP.level = 'MainLine'
    LEFT JOIN RestInterface_product PRD ON NPP.product_id = PRD.id
    LEFT JOIN RestInterface_site SITE ON SITE.id = ORD.site_id
WHERE
    ORD.order_type <> 'Cease'
    AND ORD.order_status = 'Closed'
    AND ORD.close_date IS NOT NULL
    AND ORD.close_date > DATE_SUB(NOW(), INTERVAL 8 DAY)
    AND ORD.close_date < DATE(NOW())
    AND ORD.id NOT IN (
        SELECT
            ORDEV.id
        FROM
            RestInterface_order ORDEV
            JOIN RestInterface_npp NPPEV ON NPPEV.order_id = ORDEV.id
            AND NPPEV.level = "Mainline"
            AND ORDEV.service_number LIKE 'EV%'
            JOIN RestInterface_product PRDEV ON PRDEV.id = NPPEV.product_id
            AND PRDEV.network_product_code != "SGN0005"
        WHERE
            ORDEV.close_date > DATE_SUB(NOW(), INTERVAL 8 DAY)
            AND ORDEV.close_date < DATE(NOW())
    )
ORDER BY
    CloseDate,
    OrderNo,
    ContactType,
    FirstName,
    LastName;
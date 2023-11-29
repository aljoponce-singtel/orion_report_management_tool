SELECT
    DISTINCT ORD.order_code AS 'OrderNo',
    CUS.name AS 'CustomerName',
    BRN.brn AS 'BRN',
    PRD.network_product_desc AS 'ProductDescription',
    ORD.order_type AS 'OrderType',
    ORD.ord_action_type AS 'OrderActionType',
    ORD.order_status AS 'OrderStatus',
    ORD.service_action_type AS 'ServiceActionType',
    ORD.arbor_disp AS 'ServiceType',
    ORD.business_sector AS 'Sector',
    ORD.initial_crd AS 'InitialCRD',
    ORD.close_date AS 'CloseDate',
    ORD.current_crd AS 'CommissionDate',
    ORD.taken_date AS 'OrdCreationDate',
    PRJ.project_code AS 'ProjectID',
    CON.contact_type AS 'ContactType',
    CON.family_name AS 'FirstName',
    CON.given_name AS 'LastName',
    CON.work_phone_no AS 'WorkPhoneNo',
    CON.mobile_no AS 'MobileNo',
    CON.email_address AS 'Email',
    SITE.location AS 'Location',
    ORD.assignee AS 'Assignee',
    (
        CASE
            WHEN ORDEVOLVE.id IS NOT NULL THEN 'yes'
            ELSE 'no'
        END
    ) AS 'IsEvolve',
    ORD.service_number AS 'ServiceNumber',
    ORDEVOLVE.PackageDescription AS 'PackageDescription',
    ORDEVOLVE.ComponentDescription AS 'ComponentDescription'
FROM
    RestInterface_order ORD
    LEFT JOIN (
        SELECT
            DISTINCT ORDEV.id,
            GROUP_CONCAT(
                DISTINCT BILLEV.package_description
                ORDER BY
                    BILLEV.package_description SEPARATOR ', '
            ) AS 'PackageDescription',
            GROUP_CONCAT(
                DISTINCT BILLEV.component_description
                ORDER BY
                    BILLEV.component_description SEPARATOR ', '
            ) AS 'ComponentDescription'
        FROM
            RestInterface_order ORDEV
            JOIN RestInterface_billing BILLEV ON BILLEV.order_id = ORDEV.id
        WHERE
            ORDEV.service_number LIKE '%EV%'
            OR LOWER(BILLEV.package_description) LIKE '%evolve%'
            OR LOWER(BILLEV.component_description) LIKE '%evolve%'
        GROUP BY
            ORDEV.id
    ) ORDEVOLVE ON ORDEVOLVE.id = ORD.id
    JOIN RestInterface_contactdetails CON ON CON.order_id = ORD.id
    AND CON.contact_type IN (
        'A-end-Cust',
        'B-end-Cust',
        'Clarification-Cust',
        'Maintenance-Cust',
        'Technical-Cust'
    )
    LEFT JOIN RestInterface_customer CUS ON CUS.id = ORD.customer_id
    LEFT JOIN RestInterface_customerbrnmapping BRN ON BRN.id = ORD.customer_brn_id
    LEFT JOIN RestInterface_npp NPP ON NPP.order_id = ORD.id
    AND NPP.level = 'Mainline'
    AND NPP.status != 'Cancel'
    LEFT JOIN RestInterface_product PRD ON PRD.id = NPP.product_id
    LEFT JOIN RestInterface_project PRJ ON PRJ.id = ORD.project_id
    LEFT JOIN RestInterface_site SITE ON SITE.id = ORD.site_id
WHERE
    ORD.service_action_type != 'Transfer SI'
    AND ORD.ord_action_type NOT IN (
        'Contract Renewal ',
        'Contract Renewal',
        'ConRenew (MS)'
    )
    AND (
        ORD.business_sector LIKE '%bizseg%'
        OR ORD.business_sector LIKE '%sgo%'
        OR ORD.business_sector LIKE '%ent and govt%'
        OR ORD.business_sector LIKE '%global business%'
    )
    AND ORD.order_status = 'Closed'
    AND ORD.order_type != 'Cease'
    AND ORD.current_crd > DATE_SUB(ORD.close_date, INTERVAL 30 day)
    AND ORD.current_crd < ORD.close_date
    AND ORD.close_date BETWEEN '{start_date}'
    AND '{end_date}'
ORDER BY
    ORD.order_code,
    CON.contact_type;
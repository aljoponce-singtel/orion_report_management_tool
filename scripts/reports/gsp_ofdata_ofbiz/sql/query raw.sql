SELECT
    DISTINCT ORD.order_code AS 'OrderNo',
    CUS.name AS 'CustomerName',
    BRN.brn AS 'BRN',
    PRD.network_product_desc AS 'ProductDescription',
    ORD.ord_action_type AS 'OrderType',
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
    SITE.location AS 'Address',
    ORD.assignee AS 'Assignee'
FROM
    RestInterface_order ORD
    LEFT JOIN RestInterface_contactdetails CON ON CON.order_id = ORD.id
    AND CON.contact_type IN (
        'A-end-Cust',
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
    AND ORD.ord_action_type != 'Contract Renewal '
    AND (
        ORD.business_sector LIKE '%bizseg%'
        OR ORD.business_sector LIKE '%sgo%'
        OR ORD.business_sector LIKE '%ent and govt%'
        OR ORD.business_sector LIKE '%global business%'
    )
    AND ORD.arbor_disp IN (
        'CN - Meg@pop Suite Of IP Services',
        'SingNet',
        'DigiNet',
        'ISDN',
        'CN - ConnectPlus IP VPN',
        'ConnectPlus E-Line',
        'ILC',
        'Software Defined Networking'
    )
    AND ORD.order_status = 'Closed'
    AND ORD.current_crd > DATE_SUB(ORD.close_date, INTERVAL 30 day)
    AND ORD.current_crd < ORD.close_date
    AND ORD.close_date BETWEEN '2023-07-31'
    AND '2023-08-06'
ORDER BY
    ORD.order_code,
    CON.contact_type;
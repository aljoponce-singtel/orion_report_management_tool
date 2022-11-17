SELECT
    DISTINCT ORD.order_code AS OrderCode,
    PRD.network_product_code AS NetworkProductCode,
    PER.role AS GroupID,
    ORD.current_crd AS CRD,
    ORD.taken_date AS TakenDate,
    ORD.order_type AS OrderType,
    ORD.service_number AS ServiceNumber,
    PRJ.project_code AS ProjectCode,
    CUS.name AS CustomerName,
    SITE.location AS AEndAddress,
    CON.given_name AS FamilyName,
    CON.family_name AS GivenName,
    CON.contact_type AS ContactType,
    CON.email_address AS EmailAddress,
    PAR.parameter_name AS ParameterName,
    PAR.parameter_value AS ParameterValue
FROM
    RestInterface_order ORD
    JOIN RestInterface_activity ACT ON ORD.id = ACT.order_id
    LEFT JOIN RestInterface_person PER ON ACT.person_id = PER.id
    LEFT JOIN RestInterface_project PRJ ON ORD.project_id = PRJ.id
    LEFT JOIN RestInterface_customer CUS ON ORD.customer_id = CUS.id
    LEFT JOIN RestInterface_site SITE ON ORD.site_id = SITE.id
    LEFT JOIN RestInterface_contactdetails CON ON ORD.id = CON.order_id
    AND CON.contact_type IN ({ })
    LEFT JOIN RestInterface_npp NPP ON ORD.id = NPP.order_id
    AND NPP.level = 'Mainline'
    AND NPP.status <> 'Cancel'
    LEFT JOIN RestInterface_parameter PAR ON NPP.id = PAR.npp_id
    AND PAR.parameter_name IN ({ })
    LEFT JOIN RestInterface_product PRD ON NPP.product_id = PRD.id
WHERE
    ORD.order_type IN ({ })
    AND PRD.network_product_code IN ({ })
    AND ORD.taken_date BETWEEN '{}'
    AND '{}'
ORDER BY
    OrderCode,
    ContactType,
    ParameterName;
USE o2puat;

SELECT DISTINCT
    PRJ.project_code,
    ORD.order_code AS OrderCode,
    ORD.service_number AS ServiceNumber,
    PRD.network_product_code AS ProductCode,
    CKT.circuit_code AS CircuitCode,
    ORD.taken_date AS TakenDate,
    ORD.current_crd AS CRD,
    ORD.order_status AS OrderStatus,
    ORD.order_type AS OrderType,
    ORD.ord_action_type AS OrderActionType,
    ORD.arbor_disp AS ArborServiceType,
    CAST(
        ACT.activity_code AS SIGNED INTEGER
    ) AS ActStepNo,
    ACT.name AS ActivityName,
    PER.email AS ActivityOwner,
    ACT.status AS ActivityStatus,
    -- IFNULL(DLY.reason, '') AS DelayReason,
    CUS.name AS CustomerName,
    -- ORD.customer_brn_id,
    -- SITE.location AS SiteAddress_A,
    -- SITE.second_location AS SiteAddress_B,
    -- SITE.second_location,
    BRN.brn,
    ORD.id AS OrderID,
    ACT.id AS ActivityID
FROM
    RestInterface_order ORD
    JOIN RestInterface_activity ACT ON ORD.id = ACT.order_id
    LEFT JOIN RestInterface_project PRJ ON PRJ.id = ORD.project_id
    LEFT JOIN RestInterface_circuit CKT ON ORD.circuit_id = CKT.id
    LEFT JOIN RestInterface_person PER ON PER.id = ACT.person_id
    LEFT JOIN RestInterface_user USR2 ON USR2.id = PER.user_id
    LEFT JOIN RestInterface_customer CUS ON CUS.id = ORD.customer_id
    LEFT JOIN RestInterface_npp NPP ON NPP.order_id = ORD.id
    AND NPP.level = 'Mainline'
    LEFT JOIN RestInterface_product PRD ON PRD.id = NPP.product_id
    LEFT JOIN RestInterface_site SITE ON SITE.id = ORD.site_id
    LEFT JOIN RestInterface_customerbrnmapping BRN ON BRN.id = ORD.customer_brn_id
WHERE
    -- ORD.order_code IN ("ZML7043003", "ZML7043004")
    CKT.circuit_code IN ("4017668")
ORDER BY CRD DESC, OrderCode, ActStepNo;
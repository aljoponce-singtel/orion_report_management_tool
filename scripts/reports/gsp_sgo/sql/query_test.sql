SELECT
    DISTINCT ORD.order_code AS 'Workorder',
    ORD.service_number AS 'Service No',
    CUS.name AS 'Customer Name',
    ACT.name AS 'Activity name',
    PER.role AS 'Group ID',
    ORD.current_crd AS 'CRD',
    ORD.order_type AS 'Order type',
    -- NPP.level AS 'NPP Level',
    -- NPP.status AS 'NPP Status',
    -- PRD.network_product_code AS 'NPC',
    ORD.taken_date AS 'Taken date',
    ACT.status AS 'Act Status',
    ACT.completed_date AS 'Comm date',
    ACT.performer_id AS 'Performer ID'
FROM
    RestInterface_order ORD
    JOIN RestInterface_activity ACT ON ORD.id = ACT.order_id
    JOIN RestInterface_person PER ON ACT.person_id = PER.id
    LEFT JOIN RestInterface_customer CUS ON CUS.id = ORD.customer_id
    LEFT JOIN RestInterface_user USR ON PER.user_id = USR.id
    LEFT JOIN RestInterface_npp NPP ON NPP.order_id = ORD.id
    AND NPP.status != 'Cancel'
    LEFT JOIN RestInterface_product PRD ON PRD.id = NPP.product_id
WHERE
    ACT.name IN (
        'Cease Resale SGO',
        'Cease Resale SGO CHN',
        'Cease Resale SGO HK',
        'Cease Resale SGO India',
        'Cease Resale SGO JP',
        'Cease Resale SGO KR',
        'Cease Resale SGO TW',
        'Cease Resale SGO UK',
        'Cease Resale SGO USA',
        'Change Resale SGO',
        'Change Resale SGO CHN',
        'Change Resale SGO HK',
        'Change Resale SGO India',
        'Change Resale SGO JP',
        'Change Resale SGO KR',
        'Change Resale SGO TW',
        'Change Resale SGO UK',
        'Change Resale SGO USA',
        'Partner Coordination',
        'LLC Accepted by Singtel'
    )
    AND PER.role IN (
        'GIP_HK',
        'GIP_IND',
        'GIP_INS',
        'GIP_MLA',
        'GIP_PHL',
        'GIP_THA',
        'GIP_UK',
        'GIP_USA',
        'GIP_VTM',
        'RESALE_IND',
        'RESALE_INS',
        'RESALE_MLA',
        'RESALE_PHL',
        'RESALE_THA',
        'RESALE_USA',
        'RESALE_VTM',
        'Resale_HK',
        'Resale_UK',
        'SDWAN_INS',
        'SDWAN_MLA',
        'SDWAN_PHL',
        'SDWAN_THA',
        'SDWAN_TW',
        'SDWAN_VTM',
        'GIP_CHN',
        'Resale_CHN',
        'GIP_TWN',
        'Resale_TW',
        'GIP_JP',
        'Resale_JP',
        'GIP_KR',
        'Resale_KR',
        'GIP_BGD',
        'RESALE_BGD'
    )
    AND ORD.order_code IN (
        'ZBO0520006',
        'ZHN4327009',
        'ZEM5511014',
        'ZBF8019036',
        'ZEM7100013',
        'ZJR1829003',
        'ZJB9414001',
        'ZGG3827005',
        'ZHR8612007',
        'ZJB4443002',
        'ZJR1829008',
        'ZJQ1668003',
        'YIR1085004',
        'ZKL9139001',
        'ZIV8816001'
    )
ORDER BY
    ORD.order_code,
    ACT.name;
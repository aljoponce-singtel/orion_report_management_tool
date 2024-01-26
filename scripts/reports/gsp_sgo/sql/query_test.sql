SELECT
    DISTINCT DISTINCT ORD.order_code AS 'Workorder',
    ORD.service_number AS 'ServiceNo',
    CUS.name AS 'CustomerName',
    CAST(ACT.activity_code AS SIGNED INTEGER) AS 'StepNo',
    ACT.name AS 'ActivityName',
    PER.role AS 'GroupID',
    ORD.current_crd AS 'CRD',
    ORD.order_type AS 'OrderType'
FROM
    RestInterface_order ORD
    JOIN RestInterface_activity ACT ON ORD.id = ACT.order_id
    JOIN (
        SELECT
            order_id,
            name,
            MAX(CAST(activity_code AS SIGNED INTEGER)) AS activity_code
        FROM
            RestInterface_activity
        WHERE
            tag_name = "Pegasus"
            AND name IN (
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
            AND completed_date BETWEEN '2023-06-01'
            AND '2023-06-30'
        GROUP BY
            order_id,
            name
    ) ACT_MAX ON ACT_MAX.order_id = ACT.order_id
    AND ACT_MAX.activity_code = CAST(ACT.activity_code AS SIGNED INTEGER)
    JOIN RestInterface_person PER ON ACT.person_id = PER.id
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
    LEFT JOIN RestInterface_customer CUS ON CUS.id = ORD.customer_id
    LEFT JOIN RestInterface_user USR ON PER.user_id = USR.id
    LEFT JOIN RestInterface_npp NPP ON NPP.order_id = ORD.id
    AND NPP.status != 'Cancel'
    LEFT JOIN RestInterface_product PRD ON PRD.id = NPP.product_id
WHERE
    ORD.order_code IN ('YOK3990002')
ORDER BY
    ORD.order_code,
    StepNo,
    ACT.name;
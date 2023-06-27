SELECT
    DISTINCT ORD.order_code AS 'Workorder',
    ORD.service_number AS 'Service No',
    CUS.name AS 'Customer Name',
    ACT.name AS 'ACT.name',
    PER.role AS 'Group ID',
    ORD.current_crd AS 'CRD',
    ORD.order_type AS 'Order type',
    PAR.STPoNo AS 'PO No',
    PRD.network_product_code AS 'NPC',
    ORD.taken_date AS 'Order creation date',
    ACT.completed_date AS 'Comm date',
    PAR.OriginCtry AS 'Originating Country',
    PAR.OriginCarr AS 'Originating Carrier',
    PAR.MainSvcType AS 'Main Svc Type',
    PAR.MainSvcNo AS 'Main Svc No',
    PAR.LLC_Partner_Ref AS 'LLC Partner reference'
FROM
    RestInterface_order ORD
    JOIN RestInterface_activity ACT ON ORD.id = ACT.order_id
    JOIN RestInterface_person PER ON ACT.person_id = PER.id
    LEFT JOIN RestInterface_customer CUS ON CUS.id = ORD.customer_id
    LEFT JOIN RestInterface_user USR ON PER.user_id = USR.id
    LEFT JOIN RestInterface_npp NPP ON NPP.order_id = ORD.id
    AND NPP.level = 'Mainline'
    LEFT JOIN RestInterface_product PRD ON PRD.id = NPP.product_id
    LEFT JOIN (
        SELECT
            npp_id,
            MAX(
                CASE
                    WHEN parameter_name = 'STPoNo' THEN parameter_value
                END
            ) STPoNo,
            MAX(
                CASE
                    WHEN parameter_name = 'OriginCtry' THEN parameter_value
                END
            ) OriginCtry,
            MAX(
                CASE
                    WHEN parameter_name = 'OriginCarr' THEN parameter_value
                END
            ) OriginCarr,
            MAX(
                CASE
                    WHEN parameter_name = 'MainSvcType' THEN parameter_value
                END
            ) MainSvcType,
            MAX(
                CASE
                    WHEN parameter_name = 'MainSvcNo' THEN parameter_value
                END
            ) MainSvcNo,
            MAX(
                CASE
                    WHEN parameter_name = 'LLC_Partner_Ref' THEN parameter_value
                END
            ) LLC_Partner_Ref
        FROM
            RestInterface_parameter
        GROUP BY
            npp_id
    ) PAR ON PAR.npp_id = NPP.id
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
    AND ACT.completed_date BETWEEN '2023-04-26'
    AND '2023-05-25'
ORDER BY
    ORD.order_code,
    ACT.name;
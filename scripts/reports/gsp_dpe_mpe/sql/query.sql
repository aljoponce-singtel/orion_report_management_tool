SELECT
    DISTINCT ORD.order_code AS 'Workorder',
    ORD.service_number AS 'Service No',
    CUS.name AS 'Customer Name',
    ACT.name AS 'Activity name',
    PER.role AS 'Group ID',
    ORD.current_crd AS 'CRD',
    ORD.order_type AS 'Order type',
    PAR.STPoNo AS 'PO No',
    'TBD' AS 'PO/SO No',
    'TBD' AS 'GRS No',
    NPP.level AS 'NPP Level',
    PRD.network_product_code AS 'NPC',
    ORD.taken_date AS 'Order creation date',
    ACT.status AS 'Act Status',
    ACT.completed_date AS 'Comm date',
    PAR.OriginCtry AS 'Originating Country',
    PAR.OriginCarr AS 'Originating Carrier',
    PAR.MainSvcType AS 'Main Svc Type',
    PAR.MainSvcNo AS 'Main Svc No',
    PAR.LLC_Partner_Ref AS 'LLC Partner reference',
    PAR.CrossConnReq AS 'Cross Connect Reference',
    'TBD' AS 'Purchasing Group',
    'TBD' AS 'Product Type',
    PAR.IMPGcode AS 'IMPG Code',
    PER.email AS 'Group Owner',
    ACT.performer_id AS 'Performer ID'
FROM
    RestInterface_order ORD
    JOIN RestInterface_activity ACT ON ORD.id = ACT.order_id
    JOIN RestInterface_person PER ON ACT.person_id = PER.id
    LEFT JOIN RestInterface_customer CUS ON CUS.id = ORD.customer_id
    LEFT JOIN RestInterface_user USR ON PER.user_id = USR.id
    LEFT JOIN RestInterface_npp NPP ON NPP.order_id = ORD.id
    LEFT JOIN RestInterface_product PRD ON PRD.id = NPP.product_id
    LEFT JOIN (
        SELECT
            NPP_INNER.id,
            MAX(
                CASE
                    WHEN PAR_INNER.parameter_name = 'STPoNo' THEN PAR_INNER.parameter_value
                END
            ) STPoNo,
            MAX(
                CASE
                    WHEN PAR_INNER.parameter_name = 'OriginCtry' THEN PAR_INNER.parameter_value
                END
            ) OriginCtry,
            MAX(
                CASE
                    WHEN PAR_INNER.parameter_name = 'OriginCarr' THEN PAR_INNER.parameter_value
                END
            ) OriginCarr,
            MAX(
                CASE
                    WHEN PAR_INNER.parameter_name = 'MainSvcType' THEN PAR_INNER.parameter_value
                END
            ) MainSvcType,
            MAX(
                CASE
                    WHEN PAR_INNER.parameter_name = 'MainSvcNo' THEN PAR_INNER.parameter_value
                END
            ) MainSvcNo,
            MAX(
                CASE
                    WHEN PAR_INNER.parameter_name = 'LLC_Partner_Ref' THEN PAR_INNER.parameter_value
                END
            ) LLC_Partner_Ref,
            MAX(
                CASE
                    WHEN PAR_INNER.parameter_name = 'CrossConnReq' THEN PAR_INNER.parameter_value
                END
            ) CrossConnReq,
            MAX(
                CASE
                    WHEN PAR_INNER.parameter_name = 'IMPGcode' THEN PAR_INNER.parameter_value
                END
            ) IMPGcode
        FROM
            RestInterface_npp NPP_INNER
            JOIN RestInterface_parameter PAR_INNER ON PAR_INNER.npp_id = NPP_INNER.id
        WHERE
            NPP_INNER.status != 'Cancel'
        GROUP BY
            NPP_INNER.id
    ) PAR ON PAR.id = NPP.id
WHERE
    (
        (
            PER.role = 'SDE_TP'
            AND ACT.name = 'Updating of TP Info'
        )
        OR (
            PER.role IN (
                'OLLC_AUS',
                'OLLC_CHN',
                'OLLC_HKG',
                'OLLC_IND',
                'OLLC_INS',
                'OLLC_JPN',
                'OLLC_KR',
                'OLLC_MLA',
                'OLLC_PHL',
                'OLLC_ROW',
                'OLLC_SNG',
                'OLLC_THA',
                'OLLC_TWN',
                'OLLC_UK',
                'OLLC_USA',
                'OLLC_VTM'
            )
            AND ACT.name = 'OLLC Order Ack'
        )
        OR PER.role = 'IMPACT'
        OR PER.role IN (
            'CPE',
            'CPE_CSE',
            'EWO',
            'EWO_CSE',
            'IMPACT_CSE',
            'LAN_CPE',
            'LAN_CPE_TR',
            'RLAN',
            'RLAN_CSE',
            'SDLAN',
            'SDWAN',
            'TOPS',
            'TOPS_CSE',
            'TSGRS',
            'TSGRS_CSE',
            'WIFI',
            'WIFI_CSE',
            'WLAN',
            'WLAN_CSE'
        )
    )
    AND NPP.status != 'Cancel'
    AND ACT.completed_date BETWEEN '2023-07-01'
    AND '2023-07-31'
ORDER BY
    ORD.order_code,
    ACT.name;
SELECT
    DISTINCT ORD.order_code AS "WO Number",
    ORD.close_date AS "Closed Date",
    CUS.name AS "Customer Name",
    ORD.service_number AS "Service No",
    ORD.initial_crd "Initial CRD",
    PRD.network_product_code AS 'Product Code',
    ORD.speed AS "Speed",
    PAR.parameter_name,
    PAR.parameter_value
FROM
    RestInterface_order ORD
    LEFT JOIN RestInterface_customer CUS ON CUS.id = ORD.customer_id
    LEFT JOIN RestInterface_npp NPP ON NPP.order_id = ORD.id
    AND NPP.level = "Mainline"
    AND NPP.status != "Cancel"
    LEFT JOIN RestInterface_product PRD ON PRD.id = NPP.product_id
    LEFT JOIN RestInterface_parameter PAR ON PAR.npp_id = NPP.id
    AND PAR.parameter_name IN (
        'UpLkSpd',
        'OriginCtry',
        'OriginState',
        'TermCtry',
        'TermState',
        'OLLCTy',
        'OLLCNetworkTy',
        'MainSvcNo',
        'Interface',
        'LLC_Partner_Ref',
        'LLC_Partner_Name',
        'LLC_Receipt_By_Partner',
        'LLC_Accept_By_ST',
        'LLC_Xconn_Ref',
        'LLC_Xconn_Partner_Name',
        'LLC_Xconn_Accept_By_ST',
        'FOC_Date',
        'Date_Received_From_Partner_FOC',
        'OLLCOrdPld',
        'SiteSrvyDt',
        'SurveyDate',
        'DPMReDate',
        'PartnerAckDate'
    )
WHERE
    PRD.product_type_id = (
        SELECT
            id
        FROM
            RestInterface_producttype
        WHERE
            title = "OLLC"
    )
    AND ORD.order_code IN (
        'ZJP4632002',
        'ZKC7814005',
        'ZKP0343001',
        'ZKP7707004',
        'ZKC8290001',
        'ZIZ5091004',
        'ZHR0723006',
        'ZIJ2017002'
    )
ORDER BY
    ORD.order_code,
    PRD.network_product_code,
    PAR.parameter_name,
    PAR.parameter_value;
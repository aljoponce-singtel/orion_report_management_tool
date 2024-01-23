SELECT
    DISTINCT ORD.order_code AS "WO Number",
    ORD.close_date AS "Closed Date",
    CUS.name AS "Customer Name",
    ORD.service_number AS "Service No",
    ORD.initial_crd "Initial CRD",
    PRD.network_product_code AS 'Product Code',
    (
        CASE
            WHEN PAR.UpLkSpd IS NULL THEN ORD.speed
            ELSE PAR.UpLkSpd
        END
    ) AS 'Speed',
    PAR.OriginCtry AS 'Orginating Country',
    PAR.OriginState AS 'Orginating State',
    PAR.TermCtry AS 'Terminating Country',
    PAR.TermState AS 'Terminating State',
    PAR.OLLCTy AS 'OLLC Type',
    PAR.OLLCNetworkTy AS 'OLLC Network Type',
    PAR.MainSvcNo AS 'Main Svc No',
    PAR.Interface AS 'Interface',
    PAR.LLC_Partner_Ref AS 'LLC Partner reference',
    PAR.LLC_Partner_Name AS 'LLC Partner Name',
    PAR.LLC_Receipt_By_Partner AS 'LLC Received from Partner',
    PAR.LLC_Accept_By_ST AS 'LLC Accepted by Singtel',
    PAR.LLC_Xconn_Ref AS 'LLC X-connnect reference',
    PAR.LLC_Xconn_Partner_Name AS 'LLC X-connect Partner Name',
    PAR.LLC_Xconn_Accept_By_ST AS 'LLC X-connect Accepted by Singtel',
    PAR.FOC_Date AS 'FOC Date',
    PAR.Date_Received_From_Partner_FOC AS 'Date received from Partner (FOC)',
    PAR.OLLCOrdPld AS 'OLLC Order Placed',
    PAR.SiteSrvyDt AS 'Site survey Date',
    PAR.SurveyDate AS 'Actual Site survey completion',
    PAR.DPMReDate AS 'DPE received date',
    PAR.PartnerAckDate AS 'Partner Ack Date'
FROM
    RestInterface_order ORD
    LEFT JOIN RestInterface_customer CUS ON CUS.id = ORD.customer_id
    LEFT JOIN RestInterface_npp NPP ON NPP.order_id = ORD.id
    AND NPP.level = "Mainline"
    AND NPP.status != "Cancel"
    LEFT JOIN RestInterface_product PRD ON PRD.id = NPP.product_id
    LEFT JOIN (
        SELECT
            npp_id,
            MAX(
                CASE
                    WHEN parameter_name = 'UpLkSpd' THEN parameter_value
                END
            ) UpLkSpd,
            MAX(
                CASE
                    WHEN parameter_name = 'OriginCtry' THEN parameter_value
                END
            ) OriginCtry,
            MAX(
                CASE
                    WHEN parameter_name = 'OriginState' THEN parameter_value
                END
            ) OriginState,
            MAX(
                CASE
                    WHEN parameter_name = 'TermCtry' THEN parameter_value
                END
            ) TermCtry,
            MAX(
                CASE
                    WHEN parameter_name = 'TermState' THEN parameter_value
                END
            ) TermState,
            MAX(
                CASE
                    WHEN parameter_name = 'OLLCTy' THEN parameter_value
                END
            ) OLLCTy,
            MAX(
                CASE
                    WHEN parameter_name = 'OLLCNetworkTy' THEN parameter_value
                END
            ) OLLCNetworkTy,
            MAX(
                CASE
                    WHEN parameter_name = 'MainSvcNo' THEN parameter_value
                END
            ) MainSvcNo,
            MAX(
                CASE
                    WHEN parameter_name = 'Interface' THEN parameter_value
                END
            ) Interface,
            MAX(
                CASE
                    WHEN parameter_name = 'LLC_Partner_Ref' THEN parameter_value
                END
            ) LLC_Partner_Ref,
            MAX(
                CASE
                    WHEN parameter_name = 'LLC_Partner_Name' THEN parameter_value
                END
            ) LLC_Partner_Name,
            MAX(
                CASE
                    WHEN parameter_name = 'LLC_Receipt_By_Partner' THEN parameter_value
                END
            ) LLC_Receipt_By_Partner,
            MAX(
                CASE
                    WHEN parameter_name = 'LLC_Accept_By_ST' THEN parameter_value
                END
            ) LLC_Accept_By_ST,
            MAX(
                CASE
                    WHEN parameter_name = 'LLC_Xconn_Ref' THEN parameter_value
                END
            ) LLC_Xconn_Ref,
            MAX(
                CASE
                    WHEN parameter_name = 'LLC_Xconn_Partner_Name' THEN parameter_value
                END
            ) LLC_Xconn_Partner_Name,
            MAX(
                CASE
                    WHEN parameter_name = 'LLC_Xconn_Accept_By_ST' THEN parameter_value
                END
            ) LLC_Xconn_Accept_By_ST,
            MAX(
                CASE
                    WHEN parameter_name = 'FOC_Date' THEN parameter_value
                END
            ) FOC_Date,
            MAX(
                CASE
                    WHEN parameter_name = 'Date_Received_From_Partner_FOC' THEN parameter_value
                END
            ) Date_Received_From_Partner_FOC,
            MAX(
                CASE
                    WHEN parameter_name = 'OLLCOrdPld' THEN parameter_value
                END
            ) OLLCOrdPld,
            MAX(
                CASE
                    WHEN parameter_name = 'SiteSrvyDt' THEN parameter_value
                END
            ) SiteSrvyDt,
            MAX(
                CASE
                    WHEN parameter_name = 'SurveyDate' THEN parameter_value
                END
            ) SurveyDate,
            MAX(
                CASE
                    WHEN parameter_name = 'DPMReDate' THEN parameter_value
                END
            ) DPMReDate,
            MAX(
                CASE
                    WHEN parameter_name = 'PartnerAckDate' THEN parameter_value
                END
            ) PartnerAckDate
        FROM
            RestInterface_parameter
        GROUP BY
            npp_id
    ) PAR ON PAR.npp_id = NPP.id
WHERE
    PRD.product_type_id = (
        SELECT
            id
        FROM
            RestInterface_producttype
        WHERE
            title = "OLLC"
    )
    AND ORD.order_status = 'Closed'
    AND ORD.close_date BETWEEN '{start_date}'
    AND '{end_date}'
ORDER BY
    ORD.order_code,
    PRD.network_product_code;
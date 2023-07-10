SELECT
    DISTINCT ORD.order_code,
    ORD.service_number,
    CUS.name AS customer,
    ORD.order_type,
    ORD.order_status,
    ORD.taken_date,
    ORD.current_crd,
    ORD.initial_crd,
    ORD.close_date,
    ORD.assignee,
    PRJ.project_code,
    CKT.circuit_code,
    NPP.level AS npp_level,
    PRD.network_product_code AS product_code,
    PRD.network_product_desc AS product_description,
    PAR.PartnerNm,
    PAR.OLLCAPartnerContractStartDt,
    PAR.OLLCBPartnerContractStartDt,
    PAR.OLLCAPartnerContractTerm,
    PAR.OLLCATax,
    PAR.LLC_Partner_Name,
    PAR.LLC_Partner_Ref,
    PAR.PartnerCctRef,
    PAR.STPoNo,
    PAR.STIntSvcNo,
    PAR.IMPGcode,
    PAR.Model,
    ORD.business_sector,
    SITE.site_code AS exchange_code_a,
    SITE.site_code_second AS exchange_code_b,
    BRN.brn,
    ORD.am_id,
    ORD.sde_received_date,
    ORD.arbor_disp AS arbor_service,
    ORD.service_type,
    ORD.order_priority,
    SINOTE.date_created AS crd_amendment_date,
    REGEXP_SUBSTR(
        SINOTE.details,
        BINARY '(?<=Old CRD:)(.*)(?= New CRD:[0-9]{8})'
    ) AS old_crd,
    REGEXP_SUBSTR(
        SINOTE.details,
        BINARY '(?<=New CRD:)(.*)(?= Category Code:)'
    ) AS new_crd,
    NOTEDLY.reason AS crd_amendment_reason,
    NOTEDLY.reason_gsp AS crd_amendment_reason_gsp,
    ACT.ollc_order_ack AS 'OLLC Order Ack',
    ACT.ollc_site_survey AS 'OLLC Site Survey',
    ACT.foc_date_received AS 'FOC Date Received',
    ACT.llc_accepted_by_singtel AS 'LLC Accepted by Singtel'
FROM
    RestInterface_order ORD
    LEFT JOIN (
        SELECT
            order_id,
            MAX(
                CASE
                    WHEN name = 'OLLC Order Ack' THEN completed_date
                END
            ) ollc_order_ack,
            MAX(
                CASE
                    WHEN name = 'OLLC Site Survey' THEN completed_date
                END
            ) ollc_site_survey,
            MAX(
                CASE
                    WHEN name = 'FOC Date Received' THEN completed_date
                END
            ) foc_date_received,
            MAX(
                CASE
                    WHEN name = 'LLC Accepted by Singtel' THEN completed_date
                END
            ) llc_accepted_by_singtel
        FROM
            RestInterface_activity
        GROUP BY
            order_id
    ) ACT ON ACT.order_id = ORD.id
    LEFT JOIN (
        SELECT
            SINOTEINNER.*
        FROM
            o2pprod.RestInterface_ordersinote SINOTEINNER
            JOIN (
                SELECT
                    order_id,
                    MAX(note_code) AS note_code
                FROM
                    o2pprod.RestInterface_ordersinote
                WHERE
                    categoty = 'CRD'
                    AND sub_categoty = 'CRD Change History'
                    AND reason_code IS NOT NULL
                GROUP BY
                    order_id
            ) SINOTEMAX ON SINOTEMAX.order_id = SINOTEINNER.order_id
            AND SINOTEMAX.note_code = SINOTEINNER.note_code
    ) SINOTE ON SINOTE.order_id = ORD.id
    LEFT JOIN RestInterface_delayreason NOTEDLY ON NOTEDLY.code = SINOTE.reason_code
    LEFT JOIN RestInterface_project PRJ ON ORD.project_id = PRJ.id
    LEFT JOIN RestInterface_circuit CKT ON ORD.circuit_id = CKT.id
    LEFT JOIN RestInterface_customer CUS ON CUS.id = ORD.customer_id
    LEFT JOIN RestInterface_site SITE ON SITE.id = ORD.site_id
    LEFT JOIN RestInterface_customerbrnmapping BRN ON BRN.id = ORD.customer_brn_id
    LEFT JOIN RestInterface_npp NPP ON NPP.order_id = ORD.id
    LEFT JOIN RestInterface_product PRD ON PRD.id = NPP.product_id
    LEFT JOIN (
        SELECT
            npp_id,
            MAX(
                CASE
                    WHEN parameter_name = 'IMPGcode' THEN parameter_value
                END
            ) IMPGcode,
            MAX(
                CASE
                    WHEN parameter_name = 'LLC_Partner_Name' THEN parameter_value
                END
            ) LLC_Partner_Name,
            MAX(
                CASE
                    WHEN parameter_name = 'LLC_Partner_Ref' THEN parameter_value
                END
            ) LLC_Partner_Ref,
            MAX(
                CASE
                    WHEN parameter_name = 'Model' THEN parameter_value
                END
            ) Model,
            MAX(
                CASE
                    WHEN parameter_name = 'OLLCAPartnerContractStartDt' THEN parameter_value
                END
            ) OLLCAPartnerContractStartDt,
            MAX(
                CASE
                    WHEN parameter_name = 'OLLCAPartnerContractTerm' THEN parameter_value
                END
            ) OLLCAPartnerContractTerm,
            MAX(
                CASE
                    WHEN parameter_name = 'OLLCATax' THEN parameter_value
                END
            ) OLLCATax,
            MAX(
                CASE
                    WHEN parameter_name = 'OLLCBPartnerContractStartDt' THEN parameter_value
                END
            ) OLLCBPartnerContractStartDt,
            MAX(
                CASE
                    WHEN parameter_name = 'PartnerCctRef' THEN parameter_value
                END
            ) PartnerCctRef,
            MAX(
                CASE
                    WHEN parameter_name = 'PartnerNm' THEN parameter_value
                END
            ) PartnerNm,
            MAX(
                CASE
                    WHEN parameter_name = 'STIntSvcNo' THEN parameter_value
                END
            ) STIntSvcNo,
            MAX(
                CASE
                    WHEN parameter_name = 'STPoNo' THEN parameter_value
                END
            ) STPoNo
        FROM
            RestInterface_parameter
        GROUP BY
            npp_id
    ) PAR ON PAR.npp_id = NPP.id
WHERE
    ORD.order_status IN ('Submitted', 'Closed')
    AND ORD.current_crd BETWEEN '2023-03-15' AND '2023-09-15';
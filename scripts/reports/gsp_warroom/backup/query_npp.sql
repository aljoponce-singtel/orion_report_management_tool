SELECT
    DISTINCT ORD.order_code,
    ORD.order_taken_by AS created_by,
    ORD.service_order_number AS service_order_no,
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
    PAR.AELineCeaseDt,
    PAR.AELineCurrencyCd,
    PAR.AELineMRC,
    PAR.AELineOTC,
    PAR.AELinePartnerContractStartDt,
    PAR.AELinePartnerContractTerm,
    PAR.AELinePartnerNm,
    PAR.AELineSTPO,
    PAR.AELineTax,
    PAR.AELineTPTy,
    PAR.APartnerCctRef,
    PAR.BOLLCBCurrencyCd,
    PAR.BOLLCBMRC,
    PAR.BOLLCBPartnerNm,
    PAR.BOLLCBStateIndia,
    PAR.BOLLCBTax,
    PAR.IMPGcode,
    PAR.InternetCurrencyCd,
    PAR.InternetMRC,
    PAR.InternetOTC,
    PAR.InternetPartnerContractStartDt,
    PAR.InternetPartnerContractTerm,
    PAR.LLC_Partner_Name,
    PAR.LLC_Partner_Ref,
    PAR.Model,
    PAR.OLLCAdminFee,
    PAR.OLLCAPartnerContractStartDt,
    PAR.OLLCAPartnerContractTerm,
    PAR.OLLCAStateIndia,
    PAR.OLLCATax,
    PAR.OLLCBPartnerContractStartDt,
    PAR.OLLCCurrencyCd,
    PAR.OLLCMRC,
    PAR.OLLCOTC,
    PAR.OLLCPartnerContractStartDt,
    PAR.OLLCPartnerContractTerm,
    PAR.OLLCPartnerNm,
    PAR.OLLCPPNo,
    PAR.OLLCTax,
    PAR.OriginState,
    PAR.PartnerCctRef,
    PAR.PartnerNm,
    PAR.PortCurrencyCd,
    PAR.PortMRC,
    PAR.PortOTC,
    PAR.PortPartnerContractStartDt,
    PAR.PortPartnerContractTerm,
    PAR.PortTax,
    PAR.STIntSvcNo,
    PAR.STPoNo,
    PAR.SvcNo,
    PAR.TermCarr,
    PAR.InterCoReq,
    ORD.business_sector,
    SITE.site_code AS exchange_code_a,
    SITE.site_code_second AS exchange_code_b,
    SITE.site_code AS a_end_address,
    SITE.site_code_second AS b_end_address,
    BRN.brn,
    CON.a_end_cust_contact,
    CON.b_end_cust_contact,
    CON.aam_contact,
    CON.am_contact,
    CON.pm_contact,
    CON.reseller_contact,
    CON.techincal_cust_contact,
    ORD.am_id,
    ORD.secondary_am_id,
    ORD.sde_received_date,
    ORD.arbor_disp AS arbor_service,
    ORD.service_type,
    ORD.order_priority,
    ORD.speed,
    ORD.ord_action_type AS order_action_type,
    SINOTE.date_created AS crd_amendment_date,
    DATE_FORMAT(
        REGEXP_SUBSTR(
            SINOTE.details,
            BINARY '(?<=Old CRD:)(.*)(?= New CRD:[0-9]{{8}})'
        ),
        '%Y-%m-%d'
    ) AS old_crd,
    DATE_FORMAT(
        REGEXP_SUBSTR(
            SINOTE.details,
            BINARY '(?<=New CRD:)(.*)(?= Category Code:)'
        ),
        '%Y-%m-%d'
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
    AND NPP.status != 'Cancel'
    LEFT JOIN (
        SELECT
            order_id,
            MAX(
                CASE
                    WHEN contact_type = "A-end-Cust" THEN GROUP_CONCAT(
                        DISTINCT email_address
                        ORDER BY
                            email_address SEPARATOR '; '
                    )
                END
            ) a_end_cust_contact,
            MAX(
                CASE
                    WHEN contact_type = "B-end-Cust" THEN GROUP_CONCAT(
                        DISTINCT email_address
                        ORDER BY
                            email_address SEPARATOR '; '
                    )
                END
            ) b_end_cust_contact,
            MAX(
                CASE
                    WHEN contact_type = "AAM" THEN GROUP_CONCAT(
                        DISTINCT email_address
                        ORDER BY
                            email_address SEPARATOR '; '
                    )
                END
            ) aam_contact,
            MAX(
                CASE
                    WHEN contact_type = "AM" THEN GROUP_CONCAT(
                        DISTINCT email_address
                        ORDER BY
                            email_address SEPARATOR '; '
                    )
                END
            ) am_contact,
            MAX(
                CASE
                    WHEN contact_type = "Project Manager" THEN GROUP_CONCAT(
                        DISTINCT email_address
                        ORDER BY
                            email_address SEPARATOR '; '
                    )
                END
            ) pm_contact,
            MAX(
                CASE
                    WHEN contact_type = "Reseller" THEN GROUP_CONCAT(
                        DISTINCT email_address
                        ORDER BY
                            email_address SEPARATOR '; '
                    )
                END
            ) reseller_contact,
            MAX(
                CASE
                    WHEN contact_type = "Techincal-Cust" THEN GROUP_CONCAT(
                        DISTINCT email_address
                        ORDER BY
                            email_address SEPARATOR '; '
                    )
                END
            ) techincal_cust_contact
        FROM
            RestInterface_contactdetails
        GROUP BY
            order_id
    ) CON ON CON.order_id = ORD.id
    LEFT JOIN (
        SELECT
            npp_id,
            MAX(
                CASE
                    WHEN parameter_name = 'AELineCeaseDt' THEN parameter_value
                END
            ) AELineCeaseDt,
            MAX(
                CASE
                    WHEN parameter_name = 'AELineCurrencyCd' THEN parameter_value
                END
            ) AELineCurrencyCd,
            MAX(
                CASE
                    WHEN parameter_name = 'AELineMRC' THEN parameter_value
                END
            ) AELineMRC,
            MAX(
                CASE
                    WHEN parameter_name = 'AELineOTC' THEN parameter_value
                END
            ) AELineOTC,
            MAX(
                CASE
                    WHEN parameter_name = 'AELinePartnerContractStartDt' THEN parameter_value
                END
            ) AELinePartnerContractStartDt,
            MAX(
                CASE
                    WHEN parameter_name = 'AELinePartnerContractTerm' THEN parameter_value
                END
            ) AELinePartnerContractTerm,
            MAX(
                CASE
                    WHEN parameter_name = 'AELinePartnerNm' THEN parameter_value
                END
            ) AELinePartnerNm,
            MAX(
                CASE
                    WHEN parameter_name = 'AELineSTPO' THEN parameter_value
                END
            ) AELineSTPO,
            MAX(
                CASE
                    WHEN parameter_name = 'AELineTax' THEN parameter_value
                END
            ) AELineTax,
            MAX(
                CASE
                    WHEN parameter_name = 'AELineTPTy' THEN parameter_value
                END
            ) AELineTPTy,
            MAX(
                CASE
                    WHEN parameter_name = 'APartnerCctRef' THEN parameter_value
                END
            ) APartnerCctRef,
            MAX(
                CASE
                    WHEN parameter_name = 'BOLLCBCurrencyCd' THEN parameter_value
                END
            ) BOLLCBCurrencyCd,
            MAX(
                CASE
                    WHEN parameter_name = 'BOLLCBMRC' THEN parameter_value
                END
            ) BOLLCBMRC,
            MAX(
                CASE
                    WHEN parameter_name = 'BOLLCBPartnerNm' THEN parameter_value
                END
            ) BOLLCBPartnerNm,
            MAX(
                CASE
                    WHEN parameter_name = 'BOLLCBStateIndia' THEN parameter_value
                END
            ) BOLLCBStateIndia,
            MAX(
                CASE
                    WHEN parameter_name = 'BOLLCBTax' THEN parameter_value
                END
            ) BOLLCBTax,
            MAX(
                CASE
                    WHEN parameter_name = 'IMPGcode' THEN parameter_value
                END
            ) IMPGcode,
            MAX(
                CASE
                    WHEN parameter_name = 'InternetCurrencyCd' THEN parameter_value
                END
            ) InternetCurrencyCd,
            MAX(
                CASE
                    WHEN parameter_name = 'InternetMRC' THEN parameter_value
                END
            ) InternetMRC,
            MAX(
                CASE
                    WHEN parameter_name = 'InternetOTC' THEN parameter_value
                END
            ) InternetOTC,
            MAX(
                CASE
                    WHEN parameter_name = 'InternetPartnerContractStartDt' THEN parameter_value
                END
            ) InternetPartnerContractStartDt,
            MAX(
                CASE
                    WHEN parameter_name = 'InternetPartnerContractTerm' THEN parameter_value
                END
            ) InternetPartnerContractTerm,
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
                    WHEN parameter_name = 'OLLCAdminFee' THEN parameter_value
                END
            ) OLLCAdminFee,
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
                    WHEN parameter_name = 'OLLCAStateIndia' THEN parameter_value
                END
            ) OLLCAStateIndia,
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
                    WHEN parameter_name = 'OLLCCurrencyCd' THEN parameter_value
                END
            ) OLLCCurrencyCd,
            MAX(
                CASE
                    WHEN parameter_name = 'OLLCMRC' THEN parameter_value
                END
            ) OLLCMRC,
            MAX(
                CASE
                    WHEN parameter_name = 'OLLCOTC' THEN parameter_value
                END
            ) OLLCOTC,
            MAX(
                CASE
                    WHEN parameter_name = 'OLLCPartnerContractStartDt' THEN parameter_value
                END
            ) OLLCPartnerContractStartDt,
            MAX(
                CASE
                    WHEN parameter_name = 'OLLCPartnerContractTerm' THEN parameter_value
                END
            ) OLLCPartnerContractTerm,
            MAX(
                CASE
                    WHEN parameter_name = 'OLLCPartnerNm' THEN parameter_value
                END
            ) OLLCPartnerNm,
            MAX(
                CASE
                    WHEN parameter_name = 'OLLCPPNo' THEN parameter_value
                END
            ) OLLCPPNo,
            MAX(
                CASE
                    WHEN parameter_name = 'OLLCTax' THEN parameter_value
                END
            ) OLLCTax,
            MAX(
                CASE
                    WHEN parameter_name = 'OriginState' THEN parameter_value
                END
            ) OriginState,
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
                    WHEN parameter_name = 'PortCurrencyCd' THEN parameter_value
                END
            ) PortCurrencyCd,
            MAX(
                CASE
                    WHEN parameter_name = 'PortMRC' THEN parameter_value
                END
            ) PortMRC,
            MAX(
                CASE
                    WHEN parameter_name = 'PortOTC' THEN parameter_value
                END
            ) PortOTC,
            MAX(
                CASE
                    WHEN parameter_name = 'PortPartnerContractStartDt' THEN parameter_value
                END
            ) PortPartnerContractStartDt,
            MAX(
                CASE
                    WHEN parameter_name = 'PortPartnerContractTerm' THEN parameter_value
                END
            ) PortPartnerContractTerm,
            MAX(
                CASE
                    WHEN parameter_name = 'PortTax' THEN parameter_value
                END
            ) PortTax,
            MAX(
                CASE
                    WHEN parameter_name = 'STIntSvcNo' THEN parameter_value
                END
            ) STIntSvcNo,
            MAX(
                CASE
                    WHEN parameter_name = 'STPoNo' THEN parameter_value
                END
            ) STPoNo,
            MAX(
                CASE
                    WHEN parameter_name = 'SvcNo' THEN parameter_value
                END
            ) SvcNo,
            MAX(
                CASE
                    WHEN parameter_name = 'TermCarr' THEN parameter_value
                END
            ) TermCarr,
            MAX(
                CASE
                    WHEN parameter_name = 'SpecTP' THEN parameter_value
                END
            ) InterCoReq
        FROM
            RestInterface_parameter
        GROUP BY
            npp_id
    ) PAR ON PAR.npp_id = NPP.id
WHERE
    ORD.order_status IN ('Submitted', 'Closed')
    AND ORD.current_crd BETWEEN '{start_date}'
    AND '{end_date}';
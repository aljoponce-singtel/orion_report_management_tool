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
WHERE
    npp_id = 19293798
GROUP BY
    npp_id;
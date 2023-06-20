SELECT
    order_code,
    level,
    action,
    network_product_code,
    MAX(
        case
            when parameter_name = 'IMPGcode' then parameter_value
        end
    ) IMPGcode,
    MAX(
        case
            when parameter_name = 'LLC_Partner_Name' then parameter_value
        end
    ) LLC_Partner_Name,
    MAX(
        case
            when parameter_name = 'LLC_Partner_Ref' then parameter_value
        end
    ) LLC_Partner_Ref,
    MAX(
        case
            when parameter_name = 'Model' then parameter_value
        end
    ) Model,
    MAX(
        case
            when parameter_name = 'OLLCAPartnerContractStartDt' then parameter_value
        end
    ) OLLCAPartnerContractStartDt,
    MAX(
        case
            when parameter_name = 'OLLCAPartnerContractTerm' then parameter_value
        end
    ) OLLCAPartnerContractTerm,
    MAX(
        case
            when parameter_name = 'OLLCATax' then parameter_value
        end
    ) OLLCATax,
    MAX(
        case
            when parameter_name = 'OLLCBPartnerContractStartDt' then parameter_value
        end
    ) OLLCBPartnerContractStartDt,
    MAX(
        case
            when parameter_name = 'PartnerCctRef' then parameter_value
        end
    ) PartnerCctRef,
    MAX(
        case
            when parameter_name = 'PartnerNm' then parameter_value
        end
    ) PartnerNm,
    MAX(
        case
            when parameter_name = 'STIntSvcNo' then parameter_value
        end
    ) STIntSvcNo,
    MAX(
        case
            when parameter_name = 'STPoNo' THEN parameter_value
        END
    ) STPoNo
FROM
    tmp_ord_para
GROUP BY
    order_code,
    level,
    action,
    network_product_code;
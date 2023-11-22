INSERT INTO
    tmp_project_tracker
SELECT
    NULL,
    ECOM.WorkOrderNo,
    TRIM(ECOM.CircuitTie),
    TRIM(ECOM.BillingDependancyID)
FROM
    pegasusmulesoft.O2P_STPfields ECOM
    JOIN (
        SELECT
            WorkOrderNo,
            MAX(fileName) AS fileName
        FROM
            pegasusmulesoft.O2P_STPfields
        GROUP BY
            WorkOrderNo
    ) ECOM_MAX ON ECOM_MAX.WorkOrderNo = ECOM.WorkOrderNo
    AND ECOM_MAX.fileName = ECOM.fileName
WHERE
    TRIM(ECOM.CircuitTie) != ''
    AND TRIM(ECOM.BillingDependancyID) != '';
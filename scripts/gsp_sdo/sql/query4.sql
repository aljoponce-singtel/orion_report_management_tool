SELECT
    DISTINCT service_number AS ServiceNoNew,
    id AS OrderIdNew,
    order_code AS OrderCodeNew,
    current_crd AS CRDNew
FROM
    RestInterface_order
WHERE
    order_type = 'Provide'
    AND service_number IN ({});
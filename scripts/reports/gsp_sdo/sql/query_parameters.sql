SELECT
    DISTINCT ORD.service_number AS ServiceNumberUpd,
    ORD.order_code AS OrderCode,
    ORD.taken_date AS CreationDate,
    PAR.parameter_name AS ParameterName,
    PAR.parameter_value AS ParameterValue
FROM
    RestInterface_order ORD
    LEFT JOIN RestInterface_npp NPP ON NPP.order_id = ORD.id
    LEFT JOIN RestInterface_parameter PAR ON PAR.npp_id = NPP.id
WHERE
    ORD.order_type = 'Provide'
    AND NPP.level = 'Mainline'
    AND NPP.status <> 'Cancel'
    AND PAR.parameter_name IN ({para_names_list})
    AND ORD.service_number IN ({serviceno_list})
ORDER BY
    ORD.taken_date;
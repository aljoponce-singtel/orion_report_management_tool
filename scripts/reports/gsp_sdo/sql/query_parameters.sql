SELECT
    DISTINCT ORD.service_number AS ServiceNumberUpd,
    PAR.parameter_name AS ParameterName,
    PAR.parameter_value AS ParameterValue
FROM
    RestInterface_order ORD
    LEFT JOIN RestInterface_npp NPP ON NPP.order_id = ORD.id
    LEFT JOIN RestInterface_parameter PAR ON PAR.npp_id = NPP.id
    AND PAR.parameter_name IN ({para_names_list})
WHERE
    ORD.order_type = 'Provide'
    AND NPP.level = 'Mainline'
    AND NPP.status <> 'Cancel'
    AND ORD.service_number IN ({serviceno_list});
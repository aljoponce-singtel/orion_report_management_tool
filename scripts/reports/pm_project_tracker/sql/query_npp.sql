SELECT DISTINCT
    PRJ.project_code,
    ORD.order_code,
    NPP.level,
    NPP.status,
    PRJTRK.product_category,
    PRJTRK.circuit_layer,
    PRJTRK.type_of_product,
    PRJTRK.type_of_work,
    -- ORD.current_crd,
    -- ORD.order_status,
    -- ORD.order_type,
    -- PRD.network_product_code,
    -- PRD.network_product_desc,
    PAR.parameter_name,
    PAR.parameter_value
FROM
    RestInterface_order ORD
    JOIN o2ptest.{group_table_name} PRJTRK ON PRJTRK.order_id = ORD.id
    LEFT JOIN RestInterface_project PRJ ON ORD.project_id = PRJ.id
    LEFT JOIN RestInterface_npp NPP ON NPP.order_id = ORD.id
    AND NPP.level = "Mainline"
    AND NPP.status != "Cancel"
    LEFT JOIN RestInterface_product PRD ON PRD.id = NPP.product_id
    LEFT JOIN RestInterface_parameter PAR ON PAR.npp_id = NPP.id
WHERE
    ORD.project_tracker_group_name = (
        SELECT project_tracker_group_name
        FROM o2ptest.{group_table_name}
        LIMIT 1
    )
ORDER BY ORD.order_code, PAR.parameter_name

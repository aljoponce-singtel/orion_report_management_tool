SELECT DISTINCT
    ORD.id,
    ORD.order_code,
    ORD.order_type,
    ORD.order_status,
    ORD.order_priority,
    ORD.business_sector,
    ORD.current_crd,
    ORD.initial_crd,
    ORD.taken_date,
    ORD.sde_received_date,
    ORD.arbor_service_type,
    ORD.service_number,
    ORD.service_type,
    CUS.name,
    ORD.assignee,
    PRJ.project_code,
    ORD.circuit_id,
    ORD.product_description,
    ACT.name,
    ACT.due_date,
    ACT.status Act_Status,
    ACT.predecessor_list,
    ACT.activity_code,
    ACT.ready_date,
    ACT.completed_date,
    PER.role,
    CKT.circuit_code,
    PAR.parameter_name,
    PAR.parameter_value
FROM
    RestInterface_order ORD
    JOIN RestInterface_activity ACT ON ACT.order_id = ORD.id
    LEFT JOIN RestInterface_project PRJ ON ORD.project_id = PRJ.id
    LEFT JOIN RestInterface_circuit CKT ON ORD.circuit_id = CKT.id
    LEFT JOIN RestInterface_person PER ON PER.id = ACT.id
    LEFT JOIN RestInterface_customer CUS ON CUS.id = ORD.customer_id
    LEFT JOIN RestInterface_npp NPP ON NPP.order_id = ORD.id
    AND NPP.level = 'Mainline'
    AND NPP.status != 'Cancel'
    LEFT JOIN RestInterface_parameter PAR ON PAR.npp_id = NPP.id
    AND PAR.parameter_name = 'Type'
    AND PAR.parameter_value IN ('1', '2', '010', '020')
WHERE
    ORD.order_status IN (
        'Submitted',
        'PONR',
        'Pending Cancellation',
        'Completed'
    )
    AND ORD.current_crd <= DATE_ADD(now(), INTERVAL 3 MONTH)
    AND ACT.tag_name = 'Pegasus';
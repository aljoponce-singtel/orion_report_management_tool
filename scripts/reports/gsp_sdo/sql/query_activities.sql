SELECT
    ORD.order_code AS OrderCodeNew,
    PER.role AS GroupId,
    CAST(ACT.activity_code AS UNSIGNED) AS step_no,
    ACT.name AS ActivityName,
    ACT.due_date,
    ACT.status,
    ACT.ready_date,
    DATE(ACT.exe_date) AS exe_date,
    DATE(ACT.dly_date) AS dly_date,
    ACT.completed_date
FROM
    RestInterface_order ORD
    JOIN RestInterface_activity ACT ON ACT.order_id = ORD.id
    LEFT JOIN RestInterface_person PER ON PER.id = ACT.person_id
WHERE
    ACT.name IN ({unique_activities})
    AND ORD.order_code IN ({order_list})
ORDER BY
    OrderCodeNew,
    step_no;
SELECT
    DISTINCT report_id,
    Workorder_no,
    Product_Code,
    Group_ID_1,
    Activity_Name_1,
    COM_1,
    Group_ID_2,
    Activity_Name_2,
    COM_2,
    DATE(update_time)
FROM
    t_GSP_ip_svcs
WHERE
    report_id IN ('GSDT7')
    AND DATE(update_time) = '2022-05-01'
ORDER BY
    report_id;
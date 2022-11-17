SELECT
    DISTINCT Workorder_no
FROM
    t_GSP_ip_svcs
WHERE
    report_id IN ('{}')
    AND Order_Type = 'Provide'
    AND Product_Code IN (
        {}
    )
    AND DATE(update_time) = '{}';
SELECT
    DISTINCT Workorder_no
FROM
    o2ptableau.t_GSP_ip_svcs
WHERE
    report_id IN ('{report_id}')
    AND Order_Type = 'Provide'
    AND Product_Code IN ({product_code_list})
    AND DATE(update_time) = '{report_date}';
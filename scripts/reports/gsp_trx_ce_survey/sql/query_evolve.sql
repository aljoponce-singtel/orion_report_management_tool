SELECT
    ORDEV.order_code,
    ORDEV.service_number,
    ORDEV.order_type,
    ORDEV.order_status,
    ORDEV.close_date,
    NPPEV.level,
    PRDEV.network_product_code,
    PRDEV.network_product_desc
FROM
    RestInterface_order ORDEV
    JOIN RestInterface_npp NPPEV ON NPPEV.order_id = ORDEV.id
    AND NPPEV.level = "Mainline"
    AND ORDEV.service_number LIKE 'EV%'
    JOIN RestInterface_product PRDEV ON PRDEV.id = NPPEV.product_id
    AND PRDEV.network_product_code != "SGN0005"
WHERE
    YEAR(ORDEV.close_date) = '2023'
ORDER BY
    ORDEV.close_date,
    ORDEV.order_code;
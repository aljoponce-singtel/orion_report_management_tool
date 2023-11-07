SELECT
    DISTINCT ORDEV.order_code,
    ORDEV.service_number,
    ORDEV.order_type,
    ORDEV.order_status,
    ORDEV.close_date,
    NPPEV.level,
    PRDEV.network_product_code,
    BILLEV.package_description,
    BILLEV.component_description
FROM
    RestInterface_order ORDEV
    LEFT JOIN RestInterface_billing BILLEV ON BILLEV.order_id = ORDEV.id
    LEFT JOIN RestInterface_npp NPPEV ON NPPEV.order_id = ORDEV.id
    AND NPPEV.level = "Mainline"
    LEFT JOIN RestInterface_product PRDEV ON PRDEV.id = NPPEV.product_id
WHERE
    ORDEV.close_date > DATE_SUB(NOW(), INTERVAL 8 DAY)
    AND ORDEV.close_date < DATE(NOW())
    AND PRDEV.network_product_code != "SGN0005"
    AND (
        ORDEV.service_number LIKE '%EV%'
        OR LOWER(BILLEV.package_description) LIKE '%evolve%'
        OR LOWER(BILLEV.component_description) LIKE '%evolve%'
    )
ORDER BY
    ORDEV.close_date,
    ORDEV.order_code;
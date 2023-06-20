SELECT
    ORD.order_code,
    BILL.*
FROM
    o2pprod.RestInterface_order ORD
    JOIN (
        SELECT
            BILLINNER.*
        FROM
            o2pprod.RestInterface_billing BILLINNER
            JOIN (
                SELECT
                    BILLMAX.component_id,
                    BILLMAX.order_id,
                    MAX(BILLMAX.id) AS id
                FROM
                    o2pprod.RestInterface_billing BILLMAX
                GROUP BY
                    BILLMAX.component_id,
                    BILLMAX.order_id
            ) BILLMAX ON BILLMAX.id = BILLINNER.id
    ) BILL ON BILL.order_id = ORD.id
WHERE
    ORD.order_code = "YQH1722003" \G
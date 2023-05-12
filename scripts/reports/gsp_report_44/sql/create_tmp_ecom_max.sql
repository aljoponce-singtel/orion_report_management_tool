CREATE TABLE o2pprod.tmp_ecom_max AS
SELECT
    WorkOrderNo,
    OrderTakenDate,
    OrderTakenBy,
    MAX(fileName) AS fileName
FROM
    pegasusmulesoft.O2P_STPfields
WHERE
    DATE(OrderTakenDate) BETWEEN '2023-03-01'
    AND '2023-03-31'
GROUP BY
    WorkOrderNo,
    OrderTakenDate,
    OrderTakenBy;

ALTER TABLE
    o2pprod.tmp_ecom_max
ADD
    COLUMN order_id int(11);

CREATE INDEX order_id_idx ON o2pprod.tmp_ecom_max (order_id);

UPDATE
    o2pprod.tmp_ecom_max STP,
    o2pprod.RestInterface_order ORD
SET
    STP.order_id = ORD.id
WHERE
    STP.WorkOrderNo = ORD.order_code;
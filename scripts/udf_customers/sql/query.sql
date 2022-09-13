SELECT DISTINCT
    cus.name           AS 'Customer Name'
  , brn.brn            AS 'BRN'
  , ord.order_code     AS 'Work Order Number'
  , ord.service_number AS 'Service Number'
  , ord.taken_date     AS 'Order Creation Date'
  , ord.current_crd    AS 'CRD'
  , ckt.circuit_code   AS 'Circuit Tie'
FROM
    RestInterface_order ord
    LEFT JOIN
        RestInterface_customerbrnmapping brn
        ON
            brn.id = ord.customer_brn_id
    LEFT JOIN
        RestInterface_customer cus
        ON
            cus.id = brn.customer_id
    LEFT JOIN
        RestInterface_circuit ckt
        ON
            ckt.id = ord.circuit_id
WHERE
    brn.brn IN ('199405882Z'         , '197600379E', '201924026H', '198401908G'
              , '0107-01-019678-0001', '199001413D', '8980140', 'S73FC2287H'
              , '200104750M'         , '10384803110', '199701117H', '214-86-18758'
              , '200208943K'         , '08980140', 'F   02287Z' )
    AND YEAR(ord.taken_date) >= '2022'
ORDER BY
    cus.name
  , ord.order_code
;
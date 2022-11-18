SELECT
    CUS.name             AS 'CustomerName'
  , BRN.brn              AS 'BRN'
  , ORD.order_code       AS 'OrderCode'
  , ORD.service_number   AS 'ServiceNumber'
  , ORD.taken_date       AS 'OrderCreationDate'
  , ORD.current_crd      AS 'CRD'
  , CKT.circuit_code     AS 'CircuitCode'
  , DATE(ORD.created_at) AS 'DateAddedToOrion'
FROM
    RestInterface_order ORD
    JOIN
        RestInterface_customerbrnmapping BRN
        ON
            BRN.id = ORD.customer_brn_id
    LEFT JOIN
        RestInterface_customer CUS
        ON
            CUS.id = BRN.customer_id
    LEFT JOIN
        RestInterface_circuit CKT
        ON
            CKT.id = ORD.circuit_id
WHERE
    BRN.brn IN ( '199405882Z'        , '197600379E', '201924026H', '198401908G'
              , '0107-01-019678-0001', '199001413D', '8980140', 'S73FC2287H'
              , '200104750M'         , '10384803110', '199701117H', '214-86-18758'
              , '200208943K'         , '08980140', 'F   02287Z' )
    AND DATE(ORD.created_at) BETWEEN DATE_SUB(DATE(NOW()), INTERVAL 1 DAY) AND DATE(NOW())
ORDER BY
    'DateAddedToOrion'
  , 'CustomerName'
  , 'BRN'
  , 'OrderCode'
;
SELECT
    DISTINCT PRDTYPE1.title AS product_L1,
    PRDTYPE2.title AS product_L2,
    PRDTYPE3.title AS product_L3,
    PRDTYPE4.parent_id AS parent_id,
    PRD.network_product_code AS product_code,
    PRD.network_product_desc AS product_description
FROM
    RestInterface_product PRD
    JOIN RestInterface_producttype PRDTYPE4 ON PRDTYPE4.title = PRD.network_product_code
    JOIN RestInterface_producttype PRDTYPE3 ON PRDTYPE3.id = PRDTYPE4.parent_id
    JOIN RestInterface_producttype PRDTYPE2 ON PRDTYPE2.id = PRDTYPE3.parent_id
    JOIN RestInterface_producttype PRDTYPE1 ON PRDTYPE1.id = PRDTYPE2.parent_id
    LEFT JOIN RestInterface_npp NPP ON NPP.product_id = PRD.id
    LEFT JOIN RestInterface_order ORD ON ORD.id = NPP.order_id
    AND NPP.level = "Mainline"
ORDER BY
    product_L1,
    product_L2,
    product_L3,
    parent_id,
    product_code;
SELECT
    PRDTYPE1.title AS product_L1,
    PRDTYPE2.title AS product_L2,
    PRDTYPE3.title AS product_L3,
    PRDTYPE4.title AS product_code,
    PRD.network_product_desc AS product_description,
    PRDTYPE4.parent_id AS expected_product_type_id,
    PRD.product_type_id AS actual_product_type_id
FROM
    RestInterface_producttype PRDTYPE4
    JOIN RestInterface_producttype PRDTYPE3 ON PRDTYPE3.id = PRDTYPE4.parent_id
    JOIN RestInterface_producttype PRDTYPE2 ON PRDTYPE2.id = PRDTYPE3.parent_id
    JOIN RestInterface_producttype PRDTYPE1 ON PRDTYPE1.id = PRDTYPE2.parent_id
    LEFT JOIN RestInterface_product PRD ON PRD.network_product_code = PRDTYPE4.title
WHERE
    PRD.id IS NOT NULL
    AND PRD.product_type_id IS NULL
ORDER BY
    product_L1,
    product_L2,
    product_L3,
    product_code;
UPDATE
    RestInterface_producttype PRDTYPE4
    JOIN RestInterface_producttype PRDTYPE3 ON PRDTYPE3.id = PRDTYPE4.parent_id
    JOIN RestInterface_producttype PRDTYPE2 ON PRDTYPE2.id = PRDTYPE3.parent_id
    JOIN RestInterface_producttype PRDTYPE1 ON PRDTYPE1.id = PRDTYPE2.parent_id
    LEFT JOIN RestInterface_product PRD ON PRD.network_product_code = PRDTYPE4.title
SET
    PRD.product_type_id = PRDTYPE4.parent_id
WHERE
    PRD.id IS NOT NULL
    AND PRD.product_type_id IS NULL;
SELECT
    DISTINCT QUERY.network_product_desc AS product_description,
    QUERY.bizseg,
    QUERY.eag_gb
FROM
    (
        SELECT
            PRD.network_product_desc,
            MAX(
                CASE
                    WHEN ORD.business_sector LIKE '%bizseg%' THEN 'yes'
                    ELSE 'no'
                END
            ) bizseg,
            MAX(
                CASE
                    WHEN (
                        ORD.business_sector LIKE '%sgo%'
                        OR ORD.business_sector LIKE '%ent and govt%'
                        OR ORD.business_sector LIKE '%global business%'
                    ) THEN 'yes'
                    ELSE 'no'
                END
            ) eag_gb
        FROM
            RestInterface_order ORD
            JOIN RestInterface_npp NPP ON NPP.order_id = ORD.id
            AND NPP.level = 'Mainline'
            AND NPP.status != 'Cancel'
            JOIN RestInterface_product PRD ON PRD.id = NPP.product_id
        WHERE
            ORD.business_sector LIKE '%bizseg%'
            OR ORD.business_sector LIKE '%sgo%'
            OR ORD.business_sector LIKE '%ent and govt%'
            OR ORD.business_sector LIKE '%global business%'
        GROUP BY
            PRD.network_product_desc
    ) QUERY
ORDER BY
    product_description;
SELECT
    DISTINCT QUERY.order_status AS order_status,
    QUERY.bizseg,
    QUERY.eag_gb
FROM
    (
        SELECT
            order_status,
            MAX(
                CASE
                    WHEN business_sector LIKE '%bizseg%' THEN 'yes'
                    ELSE 'no'
                END
            ) bizseg,
            MAX(
                CASE
                    WHEN (
                        business_sector LIKE '%sgo%'
                        OR business_sector LIKE '%ent and govt%'
                        OR business_sector LIKE '%global business%'
                    ) THEN 'yes'
                    ELSE 'no'
                END
            ) eag_gb
        FROM
            RestInterface_order
        WHERE
            business_sector LIKE '%bizseg%'
            OR business_sector LIKE '%sgo%'
            OR business_sector LIKE '%ent and govt%'
            OR business_sector LIKE '%global business%'
        GROUP BY
            order_status
    ) QUERY
ORDER BY
    order_status;
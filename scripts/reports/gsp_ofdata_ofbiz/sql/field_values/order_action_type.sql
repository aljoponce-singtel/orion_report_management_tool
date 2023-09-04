SELECT
    DISTINCT QUERY.ord_action_type AS order_action_type,
    QUERY.bizseg,
    QUERY.eag_gb
FROM
    (
        SELECT
            ord_action_type,
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
            ord_action_type
    ) QUERY
ORDER BY
    order_action_type;
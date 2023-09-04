SELECT
    DISTINCT QUERY.contact_type AS contact_type,
    QUERY.bizseg,
    QUERY.eag_gb
FROM
    (
        SELECT
            CON.contact_type,
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
            JOIN RestInterface_contactdetails CON ON CON.order_id = ORD.id
        WHERE
            ORD.business_sector LIKE '%bizseg%'
            OR ORD.business_sector LIKE '%sgo%'
            OR ORD.business_sector LIKE '%ent and govt%'
            OR ORD.business_sector LIKE '%global business%'
        GROUP BY
            CON.contact_type
    ) QUERY
ORDER BY
    contact_type;
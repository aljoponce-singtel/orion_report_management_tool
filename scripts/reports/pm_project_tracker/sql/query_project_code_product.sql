SELECT
    DISTINCT PRJ.project_code,
    ORD.service_order_number,
    PRJTRK.circuit_tie,
    (
        CASE
            WHEN ORD.service_number REGEXP "^M[0-9]{7}$"
            AND PRD.network_product_code REGEXP "^DME[0-9]{4}$" THEN "MetroE"
            WHEN ORD.service_number REGEXP "^M[0-9]{7}$"
            AND PRD.network_product_code REGEXP "^ISD[0-9]{4}$" THEN "ISDN30"
            WHEN ORD.service_number REGEXP "^GW[0-9]{7}$"
            OR ORD.service_number REGEXP "^GWM[0-9]{7}$" THEN "Gigawave"
            WHEN PRD.network_product_code REGEXP "^CPE[0-9]{4}$" THEN "MS-CPE"
            WHEN ORD.service_number REGEXP "^RMS00000[0-9]{5}$" THEN "MS - RMS"
            WHEN ORD.service_number REGEXP "^[0-9]{8}SNG$" THEN "Cplus IPVPN ( in SG) (GSP)"
            WHEN ORD.service_number REGEXP "^[0-9]{8}(?!SNG)[A-Z]{3}$"
            OR ORD.service_number REGEXP "^[A-Z]{3}[0-9]{10}LLC$" THEN "Cplus IPVPN (Overseas) (GSP)"
        END
    ) AS product_name,
    (
        CASE
            WHEN ORD.service_number REGEXP "^M[0-9]{7}$" THEN "Diginet"
            WHEN ORD.service_number REGEXP "^GW[0-9]{7}$" THEN "Gigawave"
            WHEN ORD.service_number REGEXP "^GWM[0-9]{7}$" THEN "Gigawave Monitoring"
            WHEN PRD.network_product_code REGEXP "^CPE[0-9]{4}$" THEN "CPE"
            WHEN ORD.service_number REGEXP "^RMS00000[0-9]{5}$" THEN "RMS"
            WHEN ORD.service_number REGEXP "^[0-9]{8}SNG$" THEN "Cplus PE Port"
            WHEN ORD.service_number REGEXP "^[0-9]{8}(?!SNG)[A-Z]{3}$" THEN "Cplus PE Port"
            WHEN ORD.service_number REGEXP "^[A-Z]{3}[0-9]{10}LLC$" THEN "OLLC (resale data service)"
        END
    ) AS exp_type_of_work
FROM
    RestInterface_project PRJ
    JOIN RestInterface_order ORD ON ORD.project_id = PRJ.id
    LEFT JOIN tmp_project_tracker PRJTRK ON PRJTRK.order_id = ORD.id
    LEFT JOIN RestInterface_npp NPP ON NPP.order_id = ORD.id
    AND NPP.level = 'Mainline'
    LEFT JOIN RestInterface_product PRD ON PRD.id = NPP.product_id
    LEFT JOIN RestInterface_site SITE ON SITE.id = ORD.site_id
WHERE
    PRJ.project_code = "KWAF153P"
ORDER BY
    PRJ.project_code,
    ORD.service_order_number,
    PRJTRK.circuit_tie,
    product_name,
    exp_type_of_work;
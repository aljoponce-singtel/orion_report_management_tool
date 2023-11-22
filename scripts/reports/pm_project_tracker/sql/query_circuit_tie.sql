SELECT
    DISTINCT PRJ.project_code,
    PRJTRK.billing_dependency_id,
    PRJTRK.circuit_tie,
    ORD.service_order_number AS service_ord_no,
    ORD.taken_date AS creation_date,
    ORD.order_code,
    ORD.order_type,
    ORD.order_status,
    ORD.service_number,
    (
        CASE
            WHEN PRD.network_product_code REGEXP "^DME[0-9]{4}$" THEN "MetroE"
            WHEN PRD.network_product_code REGEXP "^ISD[0-9]{4}$" THEN "ISDN30"
            WHEN PRD.network_product_code REGEXP "^CPE[0-9]{4}$" THEN "MS-CPE"
            WHEN ORD.service_number REGEXP "^RMS[0-9]{10}$" THEN "MS - RMS"
            WHEN ORD.service_number REGEXP "^001[0-9]{5}SNG$" THEN "Cplus IPVPN ( in SG) (GSP)"
            WHEN ORD.service_number REGEXP "^001[0-9]{5}(?!SNG)[A-Z]{3}$"
            OR ORD.service_number REGEXP "^[A-Z]{3}[0-9]{10}LLC$" THEN "Cplus IPVPN (Overseas) (GSP)"
        END
    ) AS product_name,
    (
        CASE
            WHEN PRD.network_product_code REGEXP "^DME[0-9]{4}$" THEN "Diginet"
            WHEN PRD.network_product_code REGEXP "^ISD[0-9]{4}$" THEN "Diginet"
            WHEN PRD.network_product_code REGEXP "^CPE[0-9]{4}$" THEN "CPE"
            WHEN ORD.service_number REGEXP "^RMS[0-9]{10}$" THEN "RMS"
            WHEN ORD.service_number REGEXP "^001[0-9]{5}SNG$" THEN "Cplus PE Port"
            WHEN ORD.service_number REGEXP "^001[0-9]{5}(?!SNG)[A-Z]{3}$" THEN "Cplus PE Port"
            WHEN ORD.service_number REGEXP "^[A-Z]{3}[0-9]{10}LLC$" THEN "OLLC (resale data service)"
        END
    ) AS exp_type_of_work,
    NPP.status AS m_status,
    PRD.network_product_code AS m_product_code,
    ORD.arbor_disp AS arbor_svc_type,
    PRD.network_product_desc AS m_product_description,
    SITE.location AS site_a,
    SITE.second_location AS site_b,
    NULL AS dummy
FROM
    RestInterface_project PRJ
    JOIN RestInterface_order ORD ON ORD.project_id = PRJ.id
    LEFT JOIN tmp_project_tracker PRJTRK ON PRJTRK.order_id = ORD.id
    LEFT JOIN RestInterface_npp NPP ON NPP.order_id = ORD.id
    AND NPP.level = 'Mainline'
    LEFT JOIN RestInterface_product PRD ON PRD.id = NPP.product_id
    LEFT JOIN RestInterface_site SITE ON SITE.id = ORD.site_id
WHERE
    PRJTRK.circuit_tie LIKE "%3566881%"
ORDER BY
    PRJ.project_code,
    ORD.service_order_number,
    ORD.taken_date,
    ORD.order_code;
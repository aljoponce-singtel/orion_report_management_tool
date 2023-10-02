SELECT
    DISTINCT ORD.order_code,
    CUS.name AS customer,
    ORD.order_type,
    ORD.order_status,
    ORD.order_priority,
    ORD.business_sector,
    ORD.current_crd,
    ORD.initial_crd,
    ORD.taken_date,
    ORD.completed_date,
    ORD.close_date,
    ORD.sde_received_date,
    ORD.arbor_service_type,
    ORD.service_number,
    ORD.service_type,
    PRJ.project_code,
    SITE.site_code,
    PRD.network_product_code AS product_code,
    PRD.network_product_desc AS product_description,
    GSP.group_id,
    ACT.name AS activity_name,
    ACT.completed_date AS act_com_date,
    PAR.parameter_name,
    PAR.parameter_value,
    ORD.arbor_disp AS service,
    SITE.country_desc_a_end AS country
FROM
    RestInterface_order ORD
    JOIN RestInterface_activity ACT ON ACT.order_id = ORD.id
    JOIN RestInterface_person PER ON PER.id = ACT.person_id
    JOIN GSP_Q_ownership GSP ON GSP.group_id = PER.role
    LEFT JOIN RestInterface_customer CUS ON CUS.id = ORD.customer_id
    LEFT JOIN RestInterface_project PRJ ON PRJ.id = ORD.project_id
    LEFT JOIN RestInterface_site SITE ON SITE.id = ORD.site_id
    LEFT JOIN RestInterface_npp NPP ON NPP.order_id = ORD.id
    AND NPP.level = 'Mainline'
    AND NPP.status != 'Cancel'
    LEFT JOIN RestInterface_parameter PAR ON PAR.npp_id = NPP.id
    AND PAR.parameter_name = 'Platform'
    LEFT JOIN RestInterface_product PRD ON PRD.id = NPP.product_id
WHERE
    GSP.department = 'GD_GSP'
    AND (
        (
            ORD.order_status = 'Closed'
            AND ORD.close_date BETWEEN '2019-09-01'
            AND NOW()
        )
        OR (
            ACT.completed_date BETWEEN '2019-09-01'
            AND NOW()
            AND (
                (
                    ORD.service_number LIKE 'RMS%'
                    OR ORD.service_number LIKE 'SDF%'
                    OR ORD.service_number LIKE 'SDV%'
                    OR ORD.service_number LIKE 'MLI%'
                    OR ORD.service_number LIKE 'M2M%'
                )
                OR ACT.name = 'Extranet Config'
            )
        )
    );
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
    ESOM_TMP.ready_date AS act_rdy_date,
    ESOM_TMP.completed_date AS act_com_date,
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
    LEFT JOIN (
        SELECT
            DISTINCT ESOM.orderid AS order_id,
            ESOM.workorderno AS order_code,
            ESOM.groupid AS group_id,
            PER.id AS person_id,
            ACT.id AS activity_id,
            ESOM.activityname AS activity_name,
            ESOM_STATUS.ready_date,
            ESOM_STATUS.completed_date
        FROM
            pegasusmulesoft.tmp_ready_com_date ESOM
            JOIN o2pprod.RestInterface_person PER ON PER.role = ESOM.groupid
            LEFT JOIN (
                SELECT
                    orderid,
                    activityname,
                    MAX(
                        CASE
                            WHEN activitystatus = "RDY" THEN activitystatuschangedate
                        END
                    ) ready_date,
                    MAX(
                        CASE
                            WHEN activitystatus = "COM" THEN activitystatuschangedate
                        END
                    ) completed_date
                FROM
                    pegasusmulesoft.tmp_ready_com_date
                GROUP BY
                    orderid,
                    activityname
            ) ESOM_STATUS ON ESOM_STATUS.orderid = ESOM.orderid
            AND ESOM_STATUS.activityname = ESOM.activityname
            LEFT JOIN o2pprod.RestInterface_activity ACT ON ACT.order_id = ESOM_STATUS.orderid
            AND ACT.name = ESOM_STATUS.activityname
        WHERE
            ESOM.orderid IS NOT NULL
    ) ESOM_TMP ON ESOM_TMP.order_id = ACT.order_id
    AND ESOM_TMP.activity_id = ACT.id
WHERE
    GSP.department = 'GD_GSP'
    AND ORD.taken_date BETWEEN '2023-01-01'
    AND "2023-10-01";
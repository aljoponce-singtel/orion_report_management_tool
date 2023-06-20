SELECT
    DISTINCT "TBD" AS qty,
    "TBD" AS user_id,
    "TBD" AS group_code,
    ORD.arbor_disp AS arbor_service_type,
    "TBD" AS cust_id,
    ORD.service_number AS service_number,
    ORD.service_action_type AS service_action_type,
    ORD.order_code,
    ORD.order_type,
    ORD.order_status,
    ORD.current_crd AS cus_req_dt,
    ORD.taken_date AS ord_created_dt,
    (
        CASE
            WHEN ORD.taken_date >= ORD.current_crd THEN "Y"
            ELSE "N"
        END
    ) AS backdated_order,
    PRJ.project_code AS project_code,
    "TBD" AS application_mode,
    (
        CASE
            WHEN CON.contact_type = "AM" THEN CONCAT(CON.family_name, ' ', CON.given_name)
            ELSE ''
        END
    ) AS am,
    (
        CASE
            WHEN CON.contact_type = "SDE" THEN CONCAT(CON.family_name, ' ', CON.given_name)
            ELSE ''
        END
    ) AS sde,
    ORD.applied_date AS applied_date,
    ORD.received_date AS received_date,
    ORD.sde_received_date AS sde_received_date,
    ORD.operation_received_date AS operation_received_date,
    ORD.submitted_date AS submitted_date,
    "TBD" AS tat_for_sales,
    "TBD" AS tat_for_am_to_ord_raised,
    "TBD" AS sde_to_pd_tat,
    "TBD" AS tat_mcc_raising_ord,
    "TBD" AS tat_sde_processing_time,
    ORD.order_priority AS priority,
    "TBD" AS letter_generated,
    ORD.business_sector AS cus_facing_unit,
    "TBD" AS responsibility_code,
    CUS.name AS customer_name,
    ORD.ord_action_type AS order_action_type,
    "TBD" AS sub_rc_description,
    (
        CASE
            WHEN ORD.is_bulk_order = 1 THEN "YES"
            ELSE "NO"
        END
    ) AS bulk_order,
    "TBD" AS am_name,
    ORD.am_id AS am_id,
    ORD.am_email AS am_email,
    "TBD" AS icon_act_ref,
    "TBD" AS focalscope_ticket,
    "TBD" AS impact_mobile_svc_order,
    "TBD" AS impact_svc_ord_number,
    "TBD" AS package_description,
    "TBD" AS sde_wo_tat,
    (
        CASE
            WHEN CON.contact_type = "Project Manager" THEN CONCAT(CON.family_name, ' ', CON.given_name)
            ELSE ''
        END
    ) AS project_manager,
    "TBD" AS fdd,
    "TBD" AS foc_date,
    "TBD" AS date_recv_from_parter_foc,
    "TBD" AS ollc_order_placed,
    "TBD" AS site_survey_date,
    "TBD" AS dpe_received_date,
    "TBD" AS parter_ack_date,
    ORD.initial_crd AS initial_crd,
    "TBD" AS tat_ord_applied_to_sde_rcvd,
    "TBD" AS tat_sde_rcvd_to_ord_submtd,
    "TBD" AS new_installation_name,
    "TBD" AS currency,
    "TBD" AS estimated_mrc,
    "TBD" AS estimated_otc,
    SINOTE.categoty AS notes_category,
    SINOTE.sub_categoty AS notes_subcategory,
    "TBD" AS mainline_component_desc,
    "TBD" AS otc_component_desc
FROM
    o2pprod.RestInterface_order ORD
    LEFT JOIN o2pprod.RestInterface_project PRJ ON PRJ.id = ORD.project_id
    LEFT JOIN o2pprod.RestInterface_contactdetails CON ON CON.order_id = ORD.id
    AND CON.contact_type IN ("AM", "SDE", "Project Manager")
    LEFT JOIN o2pprod.RestInterface_customer CUS ON CUS.id = ORD.customer_id
    LEFT JOIN o2pprod.RestInterface_customerbrnmapping BRN ON BRN.id = ORD.customer_brn_id
    LEFT JOIN o2pprod.RestInterface_usercustomerbrnmapping USRBRN ON USRBRN.customer_brn_id = ORD.customer_brn_id
    LEFT JOIN o2pprod.RestInterface_billing BILL ON BILL.order_id = ORD.id
    LEFT JOIN (
        SELECT
            SINOTEINNER.*
        FROM
            o2pprod.RestInterface_ordersinote SINOTEINNER
            JOIN (
                SELECT
                    order_id,
                    MAX(note_code) AS note_code
                FROM
                    o2pprod.RestInterface_ordersinote
                WHERE
                    categoty = 'eRequest'
                    OR categoty = 'Non eRequest'
                GROUP BY
                    order_id
            ) SINOTEMAX ON SINOTEMAX.order_id = SINOTEINNER.order_id
            AND SINOTEMAX.note_code = SINOTEINNER.note_code
    ) SINOTE ON SINOTE.order_id = ORD.id
WHERE
    ORD.taken_date BETWEEN '{}'
    AND '{}';
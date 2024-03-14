use o2puat;

SELECT DISTINCT
    ORD.order_code,
    PRJTRK.product_category,
    PRJTRK.circuit_layer,
    PRJTRK.type_of_product,
    PRJTRK.type_of_work,
    ORD.current_crd,
    ORD.initial_crd,
    ORD.order_status,
    ORD.order_type,
    ORD.order_priority,
    ORD.ord_action_type,
    (
        CASE
            WHEN ORD.delivery_status = 1 THEN "WIP On Time"
            WHEN ORD.delivery_status = 2 THEN "WIP At Risk"
            WHEN ORD.delivery_status = 3 THEN "WIP Delay"
            WHEN ORD.delivery_status = 4 THEN "Delivered"
            WHEN ORD.delivery_status = 5 THEN "Delivered Delay"
            WHEN ORD.delivery_status = 6 THEN "NA"
        END
    ) AS delivery_status,
    (
        CASE
            WHEN ORD.closure_status = 1 THEN "Closed"
            WHEN ORD.closure_status = 2 THEN "Closed Delay"
            WHEN ORD.closure_status = 3 THEN "Not Closed"
            WHEN ORD.closure_status = 4 THEN "Not Closed Delay"
            WHEN ORD.closure_status = 5 THEN "Cancelled"
            WHEN ORD.closure_status = 6 THEN "Pending Cancellation"
        END
    ) AS closure_status,
    ORD.applied_date,
    ORD.arbor_disp,
    PRD.network_product_desc,
    SITE.location AS new_installation_address_A,
    SITE.second_location AS new_installation_address_B,
    SITEOLD.location AS existing_installation_address_A,
    SITEOLD.location_second AS existing_installation_address_B
FROM
    o2puat.RestInterface_order ORD
    JOIN o2ptest.project_tracker_group PRJTRK ON PRJTRK.order_id = ORD.id
    LEFT JOIN o2puat.RestInterface_npp NPP ON NPP.order_id = ORD.id
    AND NPP.level = "Mainline"
    AND NPP.status != "Cancel"
    LEFT JOIN o2puat.RestInterface_product PRD ON PRD.id = NPP.product_id
    LEFT JOIN o2puat.RestInterface_site SITE ON SITE.id = ORD.site_id
    LEFT JOIN o2puat.RestInterface_siteold SITEOLD ON SITEOLD.id = ORD.site_old_id
WHERE
    ORD.project_tracker_group_name = (
        SELECT svc_ord_no
        FROM o2ptest.project_tracker_group
        LIMIT 1
    )
ORDER BY ORD.order_code;
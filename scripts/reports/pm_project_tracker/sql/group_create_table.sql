USE o2puat;

DROP TABLE IF EXISTS o2ptest.project_tracker_group;

CREATE TABLE IF NOT EXISTS o2ptest.project_tracker_group AS
SELECT
    MAP_PRJTRK.project_code,
    MAP_PRJTRK.svc_ord_no,
    (
        CASE
            WHEN MAP_PRJTRK.type_of_work = "Diginet" THEN "Domestic"
            WHEN MAP_PRJTRK.type_of_work LIKE "GW%" THEN "Domestic"
            WHEN MAP_PRJTRK.type_of_work = "SingNet" THEN "Domestic"
            WHEN MAP_PRJTRK.type_of_work = "Cplus PE Port" THEN "International"
            WHEN MAP_PRJTRK.type_of_work = "Cplus ILC / Eline" THEN "International"
            WHEN MAP_PRJTRK.type_of_work = "OLLC (resale data service)" THEN "International"
            WHEN MAP_PRJTRK.type_of_work = "CPE" THEN "Hardware"
            WHEN MAP_PRJTRK.type_of_work = "RMS" THEN "Hardware"
            WHEN MAP_PRJTRK.type_of_work = "SDWAN" THEN "Hardware"
            ELSE NULL
        END
    ) AS product_category,
    (
        CASE
            WHEN MAP_PRJTRK.type_of_work = "Diginet" THEN "layer_2"
            WHEN MAP_PRJTRK.type_of_work LIKE "GW%" THEN "layer_2"
            WHEN MAP_PRJTRK.type_of_work = "SingNet" THEN "layer_3"
            WHEN MAP_PRJTRK.type_of_work = "Cplus PE Port" THEN "layer_3"
            WHEN MAP_PRJTRK.type_of_work = "Cplus ILC / Eline" THEN "layer_2"
            WHEN MAP_PRJTRK.type_of_work = "OLLC (resale data service)" THEN "layer_2"
            WHEN MAP_PRJTRK.type_of_work = "CPE" THEN "hardware"
            WHEN MAP_PRJTRK.type_of_work = "RMS" THEN "software"
            WHEN MAP_PRJTRK.type_of_work = "SDWAN" THEN "hardware"
            ELSE NULL
        END
    ) AS circuit_layer,
    (
        CASE
            WHEN MAP_PRJTRK.type_of_work = "Diginet" THEN "MetroE"
            WHEN MAP_PRJTRK.type_of_work LIKE "GW%" THEN "Gigawave"
            WHEN MAP_PRJTRK.type_of_work = "SingNet" THEN "SingNet"
            WHEN MAP_PRJTRK.type_of_work = "Cplus PE Port" THEN "CPlusIP"
            WHEN MAP_PRJTRK.type_of_work = "Cplus ILC / Eline" THEN "Eline"
            WHEN MAP_PRJTRK.type_of_work = "OLLC (resale data service)" THEN "OLLC"
            WHEN MAP_PRJTRK.type_of_work = "CPE" THEN "CPE"
            WHEN MAP_PRJTRK.type_of_work = "RMS" THEN "RMS"
            WHEN MAP_PRJTRK.type_of_work = "SDWAN" THEN "SDWAN"
            ELSE NULL
        END
    ) AS type_of_product,
    MAP_PRJTRK.type_of_work,
    MAP_PRJTRK.order_code,
    MAP_PRJTRK.service_number,
    MAP_PRJTRK.arbor_disp,
    MAP_PRJTRK.network_product_desc,
    MAP_PRJTRK.order_id,
    MAP_PRJTRK.npp_id,
    MAP_PRJTRK.product_id
FROM (
        SELECT
            MAP_PRJ.project_code, MAP_ORD.service_order_number AS svc_ord_no, (
                CASE
                    WHEN MAP_ORD.service_number REGEXP "^M\\d{7}$"
                    AND MAP_ORD.arbor_disp = "Diginet" THEN "Diginet"
                    WHEN MAP_ORD.service_number REGEXP "^GW[0-9]{7}$"
                    AND MAP_ORD.arbor_disp = "Diginet"
                    AND MAP_PRD.network_product_desc LIKE "%Channel%" THEN "GW Channel"
                    WHEN MAP_ORD.service_number REGEXP "^GW[0-9]{7}$"
                    AND MAP_ORD.arbor_disp = "Diginet"
                    AND MAP_PRD.network_product_desc LIKE "%Fibre%" THEN "GW Fiber Std"
                    WHEN MAP_ORD.service_number REGEXP "^GW[0-9]{7}$"
                    AND MAP_ORD.arbor_disp = "Diginet"
                    AND MAP_PRD.network_product_desc LIKE "%Diversity%" THEN "GW Fiber Diverse"
                    WHEN MAP_ORD.service_number REGEXP "^GWM[0-9]{7}$"
                    AND MAP_ORD.arbor_disp = "Diginet"
                    AND MAP_PRD.network_product_desc = "GWL Monitoring over MetroE" THEN "GW Monitoring"
                    WHEN MAP_ORD.service_number REGEXP "^ETHCE[0-9]{8}SNG$"
                    AND MAP_ORD.arbor_disp = "SingNet" THEN "Singnet"
                    WHEN MAP_ORD.service_number REGEXP "^ELITE[0-9]{8}SNG$"
                    AND MAP_ORD.arbor_disp = "SingNet" THEN "Singnet"
                    WHEN MAP_ORD.service_number REGEXP "^00[0-9]{6}[a-zA-Z]{3}$"
                    AND MAP_ORD.arbor_disp = "CN - ConnectPlus IP VPN" THEN "Cplus PE Port"
                    WHEN MAP_ORD.arbor_disp IN ("ILC", "ConnectPlus E-Line") THEN "Cplus ILC / Eline"
                    WHEN MAP_ORD.service_number REGEXP "^\\p{L}{3}\\d{10}LLC$"
                    AND MAP_ORD.arbor_disp = "Resale of Overseas Data Svcs" THEN "OLLC (resale data service)"
                    WHEN MAP_ORD.arbor_disp = "CPE"
                    AND MAP_PRD.network_product_desc IN (
                        "Rental Of CPE", "Sale of CPE"
                    ) THEN "CPE"
                    WHEN MAP_ORD.arbor_disp = "Router Mgmt Svc" THEN "RMS"
                    WHEN MAP_ORD.arbor_disp = "CN - ConnectPlus IP VPN"
                    AND MAP_PRD.network_product_desc LIKE "C+ SDW%" THEN "SDWAN"
                    ELSE NULL
                END
            ) AS type_of_work, MAP_ORD.order_code, MAP_ORD.service_number, MAP_ORD.arbor_disp, MAP_PRD.network_product_desc, MAP_ORD.id AS order_id, MAP_NPP.id AS npp_id, MAP_PRD.id AS product_id
        FROM
            o2puat.RestInterface_project MAP_PRJ
            JOIN o2puat.RestInterface_order MAP_ORD ON MAP_ORD.project_id = MAP_PRJ.id
            JOIN o2puat.RestInterface_npp MAP_NPP ON MAP_NPP.order_id = MAP_ORD.id
            JOIN o2puat.RestInterface_product MAP_PRD ON MAP_PRD.id = MAP_NPP.product_id
            AND MAP_NPP.level = "Mainline"
            AND MAP_NPP.status != "Cancel"
        WHERE
            MAP_ORD.project_tracker_group_name = "ZHT8643"
    ) MAP_PRJTRK
ORDER BY
    MAP_PRJTRK.project_code,
    MAP_PRJTRK.svc_ord_no,
    product_category DESC,
    circuit_layer DESC,
    MAP_PRJTRK.type_of_work,
    MAP_PRJTRK.order_code;
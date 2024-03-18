USE o2puat;

DROP TABLE IF EXISTS o2ptest.project_tracker_group;

CREATE TABLE IF NOT EXISTS o2ptest.project_tracker_group AS
SELECT DISTINCT
    MAP_PRJTRK.project_code,
    MAP_PRJTRK.project_tracker_site_id,
    MAP_PRJTRK.project_tracker_group_name,
    MAP_PRJTRK.circuit_code,
    MAP_PRJTRK.service_order_number,
    (
        CASE
            WHEN MAP_PRJTRK.type_of_work = "Diginet" THEN "Domestic"
            WHEN MAP_PRJTRK.type_of_work LIKE "GW%" THEN "Domestic"
            WHEN MAP_PRJTRK.type_of_work = "FTTH" THEN "Domestic"
            WHEN MAP_PRJTRK.type_of_work = "MegaPOP VPN" THEN "Domestic"
            WHEN MAP_PRJTRK.type_of_work = "MegaPOP (CE)" THEN "Domestic"
            WHEN MAP_PRJTRK.type_of_work = "MegaPOP" THEN "Domestic"
            WHEN MAP_PRJTRK.type_of_work = "SingNet" THEN "Domestic"
            WHEN MAP_PRJTRK.type_of_work = "Telephone Numbers" THEN "Domestic"
            WHEN MAP_PRJTRK.type_of_work = "Cplus VPN" THEN "International"
            WHEN MAP_PRJTRK.type_of_work = "Cplus PE Port" THEN "International"
            WHEN MAP_PRJTRK.type_of_work = "OLLC (resale data service)" THEN "International"
            WHEN MAP_PRJTRK.type_of_work = "Cplus ILC / Eline" THEN "International"
            WHEN MAP_PRJTRK.type_of_work = "STiX" THEN "International"
            WHEN MAP_PRJTRK.type_of_work = "Cplus Global Internet (CGI)" THEN "International"
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
            WHEN MAP_PRJTRK.type_of_work = "FTTH" THEN "layer_2"
            WHEN MAP_PRJTRK.type_of_work = "MegaPOP VPN" THEN "layer_3"
            WHEN MAP_PRJTRK.type_of_work = "MegaPOP (CE)" THEN "layer_3"
            WHEN MAP_PRJTRK.type_of_work = "MegaPOP" THEN "layer_3"
            WHEN MAP_PRJTRK.type_of_work = "SingNet" THEN "layer_3"
            WHEN MAP_PRJTRK.type_of_work = "Telephone Numbers" THEN "layer_voice"
            WHEN MAP_PRJTRK.type_of_work = "Cplus VPN" THEN "layer_3"
            WHEN MAP_PRJTRK.type_of_work = "Cplus PE Port" THEN "layer_3"
            WHEN MAP_PRJTRK.type_of_work = "OLLC (resale data service)" THEN "layer_2"
            WHEN MAP_PRJTRK.type_of_work = "Cplus ILC / Eline" THEN "layer_2"
            WHEN MAP_PRJTRK.type_of_work = "STiX" THEN "layer_3"
            WHEN MAP_PRJTRK.type_of_work = "Cplus Global Internet (CGI)" THEN "layer_3"
            WHEN MAP_PRJTRK.type_of_work = "CPE" THEN "hardware"
            WHEN MAP_PRJTRK.type_of_work = "RMS" THEN "software"
            WHEN MAP_PRJTRK.type_of_work = "SDWAN" THEN "hardware"
            ELSE NULL
        END
    ) AS circuit_layer,
    (
        CASE
            WHEN MAP_PRJTRK.type_of_work = "Diginet"
            AND MAP_PRJTRK.service_number REGEXP "^M\\d{7}$" THEN "MetroE"
            WHEN MAP_PRJTRK.type_of_work LIKE "GW%" THEN "Gigawave"
            WHEN MAP_PRJTRK.type_of_work = "FTTH" THEN "Elite"
            WHEN MAP_PRJTRK.type_of_work = "MegaPOP VPN" THEN "MegaPOP"
            WHEN MAP_PRJTRK.type_of_work = "MegaPOP (CE)" THEN "MegaPOP_OR_Ethernet"
            WHEN MAP_PRJTRK.type_of_work = "MegaPOP" THEN "MegaPOP"
            WHEN MAP_PRJTRK.type_of_work = "SingNet"
            AND MAP_PRJTRK.service_number REGEXP "^ETHCE\\d{8}SNG$" THEN "SingNet_OR_Ethernet"
            WHEN MAP_PRJTRK.type_of_work = "SingNet"
            AND MAP_PRJTRK.service_number REGEXP "^ELITE\\d{8}SNG$" THEN "SingNet"
            WHEN MAP_PRJTRK.type_of_work = "SingNet"
            AND MAP_PRJTRK.service_number REGEXP "^GWLGW\\d{7}$" THEN "SingNet"
            WHEN MAP_PRJTRK.type_of_work = "Diginet"
            AND MAP_PRJTRK.service_number REGEXP "^\\d{7}$" THEN "ISDN"
            WHEN MAP_PRJTRK.type_of_work = "Telephone Numbers"
            AND MAP_PRJTRK.service_type = "ISDN" THEN "ISDN_Telephone_Numbers"
            WHEN MAP_PRJTRK.type_of_work = "Cplus VPN" THEN "CPlusIP"
            WHEN MAP_PRJTRK.type_of_work = "Cplus PE Port" THEN "CPlusIP"
            WHEN MAP_PRJTRK.type_of_work = "OLLC (resale data service)" THEN "OLLC"
            WHEN MAP_PRJTRK.type_of_work = "Cplus ILC / Eline" THEN "Eline"
            WHEN MAP_PRJTRK.type_of_work = "Cplus Global Internet (CGI)" THEN "CGI"
            WHEN MAP_PRJTRK.type_of_work = "STiX" THEN "STiX"
            WHEN MAP_PRJTRK.type_of_work = "CPE" THEN "CPE"
            WHEN MAP_PRJTRK.type_of_work = "RMS" THEN "RMS"
            WHEN MAP_PRJTRK.type_of_work = "SDWAN" THEN "SDWAN"
            ELSE NULL
        END
    ) AS type_of_product,
    MAP_PRJTRK.type_of_work,
    MAP_PRJTRK.order_code,
    MAP_PRJTRK.service_number,
    MAP_PRJTRK.service_type,
    MAP_PRJTRK.order_product_description,
    MAP_PRJTRK.npp_product_description,
    MAP_PRJTRK.project_id,
    MAP_PRJTRK.order_id,
    MAP_PRJTRK.npp_id,
    MAP_PRJTRK.product_id
FROM (
        SELECT DISTINCT
            MAP_PRJ.project_code, MAP_ORD.project_tracker_site_id, MAP_ORD.project_tracker_group_name, MAP_CKT.circuit_code, MAP_ORD.service_order_number, (
                CASE
                    WHEN MAP_ORD.service_number REGEXP "^M\\d{7}$"
                    AND MAP_ORD.arbor_disp = "Diginet" THEN "Diginet"
                    WHEN MAP_ORD.service_number REGEXP "^GW\\d{7}$"
                    AND MAP_ORD.arbor_disp = "Diginet"
                    AND MAP_ORD.product_description LIKE "%Channel%" THEN "GW Channel"
                    WHEN MAP_ORD.service_number REGEXP "^GW\\d{7}$"
                    AND MAP_ORD.arbor_disp = "Diginet"
                    AND MAP_ORD.product_description LIKE "%Fibre%" THEN "GW Fiber Std"
                    WHEN MAP_ORD.service_number REGEXP "^GW\\d{7}$"
                    AND MAP_ORD.arbor_disp = "Diginet"
                    AND MAP_ORD.product_description LIKE "%Diversity%" THEN "GW Fiber Diverse"
                    WHEN MAP_ORD.service_number REGEXP "^GWM\\d{7}$"
                    AND MAP_ORD.arbor_disp = "Diginet"
                    AND MAP_ORD.product_description = "GWL Monitoring over MetroE" THEN "GW Monitoring"
                    WHEN MAP_ORD.arbor_disp = "TS - FTTH" THEN "FTTH"
                    WHEN MAP_ORD.arbor_disp = "CN - Meg@pop Suite Of IP Services"
                    AND MAP_ORD.product_description = "Intranet VPN" THEN "MegaPOP VPN"
                    WHEN MAP_ORD.service_number REGEXP "^CE\\d{8}SNG$"
                    AND MAP_ORD.arbor_disp = "CN - Meg@pop Suite Of IP Services" THEN "MegaPOP (CE)"
                    WHEN MAP_ORD.service_number REGEXP "^\\d{8}SNG$"
                    AND MAP_ORD.arbor_disp = "CN - Meg@pop Suite Of IP Services" THEN "MegaPOP"
                    WHEN MAP_ORD.service_number REGEXP "^ETHCE\\d{8}SNG$"
                    AND MAP_ORD.arbor_disp = "SingNet" THEN "SingNet"
                    WHEN MAP_ORD.service_number REGEXP "^ELITE\\d{8}SNG$"
                    AND MAP_ORD.arbor_disp = "SingNet" THEN "SingNet"
                    WHEN MAP_ORD.service_number REGEXP "^GWLGW\\d{7}$"
                    AND MAP_ORD.arbor_disp = "SingNet" THEN "SingNet"
                    WHEN MAP_ORD.service_number REGEXP "^\\d{7}$"
                    AND MAP_ORD.arbor_disp = "Diginet"
                    AND MAP_ORD.product_description LIKE "%ISDN%" THEN "Diginet"
                    WHEN MAP_ORD.arbor_disp = "ISDN" THEN "Telephone Numbers"
                    WHEN MAP_ORD.arbor_disp = "CN - ConnectPlus IP VPN"
                    AND MAP_ORD.product_description = "Intranet VPN" THEN "Cplus VPN"
                    WHEN MAP_ORD.service_number REGEXP "^00\\d{6}\\p{L}{3}$"
                    AND MAP_ORD.arbor_disp = "CN - ConnectPlus IP VPN" THEN "Cplus PE Port"
                    WHEN MAP_ORD.service_number REGEXP "^\\p{L}{3}\\d{10}LLC$"
                    AND MAP_ORD.arbor_disp = "Resale of Overseas Data Svcs" THEN "OLLC (resale data service)"
                    WHEN MAP_ORD.arbor_disp IN ("ILC", "ConnectPlus E-Line") THEN "Cplus ILC / Eline"
                    WHEN MAP_ORD.arbor_disp = "STIX Gateway" THEN "STiX"
                    WHEN MAP_ORD.arbor_disp = "ISDN" THEN "Telephone Numbers"
                    WHEN MAP_ORD.arbor_disp = "CN - ConnectPlus IP VPN"
                    AND MAP_ORD.product_description = "C+ Global Internet" THEN "Cplus Global Internet (CGI)"
                    WHEN MAP_ORD.arbor_disp = "CPE"
                    AND MAP_ORD.product_description IN (
                        "Rental Of CPE", "Sale of CPE"
                    ) THEN "CPE"
                    WHEN MAP_ORD.arbor_disp = "Router Mgmt Svc" THEN "RMS"
                    WHEN MAP_ORD.arbor_disp = "CN - ConnectPlus IP VPN"
                    AND MAP_ORD.product_description LIKE "C+ SDW%" THEN "SDWAN"
                    ELSE NULL
                END
            ) AS type_of_work, MAP_ORD.order_code, MAP_ORD.service_number, MAP_ORD.arbor_disp AS service_type, MAP_ORD.product_description AS order_product_description, MAP_NPP.network_product_desc AS npp_product_description, MAP_PRJ.id AS project_id, MAP_CKT.id AS circuit_id, MAP_ORD.id AS order_id, MAP_NPP.npp_id, MAP_NPP.product_id
        FROM
            o2puat.RestInterface_project MAP_PRJ
            JOIN o2puat.RestInterface_order MAP_ORD ON MAP_ORD.project_id = MAP_PRJ.id
            LEFT JOIN o2puat.RestInterface_circuit MAP_CKT ON MAP_CKT.id = MAP_ORD.circuit_id
            LEFT JOIN (
                SELECT NPP.order_id, NPP.id AS npp_id, PRD.id AS product_id, PRD.network_product_desc
                FROM o2puat.RestInterface_npp NPP
                    JOIN o2puat.RestInterface_product PRD ON PRD.id = NPP.product_id
                    AND NPP.level = "Mainline"
                    AND NPP.status != "Cancel"
            ) MAP_NPP ON MAP_NPP.order_id = MAP_ORD.id
        WHERE
            MAP_ORD.project_tracker_group_name = "ZFN8263"
    ) MAP_PRJTRK
ORDER BY
    MAP_PRJTRK.project_code,
    MAP_PRJTRK.circuit_code,
    MAP_PRJTRK.service_order_number,
    product_category DESC,
    circuit_layer DESC,
    MAP_PRJTRK.type_of_work,
    MAP_PRJTRK.order_code;
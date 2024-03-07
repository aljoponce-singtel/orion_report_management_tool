USE o2puat;

SELECT DISTINCT
    PRJTRK.project_code,
    PRJTRK.svc_ord_no,
    PRJTRK.product_category,
    PRJTRK.circuit_layer,
    PRJTRK.type_of_product,
    PRJTRK.type_of_work,
    PRJTRK.order_code,
    PRJTRK.service_number,
    ORD.order_type,
    ORD.order_status,
    ORD.taken_date,
    ORD.current_crd,
    ORD.completed_date,
    PRJTRK.arbor_disp,
    ORD.product_description,
    -- PRJTRK.network_product_desc,
    (
        SELECT (
                CASE
                    WHEN PRJTRK.product_category = "Domestic" THEN "Singapore"
                    ELSE GROUP_CONCAT(
                        CONCAT_WS(
                            " - ", PAR.parameter_name, PAR.parameter_value
                        ) SEPARATOR "; "
                    )
                END
            )
        FROM
            o2puat.RestInterface_npp NPP
            JOIN RestInterface_parameter PAR ON PAR.npp_id = NPP.id
        WHERE
            PRJTRK.npp_id = NPP.id
            AND (
                (
                    PRJTRK.type_of_product = "OLLC"
                    AND PAR.parameter_name IN ("TermCtry")
                )
                OR (
                    PRJTRK.type_of_product = "CPE"
                    AND PAR.parameter_name IN ("DestinationCtry")
                )
                OR (
                    PRJTRK.type_of_product = "RMS"
                    AND PAR.parameter_name IN ("LeadCtry")
                )
                OR (
                    PRJTRK.type_of_product = "SDWAN"
                    AND PAR.parameter_name IN ("TermCtry")
                )
                OR PRJTRK.type_of_product NOT IN("OLLC", "CPE", "RMS", "SDWAN")
                AND PAR.parameter_name IN (
                    "TermCtry", "DestinationCtry", "LeadCtry"
                )
                OR PRJTRK.product_category = "Domestic"
            )
    ) AS country,
    CON.clarification_cust,
    CON.a_end_cust,
    CON.technical_cust,
    (
        CASE
            WHEN (
                SELECT COUNT(DISTINCT PRJTRK.order_id)
                FROM o2puat.RestInterface_order ORD
                    JOIN o2ptest.project_tracker_group PRJTRK ON PRJTRK.order_id = ORD.id
                WHERE
                    ORD.order_status != "Completed"
            ) > 0 THEN (
                SELECT DATEDIFF(
                        DATE(NOW()), MIN(ORD.taken_date)
                    )
                FROM o2puat.RestInterface_order ORD
                    JOIN o2ptest.project_tracker_group PRJTRK ON PRJTRK.order_id = ORD.id
            )
            ELSE (
                SELECT DATEDIFF(
                        MAX(ORD.completed_date), MIN(ORD.taken_date)
                    )
                FROM o2puat.RestInterface_order ORD
                    JOIN o2ptest.project_tracker_group PRJTRK ON PRJTRK.order_id = ORD.id
            )
        END
    ) AS order_lapse,
    SITE.site_code,
    SITE.site_code_second,
    (
        SELECT (
                CASE
                    WHEN PRJTRK.type_of_product IN (
                        "Elite", "MetroE", "Gigawave", "MegaPOP_OR_Ethernet", "SingNet_OR_Ethernet"
                    ) THEN GROUP_CONCAT(
                        CONCAT_WS(
                            " - ", PAR.parameter_name, PAR.parameter_value
                        ) SEPARATOR "; "
                    )
                    ELSE GROUP_CONCAT(
                        CONCAT_WS(
                            " - ", PAR.parameter_name, PAR.parameter_value
                        ) SEPARATOR "; "
                    )
                END
            )
        FROM
            o2puat.RestInterface_npp NPP
            JOIN RestInterface_parameter PAR ON PAR.npp_id = NPP.id
        WHERE
            PRJTRK.npp_id = NPP.id
            AND LOWER(PAR.parameter_name) = "speed"
    ) AS sg_dom_bandwidth,
    ACT.circuit_survey_a_end,
    ACT.circuit_survey_b_end,
    ACT.inhouse_cabling_a_end,
    ACT.inhouse_cabling_b_end,
    ACT.mux_installation_a_end,
    ACT.mux_installation_b_end,
    (
        SELECT GROUP_CONCAT(
                CONCAT_WS(
                    " - ", PAR.parameter_name, PAR.parameter_value
                ) SEPARATOR "; "
            )
        FROM
            o2puat.RestInterface_npp NPP
            JOIN RestInterface_parameter PAR ON PAR.npp_id = NPP.id
        WHERE
            PRJTRK.order_id = NPP.order_id
            AND (
                (
                    PRJTRK.type_of_product = "CPlusIP"
                    AND PAR.parameter_name IN ("PoPLocation")
                )
                OR (
                    PRJTRK.type_of_product = "Eline"
                    AND PAR.parameter_name IN ("AendPOP", "BendPOP")
                )
                OR (
                    PRJTRK.type_of_product NOT IN("CPlusIP", "Eline")
                    AND PAR.parameter_name IN (
                        "PoPLocation", "AendPOP", "BendPOP"
                    )
                )
            )
    ) AS pop_location_a_b,
    (
        SELECT GROUP_CONCAT(
                CONCAT_WS(
                    " - ", PAR.parameter_name, PAR.parameter_value
                ) SEPARATOR "; "
            )
        FROM
            o2puat.RestInterface_npp NPP
            JOIN RestInterface_parameter PAR ON PAR.npp_id = NPP.id
        WHERE
            PRJTRK.order_id = NPP.order_id
            AND (
                (
                    PRJTRK.type_of_product = "CPlusIP"
                    AND PAR.parameter_name IN ("OriginPartner")
                )
                OR (
                    PRJTRK.type_of_product = "CGI"
                    AND PAR.parameter_name IN (
                        "RemDSLPartName", "PartnerCctRef"
                    )
                )
                OR (
                    PRJTRK.type_of_product = "OLLC"
                    AND PAR.parameter_name IN (
                        "LLC_Partner_Name", "LLC_Xconn_Partner_Name"
                    )
                )
                OR (
                    PRJTRK.type_of_product NOT IN("CPlusIP", "CGI", "OLLC")
                    AND PAR.parameter_name IN (
                        "OriginPartner", "RemDSLPartName", "PartnerCctRef", "LLC_Partner_Name", "LLC_Xconn_Partner_Name"
                    )
                )
            )
    ) AS partner_lsp_name_a_b,
    (
        SELECT GROUP_CONCAT(
                CONCAT_WS(
                    " - ", PAR.parameter_name, PAR.parameter_value
                ) SEPARATOR "; "
            )
        FROM
            o2puat.RestInterface_npp NPP
            JOIN RestInterface_parameter PAR ON PAR.npp_id = NPP.id
        WHERE
            PRJTRK.npp_id = NPP.id
            AND (
                (
                    PRJTRK.type_of_product IN ("OLLC", "Eline", "MetroE")
                    AND LOWER(PAR.parameter_name) = "speed"
                )
                OR (
                    PRJTRK.type_of_product = "CGI"
                    AND PAR.parameter_name IN (
                        "Speed", "speed" "LocIntSpeedDownstr", "LocIntSpeedUpstr"
                    )
                )
                OR (
                    PRJTRK.type_of_product NOT IN(
                        "OLLC", "Eline", "MetroE", "CGI"
                    )
                    AND PAR.parameter_name IN (
                        "Speed", "speed" "LocIntSpeedDownstr", "LocIntSpeedUpstr"
                    )
                )
            )
    ) AS ollc_bandwidth_a_b,
    (
        SELECT GROUP_CONCAT(
                CONCAT_WS(
                    " - ", PAR.parameter_name, PAR.parameter_value
                ) SEPARATOR "; "
            )
        FROM
            o2puat.RestInterface_npp NPP
            JOIN RestInterface_parameter PAR ON PAR.npp_id = NPP.id
        WHERE
            PRJTRK.npp_id = NPP.id
            AND (
                (
                    PRJTRK.type_of_product = "OLLC"
                    AND PAR.parameter_name IN ("interface")
                )
                OR (
                    PRJTRK.type_of_product = "Eline"
                    AND PAR.parameter_name IN ("CustInterPort")
                )
                OR (
                    PRJTRK.type_of_product = "MetroE"
                    AND PAR.parameter_name IN ("ConnectorTypeAEnd")
                )
                OR (
                    PRJTRK.type_of_product = "CGI"
                    AND PAR.parameter_name IN ("IntInterface")
                )
                OR (
                    PRJTRK.type_of_product NOT IN(
                        "OLLC", "Eline", "MetroE", "CGI"
                    )
                    AND PAR.parameter_name IN (
                        "interface", "speCustInterPorted" "ConnectorTypeAEnd", "IntInterface"
                    )
                )
            )
    ) AS circuit_handoff_a_b,
    (
        SELECT GROUP_CONCAT(
                CONCAT_WS(
                    " - ", PAR.parameter_name, PAR.parameter_value
                ) SEPARATOR "; "
            )
        FROM
            o2puat.RestInterface_npp NPP
            JOIN RestInterface_parameter PAR ON PAR.npp_id = NPP.id
        WHERE
            PRJTRK.npp_id = NPP.id
            AND PRJTRK.type_of_product = "OLLC"
            AND PAR.parameter_name IN ("OLLCOrdPld")
    ) AS ollc_order_placed_date,
    (
        SELECT GROUP_CONCAT(
                CONCAT_WS(
                    " - ", PAR.parameter_name, PAR.parameter_value
                ) SEPARATOR "; "
            )
        FROM
            o2puat.RestInterface_npp NPP
            JOIN RestInterface_parameter PAR ON PAR.npp_id = NPP.id
        WHERE
            PRJTRK.npp_id = NPP.id
            AND PRJTRK.type_of_product = "OLLC"
            AND PAR.parameter_name IN (
                "Date_Received_From_Partner_FOC"
            )
    ) AS foc_received_date,
    (
        SELECT GROUP_CONCAT(
                CONCAT_WS(
                    " - ", PAR.parameter_name, PAR.parameter_value
                ) SEPARATOR "; "
            )
        FROM
            o2puat.RestInterface_npp NPP
            JOIN RestInterface_parameter PAR ON PAR.npp_id = NPP.id
        WHERE
            PRJTRK.npp_id = NPP.id
            AND PRJTRK.type_of_product = "OLLC"
            AND PAR.parameter_name IN ("FOC_Date")
    ) AS ollc_foc_date,
    (
        SELECT GROUP_CONCAT(
                CONCAT_WS(
                    " - ", PAR.parameter_name, PAR.parameter_value
                ) SEPARATOR "; "
            )
        FROM
            o2puat.RestInterface_npp NPP
            JOIN RestInterface_parameter PAR ON PAR.npp_id = NPP.id
        WHERE
            PRJTRK.npp_id = NPP.id
            AND PRJTRK.type_of_product = "OLLC"
            AND PAR.parameter_name IN ("LLC_Partner_Ref")
    ) AS lsp_circuit_id,
    (
        SELECT GROUP_CONCAT(
                CONCAT_WS(
                    " - ", PRJTRK.order_code, ORD.current_crd
                ) SEPARATOR "; "
            )
        FROM o2puat.RestInterface_order ORD
        WHERE
            PRJTRK.order_id = ORD.id
            AND PRJTRK.type_of_product = "OLLC"
    ) AS ollc_com_act_date,
    (
        SELECT GROUP_CONCAT(
                CONCAT_WS(
                    " - ", PAR.parameter_name, PAR.parameter_value
                ) SEPARATOR "; "
            )
        FROM
            o2puat.RestInterface_npp NPP
            JOIN RestInterface_parameter PAR ON PAR.npp_id = NPP.id
        WHERE
            PRJTRK.npp_id = NPP.id
            AND (
                (
                    PRJTRK.type_of_product IN (
                        "CPlusIP", "Eline", "SingNet", "STiX"
                    )
                    AND LOWER(PAR.parameter_name) IN ("speed")
                )
                OR (
                    PRJTRK.type_of_product IN (
                        "MegaPOP", "MegaPOP_OR_Ethernet"
                    )
                    AND PAR.parameter_name IN ("SpeedD", "Speedbps", "Speed")
                )
                OR (
                    PRJTRK.type_of_product IN ("MetroE")
                    AND PAR.parameter_name IN ("speed", "_speed")
                )
                OR (
                    PRJTRK.type_of_product IN ("CGI")
                    AND PAR.parameter_name IN (
                        "speed", "LocIntSpeedDownstr", "LocIntSpeedUpstr"
                    )
                )
                OR (
                    PRJTRK.type_of_product NOT IN(
                        "CPlusIP", "Eline", "SingNet", "STiX", "MegaPOP", "MegaPOP_OR_Ethernet", "MetroE", "CGI"
                    )
                    AND PAR.parameter_name IN (
                        "speed", "SpeedD" "Speedbps", "Speed", "_speed", "LocIntSpeedDownstr", "LocIntSpeedUpstr"
                    )
                )
            )
    ) AS port_bandwidth,
    (
        SELECT GROUP_CONCAT(
                CONCAT_WS(
                    " - ", PAR.parameter_name, PAR.parameter_value
                ) SEPARATOR "; "
            )
        FROM
            o2puat.RestInterface_npp NPP
            JOIN RestInterface_parameter PAR ON PAR.npp_id = NPP.id
        WHERE
            PRJTRK.npp_id = NPP.id
            AND (
                (
                    PRJTRK.type_of_product IN (
                        "CPlusIP", "MegaPOP", "MetroE"
                    )
                    AND PAR.parameter_name IN ("MTUSize")
                )
                OR (
                    PRJTRK.type_of_product NOT IN(
                        "CPlusIP", "MegaPOP", "MegaPOP_OR_Ethernet", "MetroE"
                    )
                    AND PAR.parameter_name IN ("MTUSize")
                )
            )
    ) AS port_mtu_size,
    (
        SELECT GROUP_CONCAT(
                CONCAT_WS(
                    " - ", PAR.parameter_name, PAR.parameter_value
                ) SEPARATOR "; "
            )
        FROM
            o2puat.RestInterface_npp NPP
            JOIN RestInterface_parameter PAR ON PAR.npp_id = NPP.id
        WHERE
            PRJTRK.npp_id = NPP.id
            AND (
                (
                    PRJTRK.type_of_product IN (
                        "MegaPOP", "MegaPOP_OR_Ethernet"
                    )
                    AND PAR.parameter_name IN ("VPNNo")
                )
                OR (
                    PRJTRK.type_of_product IN ("CPlusIP")
                    AND PAR.parameter_name IN ("VPNGrp")
                )
                OR (
                    PRJTRK.type_of_product NOT IN(
                        "MegaPOP", "MegaPOP_OR_Ethernet", "CPlusIP"
                    )
                    AND PAR.parameter_name IN ("VPNNo", "VPNGrp")
                )
            )
    ) AS vpn_number,
    (
        SELECT GROUP_CONCAT(
                CONCAT_WS(
                    " - ", PAR.parameter_name, PAR.parameter_value
                ) SEPARATOR "; "
            )
        FROM
            o2puat.RestInterface_npp NPP
            JOIN RestInterface_parameter PAR ON PAR.npp_id = NPP.id
        WHERE
            PRJTRK.npp_id = NPP.id
            AND (
                (
                    PRJTRK.type_of_product IN ("CPlusIP")
                    AND PAR.parameter_name IN ("CEasNumber")
                )
                OR (
                    PRJTRK.type_of_product IN (
                        "MegaPOP", "MegaPOP_OR_Ethernet"
                    )
                    AND PAR.parameter_name IN ("BGP", "BgpAs", "BGPASNum")
                )
                OR (
                    PRJTRK.type_of_product IN ("STiX")
                    AND PAR.parameter_name IN ("ASNumber")
                )
                OR (
                    PRJTRK.type_of_product NOT IN("CPlusIP", "MegaPOP", "STiX")
                    AND PAR.parameter_name IN (
                        "CEasNumber", "BGP", "BgpAs", "BGPASNum", "ASNumber"
                    )
                )
            )
    ) AS bgp_number,
    (
        SELECT GROUP_CONCAT(
                CONCAT_WS(
                    " - ", PAR.parameter_name, PAR.parameter_value
                ) SEPARATOR "; "
            )
        FROM
            o2puat.RestInterface_npp NPP
            JOIN RestInterface_parameter PAR ON PAR.npp_id = NPP.id
        WHERE
            PRJTRK.npp_id = NPP.id
            AND (
                (
                    PRJTRK.type_of_product IN (
                        "MegaPOP", "MegaPOP_OR_Ethernet"
                    )
                    AND PAR.parameter_name IN ("IPAddWan", "WIPAdd")
                )
                OR (
                    PRJTRK.type_of_product IN ("CPlusIP")
                    AND PAR.parameter_name IN ("IPAddWan1", "IPAddWan2")
                )
                OR (
                    PRJTRK.type_of_product NOT IN(
                        "MegaPOP", "MegaPOP_OR_Ethernet", "CPlusIP"
                    )
                    AND PAR.parameter_name IN (
                        "IPAddWan", "WIPAdd", "IPAddWan1", "IPAddWan2"
                    )
                )
            )
    ) AS wan_ip,
    (
        SELECT GROUP_CONCAT(
                CONCAT_WS(
                    " - ", PAR.parameter_name, PAR.parameter_value
                ) SEPARATOR "; "
            )
        FROM
            o2puat.RestInterface_npp NPP
            JOIN RestInterface_parameter PAR ON PAR.npp_id = NPP.id
        WHERE
            PRJTRK.npp_id = NPP.id
            AND (
                (
                    PRJTRK.type_of_product IN (
                        "MegaPOP", "MegaPOP_OR_Ethernet"
                    )
                    AND PAR.parameter_name IN ("IPAddLan", "WIPAdd")
                )
                OR (
                    PRJTRK.type_of_product IN ("CPlusIP")
                    AND PAR.parameter_name IN ("LANIPAddr")
                )
                OR (
                    PRJTRK.type_of_product NOT IN(
                        "MegaPOP", "MegaPOP_OR_Ethernet", "CPlusIP"
                    )
                    AND PAR.parameter_name IN (
                        "IPAddLan", "WIPAdd", "LANIPAddr"
                    )
                )
            )
    ) AS lan_ip,
    (
        SELECT GROUP_CONCAT(
                CONCAT_WS(
                    " - ", PAR.parameter_name, PAR.parameter_value
                ) SEPARATOR "; "
            )
        FROM
            o2puat.RestInterface_npp NPP
            JOIN RestInterface_parameter PAR ON PAR.npp_id = NPP.id
        WHERE
            PRJTRK.npp_id = NPP.id
            AND (
                (
                    PRJTRK.type_of_product IN ("RMS")
                    AND PAR.parameter_name IN ("Loopback0")
                )
                OR (
                    PRJTRK.type_of_product NOT IN("RMS")
                    AND PAR.parameter_name IN ("Loopback0")
                )
            )
    ) AS rms_loopback_ip,
    so_date_for_cpe,
    (
        SELECT GROUP_CONCAT(
                CONCAT_WS(
                    " - ", PAR.parameter_name, PAR.parameter_value
                ) SEPARATOR "; "
            )
        FROM
            o2puat.RestInterface_npp NPP
            JOIN RestInterface_parameter PAR ON PAR.npp_id = NPP.id
        WHERE
            PRJTRK.npp_id = NPP.id
            AND (
                (
                    PRJTRK.type_of_product IN ("CPE")
                    AND LOWER(PAR.parameter_name) IN ("brand", "model")
                )
                OR (
                    PRJTRK.type_of_product IN ("SDWAN")
                    AND PAR.parameter_name IN ("BrACPETy", "SvcScheme")
                )
            )
    ) AS hardware_model,
    (
        SELECT GROUP_CONCAT(
                CONCAT_WS(
                    " - ", PAR.parameter_name, PAR.parameter_value
                ) SEPARATOR "; "
            )
        FROM
            o2puat.RestInterface_npp NPP
            JOIN RestInterface_parameter PAR ON PAR.npp_id = NPP.id
        WHERE
            PRJTRK.npp_id = NPP.id
            AND (
                (
                    PRJTRK.type_of_product IN ("CPE")
                    AND LOWER(PAR.parameter_name) IN ("HardIntegName")
                )
            )
    ) AS hardware_vendor,
    (
        SELECT GROUP_CONCAT(
                CONCAT_WS(
                    " - ", PAR.parameter_name, PAR.parameter_value
                ) SEPARATOR "; "
            )
        FROM
            o2puat.RestInterface_npp NPP
            JOIN RestInterface_parameter PAR ON PAR.npp_id = NPP.id
        WHERE
            PRJTRK.npp_id = NPP.id
            AND (
                (
                    PRJTRK.type_of_product IN ("CPE")
                    AND LOWER(PAR.parameter_name) IN ("IntegratorName")
                )
                OR (
                    PRJTRK.type_of_product IN ("SDWAN")
                    AND PAR.parameter_name IN ("ConfigSpt")
                )
            )
    ) AS installation_vendor,
    (
        SELECT GROUP_CONCAT(
                CONCAT_WS(
                    " - ", PAR.parameter_name, PAR.parameter_value
                ) SEPARATOR "; "
            )
        FROM
            o2puat.RestInterface_npp NPP
            JOIN RestInterface_parameter PAR ON PAR.npp_id = NPP.id
        WHERE
            PRJTRK.npp_id = NPP.id
            AND (
                (
                    PRJTRK.type_of_product IN ("CPE")
                    AND LOWER(PAR.parameter_name) IN ("InstlnHours")
                )
            )
    ) AS installation_hours
FROM
    o2puat.RestInterface_order ORD
    JOIN o2ptest.project_tracker_group PRJTRK ON PRJTRK.order_id = ORD.id
    LEFT JOIN RestInterface_site SITE ON SITE.id = ORD.site_id
    LEFT JOIN (
        SELECT
            INNER_CON.order_id, MAX(
                CASE
                    WHEN INNER_CON.contact_type = "Clarification-Cust" THEN INNER_CON.email_address
                END
            ) clarification_cust, MAX(
                CASE
                    WHEN INNER_CON.contact_type = "A-end-Cust" THEN INNER_CON.email_address
                END
            ) a_end_cust, MAX(
                CASE
                    WHEN INNER_CON.contact_type = "Technical-Cust" THEN INNER_CON.email_address
                END
            ) technical_cust
        FROM o2puat.RestInterface_contactdetails INNER_CON
            JOIN o2ptest.project_tracker_group INNER_PRJTRK ON INNER_PRJTRK.order_id = INNER_CON.order_id
        GROUP BY
            INNER_CON.order_id
    ) CON ON CON.order_id = PRJTRK.order_id
    LEFT JOIN (
        SELECT INNER_PAR.npp_id, MAX(
                CASE
                    WHEN LOWER(INNER_PAR.parameter_name) = "speed" THEN CASE
                        WHEN INNER_PRJTRK.type_of_product IN (
                            "Elite", "MetroE", "Gigawave", "MegaPOP_OR_Ethernet", "SingNet_OR_Ethernet"
                        ) THEN CONCAT(
                            INNER_PAR.parameter_name, " - ", INNER_PAR.parameter_value
                        )
                        ELSE INNER_PAR.parameter_value
                    END
                END
            ) AS sg_dom_bandwidth
        FROM o2puat.RestInterface_parameter INNER_PAR
            JOIN o2ptest.project_tracker_group INNER_PRJTRK ON INNER_PRJTRK.npp_id = INNER_PAR.npp_id
        GROUP BY
            INNER_PAR.npp_id
    ) PAR ON PAR.npp_id = PRJTRK.npp_id
    LEFT JOIN (
        SELECT
            INNER_ACT.order_id, MAX(
                CASE
                    WHEN INNER_ACT.name IN (
                        "Site Survey", "Site Survey - A End"
                    ) THEN CONCAT(
                        INNER_ACT.name, ' - ', INNER_ACT.status, ' - ', INNER_ACT.completed_date
                    )
                END
            ) circuit_survey_a_end, MAX(
                CASE
                    WHEN INNER_ACT.name IN ("Site Survey - B End") THEN CONCAT(
                        INNER_ACT.name, ' - ', INNER_ACT.status, ' - ', INNER_ACT.completed_date
                    )
                END
            ) circuit_survey_b_end, MAX(
                CASE
                    WHEN INNER_ACT.name IN (
                        "Fibre Implementation - A", "Fibre Implementation GW-A"
                    ) THEN CONCAT(
                        INNER_ACT.name, ' - ', INNER_ACT.status, ' - ', INNER_ACT.completed_date
                    )
                END
            ) inhouse_cabling_a_end, MAX(
                CASE
                    WHEN INNER_ACT.name IN (
                        " Fibre Implementation - B", "Fibre Implementation GW-B"
                    ) THEN CONCAT(
                        INNER_ACT.name, ' - ', INNER_ACT.status, ' - ', INNER_ACT.completed_date
                    )
                END
            ) inhouse_cabling_b_end, MAX(
                CASE
                    WHEN INNER_PRJTRK.type_of_product = "MetroE"
                    AND INNER_ACT.name = "CPE Installation - A End" THEN CONCAT(
                        INNER_ACT.name, ' - ', INNER_ACT.status, ' - ', INNER_ACT.completed_date
                    )
                    WHEN INNER_PRJTRK.type_of_product = "Gigawave"
                    AND INNER_ACT.name IN (
                        "Install OLT/MUX", "Install OLT/MUX Eqpt (A)"
                    ) THEN CONCAT(
                        INNER_ACT.name, ' - ', INNER_ACT.status, ' - ', INNER_ACT.completed_date
                    )
                    WHEN INNER_PRJTRK.type_of_product = "Elite"
                    AND INNER_ACT.name = "CPE Instln & Testing" THEN CONCAT(
                        INNER_ACT.name, ' - ', INNER_ACT.status, ' - ', INNER_ACT.completed_date
                    )
                END
            ) mux_installation_a_end, MAX(
                CASE
                    WHEN INNER_PRJTRK.type_of_product = "MetroE"
                    AND INNER_ACT.name = "CPE Installation - B End" THEN CONCAT(
                        INNER_ACT.name, ' - ', INNER_ACT.status, ' - ', INNER_ACT.completed_date
                    )
                    WHEN INNER_PRJTRK.type_of_product = "Gigawave"
                    AND INNER_ACT.name = "Install OLT/MUX Eqpt (B)" THEN CONCAT(
                        INNER_ACT.name, ' - ', INNER_ACT.status, ' - ', INNER_ACT.completed_date
                    )
                END
            ) mux_installation_b_end, MAX(
                CASE
                    WHEN INNER_ACT.name IN ("Raise Impact Vendor Order") THEN CONCAT(
                        INNER_ACT.name, ' - ', INNER_ACT.status, ' - ', INNER_ACT.completed_date
                    )
                END
            ) so_date_for_cpe
        FROM o2puat.RestInterface_activity INNER_ACT
            JOIN o2ptest.project_tracker_group INNER_PRJTRK ON INNER_PRJTRK.order_id = INNER_ACT.order_id
        GROUP BY
            INNER_ACT.order_id
    ) ACT ON ACT.order_id = PRJTRK.order_id;
SELECT
    DISTINCT ORD.order_code,
    ORD.service_number,
    PRD.network_product_code,
    PRD.network_product_desc,
    CUS.name AS customer_name,
    ORD.order_type,
    ORD.current_crd,
    ORD.taken_date,
    PER.role AS group_id,
    CAST(ACT.activity_code AS SIGNED INTEGER) AS act_step_no,
    ACT.name AS act_name,
    ACT.due_date AS act_due_date,
    ACT.ready_date AS act_rdy_date,
    DATE(ACT.exe_date) AS act_exe_date,
    DATE(ACT.dly_date) AS act_dly_date,
    ACT.completed_date AS act_com_date
FROM
    RestInterface_order ORD
    JOIN RestInterface_activity ACT ON ACT.order_id = ORD.id
    JOIN RestInterface_person PER ON PER.id = ACT.person_id
    LEFT JOIN RestInterface_customer CUS ON CUS.id = ORD.customer_id
    LEFT JOIN RestInterface_npp NPP ON ORD.id = NPP.order_id
    AND NPP.level = 'MainLine'
    AND NPP.status != 'Cancel'
    LEFT JOIN RestInterface_product PRD ON NPP.product_id = PRD.id
WHERE
    ORD.id IN (
        SELECT
            DISTINCT ORD.id
        FROM
            RestInterface_order ORD
            JOIN RestInterface_activity ACT ON ACT.order_id = ORD.id
            JOIN RestInterface_person PER ON PER.id = ACT.person_id
        WHERE
            PER.role LIKE 'CNP%'
            AND ACT.completed_date BETWEEN '2023-07-01'
            AND '2023-07-31'
            AND ACT.name IN (
                'Change C+ IP',
                'De-Activate C+ IP',
                'DeActivate Video Exch Svc',
                'LLC Received from Partner',
                'LLC Accepted by Singtel',
                'Activate C+ IP',
                'Cease Resale SGO',
                'OLLC Site Survey',
                'De-Activate TOPSAA on PE',
                'De-Activate RAS',
                'De-Activate RLAN on PE',
                'Pre-Configuration on PE',
                'De-Activate RMS on PE',
                'GSDT Co-ordination Work',
                'Change Resale SGO',
                'Pre-Configuration',
                'Cease MSS VPN',
                'Recovery - PNOC Work',
                'De-Activate RMS for IP/EV',
                'GSDT Co-ordination OS LLC',
                'Change RAS',
                'Extranet Config',
                'Cease Resale SGO JP',
                'm2m EVC Provisioning',
                'Activate RMS/TOPS IP/EV',
                'Config MSS VPN',
                'De-Activate RMS on CE-BQ',
                'OLLC Order Ack',
                'Cease Resale SGO CHN'
            )
    )
    AND (
        (
            PER.role LIKE 'CNP%'
            AND ACT.completed_date BETWEEN '2023-07-01'
            AND '2023-07-31'
            AND ACT.name IN (
                'Change C+ IP',
                'De-Activate C+ IP',
                'DeActivate Video Exch Svc',
                'LLC Received from Partner',
                'LLC Accepted by Singtel',
                'Activate C+ IP',
                'Cease Resale SGO',
                'OLLC Site Survey',
                'De-Activate TOPSAA on PE',
                'De-Activate RAS',
                'De-Activate RLAN on PE',
                'Pre-Configuration on PE',
                'De-Activate RMS on PE',
                'GSDT Co-ordination Work',
                'Change Resale SGO',
                'Pre-Configuration',
                'Cease MSS VPN',
                'Recovery - PNOC Work',
                'De-Activate RMS for IP/EV',
                'GSDT Co-ordination OS LLC',
                'Change RAS',
                'Extranet Config',
                'Cease Resale SGO JP',
                'm2m EVC Provisioning',
                'Activate RMS/TOPS IP/EV',
                'Config MSS VPN',
                'De-Activate RMS on CE-BQ',
                'OLLC Order Ack',
                'Cease Resale SGO CHN'
            )
        )
        OR (
            PER.role LIKE 'GSDT6%'
            AND ACT.name IN (
                'GSDT Co-ordination Work',
                'De-Activate C+ IP',
                'Cease Monitoring of IPPBX',
                'GSDT Co-ordination OS LLC',
                'GSDT Partner Cloud Access',
                'Cease In-Ctry Data Pro',
                'Change Resale SGO',
                'Ch/Modify In-Country Data',
                'De-Activate RMS on PE',
                'Disconnect RMS for FR',
                'Change C+ IP',
                'Activate C+ IP',
                'LLC Accepted by Singtel',
                'GSDT Co-ordination - BQ',
                'LLC Received from Partner',
                'In-Country Data Product',
                'OLLC Site Survey',
                'GSDT Co-ordination-RMS',
                'Pre-Configuration on PE',
                'Cease Resale SGO',
                'Disconnect RMS for ATM'
            )
        )
    )
ORDER BY
    ORD.order_code,
    act_step_no;
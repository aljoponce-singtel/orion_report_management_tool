# GSP Reports

# Overview

Add this crontab command for report scheduling:

    15 9 * * * /app/o2p/ossadmin/orion_report_management_tool/manage.sh gsp main main

This script will generate the following reports:

- [CPlusIP](#cplusip)
- [MegaPOP](#megapop)
- [Singnet](#singnet)
- [STIX](#stix)
- [Internet](#internet)
- [SDWAN](#sdwan)

## CPlusIP

### CNP

Activities:

- Change C+ IP
- De-Activate C+ IP
- DeActivate Video Exch Svc
- LLC Received from Partner
- LLC Accepted by Singtel
- Activate C+ IP
- Cease Resale SGO
- OLLC Site Survey
- De-Activate TOPSAA on PE
- De-Activate RAS
- De-Activate RLAN on PE
- Pre-Configuration on PE
- De-Activate RMS on PE
- GSDT Co-ordination Work
- Change Resale SGO
- Pre-Configuration
- Cease MSS VPN
- Recovery - PNOC Work
- De-Activate RMS for IP/EV
- GSDT Co-ordination OS LLC
- Change RAS
- Extranet Config
- Cease Resale SGO JP
- m2m EVC Provisioning
- Activate RMS/TOPS IP/EV
- Config MSS VPN
- De-Activate RMS on CE-BQ
- OLLC Order Ack
- Cease Resale SGO CHN

### GSDT6

Activities:

- GSDT Co-ordination Work
- De-Activate C+ IP
- Cease Monitoring of IPPBX
- GSDT Co-ordination OS LLC
- GSDT Partner Cloud Access
- Cease In-Ctry Data Pro
- Change Resale SGO
- Ch/Modify In-Country Data
- De-Activate RMS on PE
- Disconnect RMS for FR
- Change C+ IP
- Activate C+ IP
- LLC Accepted by Singtel
- GSDT Co-ordination - BQ
- LLC Received from Partner
- In-Country Data Product
- OLLC Site Survey
- GSDT Co-ordination-RMS
- Pre-Configuration on PE
- Cease Resale SGO
- Disconnect RMS for ATM

## MegaPOP

### MPP

Activities:

- Activate E-Access
- Activate EVPL
- Activate RMS/TOPS - MP
- Cease IaaS Connectivity
- Cessation of PE Port
- Cessation of UTM
- Change C+ IP
- Circuit Configuration
- Config PE Port
- Config UTM
- De-Activate E-Access
- De-Activate RMS on PE
- End to End PNOC Test
- Extranet Config
- GSDT Co-ordination Work
- m2m EVC Provisioning
- mLink EVC Provisioning
- mLink EVC Termination
- Pre-Configuration on PE
- Re-config E-Access
- Reconfiguration
- Recovery - PNOC Work
- SD-WAN Config
- SD-WAN Svc Provisioning

### GSDT8

Activities:

- Cease IaaS Connectivity
- Connection of RMS for MP
- GSDT Co-ordination Wk-BQ
- GSDT Co-ordination Work
- GSDT Co-ordination Wrk-BQ

## Singnet

### SGX1

Activities:

- Activate E-Access
- Cease SG Cct @PubNet
- Cease SingNet Svc
- Circuit Configuration
- Comn SgNet PubNet Wk
- Comn SingNet PubNet Wk
- De-Activate E-Access
- DNS Set-Up
- GSDT Co-ordination Work
- IP Verification - MegaPOP
- Modify Microsoft Direct
- Modify SingNet Svc
- Provision SingNet Svc
- Reconfiguration
- Recovery - PNOC Work
- SN Evolve Static IP Work

### GSDT7

Activities:

- Circuit Configuration
- GSDT Coordination
- GSDT Co-ordination WK-BQ
- GSDT Co-ordination Work
- GSDT Co-ordination Wrk-BQ

## STIX

### GSDT9

Activities:

- GSDT Co-ordination OS LLC
- GSDT Co-ordination Work

## Internet

### GSDT_PS21 & GSDT_PS23

Activities:

- GSDT Co-ordination Work
- GSDT GI Coordination Work

## SDWAN

### GSP_SDN_TM & GSDT_TM

Activities:

- GSDT Co-ordination Wk-BQ
- GSDT Co-ordination Wrk-BQ
- GSP-TM Coordination Work

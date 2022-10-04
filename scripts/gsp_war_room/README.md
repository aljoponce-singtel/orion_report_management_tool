From: Christian Lim 
Sent: Wednesday, 14 September 2022 2:23 pm
To: Jiang Xu Jiang <jiangxu.jiang@singtel.com>; Aljo Ponce <aljo.ponce@singtel.com>
Cc: Andrew Teo Kok Wee <teokokwee@singtel.com>
Subject: RE: new report requirement

Hi Jiang Xu 

For the war room report , we need all under GD in the report, because we are also tracking them

For Bill report, only order that related to GSP 

Thanks

Chris 
From: Jiang Xu Jiang <jiangxu.jiang@singtel.com> 
Sent: Wednesday, 14 September 2022 2:05 pm
To: Aljo Ponce <aljo.ponce@singtel.com>; Christian Lim <christian.lim@singtel.com>
Cc: Andrew Teo Kok Wee <teokokwee@singtel.com>
Subject: RE: new report requirement

Hi, Christian,
You are requesting all the queue within GD will be in the report. It means not only GSSP (excluding CSS) department? 

Regards
Jiang Xu




From: Christian Lim <christian.lim@singtel.com> 
Sent: Wednesday, 14 September 2022 10:07 am
To: Aljo Ponce <aljo.ponce@singtel.com>; Andrew Teo Kok Wee <teokokwee@singtel.com>
Cc: Jiang Xu Jiang <jiangxu.jiang@singtel.com>
Subject: RE: new report requirement

Hi Aljo 

Here is the summary of the discussion 

For WAR room report , we need additional field here , please extract one sample with new Department and Grouping ID , we need to ensure all Q within GD are inside , thanks 

| Department           | group_ID  | CircuitTie | ED/PD Diversity | PM/Non PM | Exchange Code | BRN | Auto escalation ? | Product Code | contained all Q , all within GD |
| -------------------- | --------- | ---------- | --------------- | --------- | ------------- | --- | ----------------- | ------------ | ------------------------------- |
| FNE_Katong Exchange  | HSDT1     | 3508802    | NA              |           |               |     | reason            |              |                                 |


For Bill report we need and please extract the filter code, to ensure we are align on the all orders that managed by GSP only

| BRN | Product Code | PM/Non PM | CRD amendment reason and category , if this is possible | We may need to do this in monthly too , thanks |
| --- | ------------ | --------- | ------------------------------------------------------- | ---------------------------------------------- |
|     |              |           |                                                         |                                                |




New columns:

gsp_warroom_report

PM/Non-PM = Yes/No
A-End Exchange Code / Exchange Code A
B-End Exchange Code / Exchange Code B - leave blank if no data
BRN
Auto-escalation delay reason

Department - switch to new auto-escalation matrix
Product Code - put beside product description
Product Description - using order table, should use product table

contains all queue, all within GD


Bill Report:

gsp_report_toBill - report name

New columns:

BRN
Product Code
PM/Non-PM

CRD amendment reason and category

report frequency - weekly, monthly (TBD)


All the orders are under GSP - Jiang Xu 



mysql> SELECT DISTINCT categoty FROM RestInterface_ordersinote;
+--------------------------+
| categoty                 |
+--------------------------+
| Work Order               |
| Service Instance Notes   |
| OM                       |
| eTracker Internal Notes  |
| CRD                      |
|                          |
| Non eRequest             |
| eRequest                 |
| EAGLE RP                 |
| Line ID                  |
| Parallel Upgrade         |
| Parallel ER              |
| eTracker External Notes  |
| Exchange Notes           |
| Linked WO                |
| Waiver Notes             |
| Parallel Downgrade       |
| Service Instance Contact |
| Payment                  |
| CPR                      |
| ONL                      |
| Mobile CI/CP Correction  |
| ICC Contact              |
| Transfer                 |
| Charge Override Notes    |
| Adjustment               |
+--------------------------+
26 rows in set (35.78 sec)


mysql> SELECT DISTINCT sub_categoty FROM RestInterface_ordersinote;
+---------------------------------------------------------------+
| sub_categoty                                                  |
+---------------------------------------------------------------+
| Sales Note (enter by OP taker)                                |
|                                                               |
| Others                                                        |
| Amendment                                                     |
| Installation Notes                                            |
| Installation Remarks                                          |
| CRD Change History                                            |
| Initial CRD                                                   |
| Pair Allocation Notes                                         |
| 07)Customised Pdts e.g. speed, contract, 16IP not in eRequest |
| 15)AM used manual Quote/SRCA                                  |
| 03)Promo & Roadshow for In-Scope Pdt                          |
| Awaiting Q Processing via Batch                               |
| 14)Projects (more than 10 circuits)                           |
| 02)Pdts Out-of-Scope e.g CPE, ILC, MS, Expan etc              |
| 12)Bid/Tender                                                 |
| eRequest                                                      |
| 13)MSA Request for In-Scope Pdts                              |
| 08)Business Scenario not supported in eRequest                |
| 11)Process Issue for In-Scope Ptds                            |
| Link order-Parallel Upgrade                                   |
| DeLink-Parallel Upgrade                                       |
| 01)Sales Sectors Out-of-Scope e.g. SGO, 1606, SN Registrar    |
| Number Mgmt Unit                                              |
| Port-in Details (Number Port)                                 |
| 04)In-scope pdts combined with Out-of-Scope pdts              |
| Link order-Parallel ER                                        |
| 10)System Issue for In-Scope Pdts                             |
| 06)Backdate Order for In-Scope Pdts                           |
| 5 circuits                                                    |
| 09)Parallel External Relocation (Retain existing MRC, Term)   |
| Account Transfer - Clone Account                              |
| Network Allocation Notes                                      |
| Singtel Committed Date                                        |
| 05)Customer does not have Internet                            |
| Admin Charge Waiver                                           |
| DeLink-Parallel ER                                            |
| Miscellaneous Notes                                           |
| MoIP Info                                                     |
| Parental Control , Internet filter VAS                        |
| Link order-Parallel Downgrade                                 |
| Equipment Purchase                                            |
| Account Transfer - Maintain Hierarchy                         |
| Singtel Internal Remarks                                      |
| DDI Cessation Notes                                           |
| Bill Processing Attributes - Change in Value                  |
| CPR SMS                                                       |
| SingNet Unicron SE                                            |
| Survey Plan Notes                                             |
| Mobile Downgrade Penalty                                      |
| Transition from Corporate to Consumer                         |
| Credit Term - Change in Value                                 |
| Cross Carriage                                                |
| Cancellation of same-day currently effective payment mode     |
| DeLink-Parallel Downgrade                                     |
| Split customer for BCC                                        |
| F1 Media                                                      |
| Low Value Bill Threshold - Change in Value                    |
| Do Not Call Registry                                          |
| Deceased                                                      |
| EMS elevate                                                   |
| SPEAR                                                         |
| Delivery Portal                                               |
| Loss of control over payments                                 |
+---------------------------------------------------------------+
64 rows in set (36.53 sec)

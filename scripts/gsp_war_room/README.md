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
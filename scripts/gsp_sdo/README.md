updated on 8 Sep 21
====================


Fields in the report

Megapop & Singnet

Start collating the provide orders from the previous GSDT8xx & GSDT7xx report that Aljo has done

Present the records as follows:

1. Workorder no
2. Service no - for Singnet specifically, those with prefix "ELITE", "ETHCE", "ETHM", "GWL"
3. Network Product Code (NPC) - Elite, CE (based on the Product code)

Below are the Singnet NPC (GSDT7)
	SGN0051	SingNet Ethernet Access  - Elite/CE
	SGN0119	SingNet MetroEthernet Access - MetroE
	SGN0157	Ethernetlink Autobackup for Ethernetlink (Fiber) - Elite/CE
	SGN0160	SingNet GigawaveLite Access Unlimited - Elite/CE - Gigawave
	SGN0170	SingNet eLite Access				- search instance - Elite/CE
	SGN0340	SingNet MetroEthernet Access (Path Diversity) - MetroE
	SGN2004	SingNet eLite Access CIR/PIR			- search instance - Elite/CE
	
Below are the Megapop NPC (GSDT8)
	ELK0052	(CE) Ethernetlink - Elite/CE
	ELK0053	(CE) Ethernetlink Exch Div - Elite/CE
	ELK0055	(CE) Ethernetlink AutoBackup w Exch Div - Elite/CE
	ELK0089	(CE) Meg@POP IPVPN Ethernetlink with Path Diversity - Elite/CE
	
	GEL0001	eLite Access					- search instance - Elite/CE
	GEL0018	eLite for SIP Voice				- search instance - IPN/SIP
	GEL0023	eLite Access (i-PhoneNet)			- search instance - IPN
	GEL0036	(Meg@POP2)eLite Access with Exch Diversity 	- search instance - Elite/CE

4. CRD
5. Customer name
6. Type of order - only provide order
7. Group ID - only those with GSDT8xx for Megapop, and GSDT7xx for Singnetting with "ELITE", "ETHCE", "ETHM"
8. Date COM

From the above, find the service instance and find the associate orders/service no for the FTTH, MetroE, Gigawave
and add on below fields to the report


For Elite, there will be a FTTH order component.
For CE, there isn't any additional componenent.

Add on the above records the following fields

1. Workorder no
2. Service no
3. Network Product Code (NPC)
4. CRD
5. Customer name
6. Order Taken Date
7. Order Type - provide only
8. Site survey (SS)
	a. Activity name
		- (Elite/SIP) Site Survey - A-end
		- (IPN)   Site Survey
		- (IPN) Check & Plan Fiber - SME   >>>> added on 8 Sep 21
		- (GW) 	  Check HSD Resources
		- (CE) Site Survey - A-end
		- (IPN/SIP) Check & Plan Fiber – ON >>>> added on 10 Sep 21
	b. Group ID - it is HSDE1
	c. Due date or activity deadline
	d. RDY date
	e. EXC date
	f. DLY date
	g. COM date
9. Routing info (RI)
	a. Activity name
		- (Elite/SIP/IPN) Cct Allocate-ETE Routing     >>>>> Added IPN on 8 Sep 21(Elite/SIP/IPN)
		- (CE) Circuit Allocation
		- (GW) TNP/HSD Activities
		- (IPN) Cct Allocate ETE Rtg – ON >>>>> added on 10 Sep 21
	b. Group ID -
	c. Due date or activity deadline
	d. RDY date
	e. EXC date
	f. DLY date
	g. COM date
10. Testing and Installation (TI)
	a. Activity name
		- (Elite) CPE Instln & Testing
		- (IPN)   DWFM Installation work
		- (GW) E-To-E Test (PNOC)
		- (CE) End-To-End Test - A End >>>> newly added on 26 Ag 21
		- (IPN) CPE Instln & Testing >>>> newly added on 26 Ag 21
		- (SIP) CPE Instln & Testing >>>> newly added on 26 Ag 21
	b. Group ID
	c. Due date or activity deadline
	d. RDY date 
	e. EXC date
	f. DLY date
	g. COM date
9. Project Manager email

ADD NEW WORKORDER CRD next to OrderCodeNew Column

- if there are multiple same activity name but different stepno, select the highest stepno
- if there are multiple FTTH with 1 provide and 1 or more non-provide order, select only the provide order
- check for multiple project managers
- for MP-SS, choose "Site Survey" if paired with "Check & Plan Fiber  - SME"
- for MP-SS, choose "Site Survey" if paired with "Check & Plan Fiber  - ON"
- if there are multiple FTTH provide orders, do not select an order where all queues are cancelled (check each matched queue's if CAN)
- if there are multiple order in the same service number, choose the order with the latest CRD
from scripts import utils
import logging
import re
import os
from scripts.DBConnection import DBConnection
from scripts.EmailClient import EmailClient
import pandas as pd

logger = logging.getLogger(__name__)
defaultConfig = None
emailConfig = None
dbConfig = None
csvFiles = []
reportsFolderPath = None
orionDb = None
tableauDb = None

headers = [
    "Workorder no",
    "Service No",
    "Product Code",
    "Product Description",
    "Customer Name",
    "Order Type",
    "CRD",
    "Order Taken Date",
    "Group ID",
    "Activity Name",
    "DUE",
    "RDY",
    "EXC",
    "DLY",
    "COM",
    "Group ID",
    "Activity Name",
    "DUE",
    "RDY",
    "EXC",
    "DLY",
    "COM"
]

headers2 = [
    "Workorder no",
    "Service No",
    "Product Code",
    "Product Description",
    "Customer Name",
    "Order Type",
    "CRD",
    "Order Taken Date",
    "Group ID",
    "Activity Name",
    "DUE",
    "RDY",
    "EXC",
    "DLY",
    "COM",
]


def initialize(config):
    global defaultConfig, emailConfig, dbConfig, reportsFolderPath, orionDb, tableauDb
    defaultConfig = config['DEFAULT']
    emailConfig = config[defaultConfig['EmailInfo']]
    dbConfig = config[defaultConfig['DatabaseEnv']]
    reportsFolderPath = os.path.join(
        os.getcwd(), defaultConfig['ReportsFolder'])

    orionDb = DBConnection(dbConfig['dbapi'], dbConfig['host'], dbConfig['port'],
                           dbConfig['orion_db'], dbConfig['orion_user'], dbConfig['orion_pwd'])
    orionDb.connect()

    tableauDb = DBConnection(dbConfig['host'], dbConfig['port'],
                             dbConfig['tableau_db'], dbConfig['tableau_user'], dbConfig['tableau_pwd'])
    tableauDb.connect()


def updateTableauDB(outputList, report_id):
    # Allow Tableaue DB update
    if defaultConfig.getboolean('UpdateTableauDB'):
        try:
            logger.info(
                'Inserting records to ' + dbConfig['tableau_db'] + '.' + defaultConfig['TableauTable'] + ' for ' + report_id.lower() + ' ...')

            columns = [
                "Workorder_no",
                "Service_No",
                "Product_Code",
                "Product_Description",
                "Customer_Name",
                "Order_Type",
                "CRD",
                "Order_Taken_Date",
                "Group_ID_1",
                "Activity_Name_1",
                "DUE_1",
                "RDY_1",
                "EXC_1",
                "DLY_1",
                "COM_1",
                "Group_ID_2",
                "Activity_Name_2",
                "DUE_2",
                "RDY_2",
                "EXC_2",
                "DLY_2",
                "COM_2"
            ]

            df = pd.DataFrame(outputList, columns=columns)

            # add new columns
            df["report_id"] = report_id.lower()
            df["update_time"] = pd.Timestamp.now()

            # set columns to datetime type
            dateColumns = ['DUE_1', 'RDY_1', 'EXC_1', 'DLY_1', 'COM_1', 'DUE_2',
                           'RDY_2', 'EXC_2', 'DLY_2', 'COM_2', 'CRD', 'Order_Taken_Date']
            df[dateColumns] = df[dateColumns].apply(
                pd.to_datetime)

            # set empty values to null
            # insert records to DB
            df.replace('', None)
            tableauDb.insertDataframeToTable(df, defaultConfig['TableauTable'])

            # logger.info("TableauDB Updated for " + report_id.lower())

        except Exception as err:
            logger.info("Failed processing DB " + dbConfig['tableau_db'] + ' at ' +
                        dbConfig['tableau_user'] + '@' + dbConfig['host'] + ':' + dbConfig['port'] + '.')
            logger.exception(err)

            raise Exception(err)


def processList(queryList, groupId_1, groupId_2, grp_1_prio, grp_2_prio):

    finalList = []
    groupId_1_list = []
    groupId_2_list = []

    column = {
        "workOrderNo": 0,
        "serviceNo": 1,
        "productCode": 2,
        "productDescription": 3,
        "customerName": 4,
        "orderType": 5,
        "groupId": 6,
        "crd": 7,
        "orderTakenDate": 8,
        "actStepNo": 9,
        "actName": 10,
        "actDueDate": 11,
        "actRdyDate": 12,
        "actExcData": 13,
        "actDlyDate": 14,
        "actComDate": 15
    }

    records = {
        "workOrderNo": "",
        "serviceNo": "",
        "productCode": "",
        "productDescription": "",
        "customerName": "",
        "orderType": "",
        "crd": "",
        "takenDate": "",
        "groupId_1": "",
        "actName_1": "",
        "due_1": "",
        "rdy_1": "",
        "exc_1": "",
        "dly_1": "",
        "com_1": "",
        "groupId_2": "",
        "actName_2": "",
        "due_2": "",
        "rdy_2": "",
        "exc_2": "",
        "dly_2": "",
        "com_2": ""
    }

    WorkOrderNo = None
    endList = ('END', 'END')
    queryList.append(endList)

    for row in queryList:

        if row[column["workOrderNo"]
               ] != WorkOrderNo and WorkOrderNo is not None:

            if len(groupId_1_list) > 1:

                priorityFound = False
                index = 0

                for items in groupId_1_list:
                    # print(WorkOrderNo + ", " + items[0] + ", " + items[1])

                    if items[1] in grp_1_prio:
                        priorityFound = True
                        records["groupId_1"] = groupId_1_list[index][0]
                        records["actName_1"] = groupId_1_list[index][1]
                        records["due_1"] = groupId_1_list[index][2]
                        records["rdy_1"] = groupId_1_list[index][3]
                        records["exc_1"] = groupId_1_list[index][4]
                        records["dly_1"] = groupId_1_list[index][5]
                        records["com_1"] = groupId_1_list[index][6]

                    index = index + 1

                if priorityFound == False:
                    maxCount = len(groupId_1_list) - 1
                    records["groupId_1"] = groupId_1_list[maxCount][0]
                    records["actName_1"] = groupId_1_list[maxCount][1]
                    records["due_1"] = groupId_1_list[maxCount][2]
                    records["rdy_1"] = groupId_1_list[maxCount][3]
                    records["exc_1"] = groupId_1_list[maxCount][4]
                    records["dly_1"] = groupId_1_list[maxCount][5]
                    records["com_1"] = groupId_1_list[maxCount][6]

            else:
                if groupId_1_list:
                    records["groupId_1"] = groupId_1_list[0][0]
                    records["actName_1"] = groupId_1_list[0][1]
                    records["due_1"] = groupId_1_list[0][2]
                    records["rdy_1"] = groupId_1_list[0][3]
                    records["exc_1"] = groupId_1_list[0][4]
                    records["dly_1"] = groupId_1_list[0][5]
                    records["com_1"] = groupId_1_list[0][6]

            # Set RDY/EXC = COM date if RDY/EXC is empty
            if records["com_1"]:
                if not records["rdy_1"]:
                    records["rdy_1"] = groupId_1_list[0][6]
                if not records["exc_1"]:
                    records["exc_1"] = groupId_1_list[0][6]

            if len(groupId_2_list) > 1:

                priorityFound = False
                index = 0

                for items in groupId_2_list:
                    # print(WorkOrderNo + ", " + items[0] + ", " + items[1])

                    if items[1] in grp_2_prio:
                        priorityFound = True
                        records["groupId_2"] = groupId_2_list[index][0]
                        records["actName_2"] = groupId_2_list[index][1]
                        records["due_2"] = groupId_2_list[index][2]
                        records["rdy_2"] = groupId_2_list[index][3]
                        records["exc_2"] = groupId_2_list[index][4]
                        records["dly_2"] = groupId_2_list[index][5]
                        records["com_2"] = groupId_2_list[index][6]

                    index = index + 1

                if priorityFound == False:
                    maxCount = len(groupId_2_list) - 1
                    records["groupId_2"] = groupId_2_list[maxCount][0]
                    records["actName_2"] = groupId_2_list[maxCount][1]
                    records["due_2"] = groupId_2_list[maxCount][2]
                    records["rdy_2"] = groupId_2_list[maxCount][3]
                    records["exc_2"] = groupId_2_list[maxCount][4]
                    records["dly_2"] = groupId_2_list[maxCount][5]
                    records["com_2"] = groupId_2_list[maxCount][6]

            else:
                if groupId_2_list:
                    records["groupId_2"] = groupId_2_list[0][0]
                    records["actName_2"] = groupId_2_list[0][1]
                    records["due_2"] = groupId_2_list[0][2]
                    records["rdy_2"] = groupId_2_list[0][3]
                    records["exc_2"] = groupId_2_list[0][4]
                    records["dly_2"] = groupId_2_list[0][5]
                    records["com_2"] = groupId_2_list[0][6]

            # Set RDY/EXC = COM date if RDY/EXC is empty
            if records["com_2"]:
                if not records["rdy_2"]:
                    records["rdy_2"] = groupId_2_list[0][6]
                if not records["exc_2"]:
                    records["exc_2"] = groupId_2_list[0][6]

            finalList.append(tuple(records.values()))
            records = dict.fromkeys(records, "")

            groupId_1_list.clear()
            groupId_2_list.clear()

        if row[column["workOrderNo"]] != 'END':

            records["workOrderNo"] = row[column["workOrderNo"]]
            records["serviceNo"] = row[column["serviceNo"]]
            records["productCode"] = row[column["productCode"]]
            records["productDescription"] = row[column["productDescription"]]
            records["customerName"] = row[column["customerName"]]
            records["orderType"] = row[column["orderType"]]
            records["crd"] = row[column["crd"]]
            records["takenDate"] = row[column["orderTakenDate"]]

            # Checks the groupId from the current record if it matches the query's groupID
            for groupId in groupId_1:
                # checks if it matches the groupId keyword and any characters after it
                if re.search(('{}.*').format(groupId), row[column["groupId"]]):
                    groupId_1_list.append((row[column["groupId"]], row[column["actName"]], row[column["actDueDate"]],
                                           row[column["actRdyDate"]], row[column["actExcData"]], row[column["actDlyDate"]], row[column["actComDate"]]))

            # Checks the groupId from the current record if it matches the query's groupID
            for groupId in groupId_2:
                # checks if it matches the groupId keyword and any characters after it
                if re.search(('{}.*').format(groupId), row[column["groupId"]]):
                    groupId_2_list.append((row[column["groupId"]], row[column["actName"]], row[column["actDueDate"]],
                                           row[column["actRdyDate"]], row[column["actExcData"]], row[column["actDlyDate"]], row[column["actComDate"]]))

        WorkOrderNo = row[column["workOrderNo"]]

    return finalList


def printRecords(records):

    for record in records:
        print(record)


def generateReport(csvfile, querylist, headers):
    logger.info("Generating report " + csvfile + " ...")
    utils.write_to_csv(csvfile, querylist, headers, reportsFolderPath)
    csvFiles.append(csvfile)


def generateCPlusIpReport(zipFileName, startDate, endDate, groupId, emailSubject, emailTo=''):

    logger.info('********************')
    logger.info("Processing [" + emailSubject + "] ...")

    csvFiles.clear()

    groupIdList_1 = ['CNP']
    groupIdStr_1 = ', '.join([(group_Id) for group_Id in groupIdList_1])

    groupIdList_2 = ['GSDT6']
    groupIdStr_2 = ', '.join([(group_Id) for group_Id in groupIdList_2])

    priority1 = ['LLC Accepted by Singtel']
    priority2 = ['GSDT Co-ordination OS LLC', 'GSDT Co-ordination Work']

    actList_1 = ['Change C+ IP',
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
                 'Cease Resale SGO CHN']

    actStr_1 = ', '.join([("'" + activity + "'") for activity in actList_1])

    actList_2 = ['GSDT Co-ordination Work',
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
                 'Disconnect RMS for ATM']

    actStr_2 = ', '.join([("'" + activity + "'") for activity in actList_2])

    queryArgs = ([[startDate, endDate, groupIdStr_1, actStr_1, groupIdStr_2, actStr_2], 'CNP'],
                 [[startDate, endDate, groupIdStr_2, actStr_2, groupIdStr_1, actStr_1], 'GSDT6'])

    for list in queryArgs:
        if groupId == '' or list[1].casefold() == groupId.casefold():
            sqlquery = ("""
                        SELECT DISTINCT ORD.order_code,
                            ORD.service_number,
                            PRD.network_product_code,
                            PRD.network_product_desc,
                            CUS.name,
                            ORD.order_type,
                            PER.role,
                            ORD.current_crd,
                            ORD.taken_date,
                            CAST(ACT.activity_code AS UNSIGNED) AS activity_code,
                            ACT.name,
                            ACT.due_date,
                            ACT.ready_date,
                            DATE(ACT.exe_date),
                            DATE(ACT.dly_date),
                            ACT.completed_date
                        FROM RestInterface_activity ACT
                            INNER JOIN RestInterface_order ORD ON ORD.id = ACT.order_id
                            LEFT OUTER JOIN RestInterface_customer CUS ON CUS.id = ORD.customer_id
                            LEFT OUTER JOIN RestInterface_person PER ON PER.id = ACT.person_id
                            LEFT OUTER JOIN RestInterface_npp NPP ON ORD.id = NPP.order_id
                            AND NPP.level = 'MainLine'
                            LEFT OUTER JOIN RestInterface_product PRD ON NPP.product_id = PRD.id
                        WHERE ORD.id IN (
                                SELECT DISTINCT ORD.id
                                FROM RestInterface_activity ACT
                                    LEFT OUTER JOIN RestInterface_order ORD ON ORD.id = ACT.order_id
                                    LEFT OUTER JOIN RestInterface_person PER ON PER.id = ACT.person_id
                                WHERE PER.role LIKE '{}%'
                                    AND ACT.completed_date BETWEEN '{}' AND '{}'
                                    AND ACT.name IN ({})
                            )
                            AND (
                                (
                                    PER.role LIKE '{}%'
                                    AND ACT.completed_date BETWEEN '{}' AND '{}'
                                    AND ACT.name IN ({})
                                )
                                OR (
                                    PER.role LIKE '{}%'
                                    AND ACT.name IN ({})
                                )
                            )
                        ORDER BY ORD.order_code,
                            activity_code;
                    """).format(list[0][2], list[0][0], list[0][1], list[0][3], list[0][2], list[0][0], list[0][1], list[0][3], list[0][4], list[0][5])

            csvFile = ("{}_{}.csv").format(
                list[1], utils.getCurrentDateTime())
            outputList = processList(orionDb.queryToList(
                sqlquery), groupIdList_1, groupIdList_2, priority1, priority2)
            generateReport(csvFile, outputList, headers)
            updateTableauDB(outputList, list[1])

    if csvFiles:
        zipFile = ("{}_{}.zip").format(zipFileName, utils.getCurrentDateTime())
        utils.zipFile(csvFiles, zipFile, reportsFolderPath,
                      defaultConfig['ZipPassword'])
        sendEmail(emailSubject, zipFile, emailTo)

    logger.info("Processing [" + emailSubject + "] complete")


def generateCPlusIpReportGrp(zipFileName, startDate, endDate, groupId, emailSubject, emailTo=''):

    logger.info('********************')
    logger.info("Processing [" + emailSubject + "] ...")

    csvFiles.clear()

    groupIdList_1 = ['CNP8', 'CNP10', 'CNP12', 'CNP30', 'CNP31', 'CNP32', 'CNP33',
                     'CNP34', 'CNP35', 'CNP36', 'CNP37', 'CNP38', 'CNP39', 'CNP40',
                     'CNP41', 'CNP42', 'CNP43', 'CNP44', 'CNP45']
    groupIdStr_1 = ', '.join([("'" + groupIdList_1 + "'")
                             for groupIdList_1 in groupIdList_1])

    groupIdList_2 = ['GSDT68', 'GSDT610', 'GSDT612', 'GSDT630', 'GSDT631', 'GSDT632',
                     'GSDT633', 'GSDT634', 'GSDT635', 'GSDT636', 'GSDT637', 'GSDT638',
                     'GSDT639', 'GSDT640', 'GSDT641', 'GSDT642', 'GSDT643', 'GSDT644', 'GSDT645']
    groupIdStr_2 = ', '.join([("'" + groupIdList_2 + "'")
                             for groupIdList_2 in groupIdList_2])

    priority1 = ['LLC Accepted by Singtel']
    priority2 = ['GSDT Co-ordination OS LLC', 'GSDT Co-ordination Work']

    actList_1 = ['Change C+ IP',
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
                 'Cease Resale SGO CHN']

    actStr_1 = ', '.join([("'" + activity + "'") for activity in actList_1])

    actList_2 = ['GSDT Co-ordination Work',
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
                 'Disconnect RMS for ATM']

    actStr_2 = ', '.join([("'" + activity + "'") for activity in actList_2])

    queryArgs = ([[startDate, endDate, groupIdStr_1, actStr_1, groupIdStr_2, actStr_2], 'CNP'],
                 [[startDate, endDate, groupIdStr_2, actStr_2, groupIdStr_1, actStr_1], 'GSDT6'])

    for list in queryArgs:
        if groupId == '' or list[1].casefold() == groupId.casefold():
            sqlquery = ("""
                        SELECT DISTINCT ORD.order_code,
                            ORD.service_number,
                            PRD.network_product_code,
                            PRD.network_product_desc,
                            CUS.name,
                            ORD.order_type,
                            PER.role,
                            ORD.current_crd,
                            ORD.taken_date,
                            CAST(ACT.activity_code AS UNSIGNED) AS activity_code,
                            ACT.name,
                            ACT.due_date,
                            ACT.ready_date,
                            DATE(ACT.exe_date),
                            DATE(ACT.dly_date),
                            ACT.completed_date
                        FROM RestInterface_activity ACT
                            INNER JOIN RestInterface_order ORD ON ORD.id = ACT.order_id
                            LEFT OUTER JOIN RestInterface_customer CUS ON CUS.id = ORD.customer_id
                            LEFT OUTER JOIN RestInterface_person PER ON PER.id = ACT.person_id
                            LEFT OUTER JOIN RestInterface_npp NPP ON ORD.id = NPP.order_id
                            AND NPP.level = 'MainLine'
                            LEFT OUTER JOIN RestInterface_product PRD ON NPP.product_id = PRD.id
                        WHERE ORD.id IN (
                                SELECT DISTINCT ORD.id
                                FROM RestInterface_activity ACT
                                    LEFT OUTER JOIN RestInterface_order ORD ON ORD.id = ACT.order_id
                                    LEFT OUTER JOIN RestInterface_person PER ON PER.id = ACT.person_id
                                WHERE PER.role IN ({})
                                    AND ACT.completed_date BETWEEN '{}' AND '{}'
                                    AND ACT.name IN ({})
                            )
                            AND (
                                (
                                    PER.role IN ({})
                                    AND ACT.completed_date BETWEEN '{}' AND '{}'
                                    AND ACT.name IN ({})
                                )
                                OR (
                                    PER.role IN ({})
                                    AND ACT.name IN ({})
                                )
                            )
                        ORDER BY ORD.order_code,
                            activity_code;
                    """).format(list[0][2], list[0][0], list[0][1], list[0][3], list[0][2], list[0][0], list[0][1], list[0][3], list[0][4], list[0][5])

            csvFile = ("{}_{}.csv").format(list[1], utils.getCurrentDateTime())
            outputList = processList(orionDb.queryToList(
                sqlquery), groupIdList_1, groupIdList_2, priority1, priority2)
            generateReport(csvFile, outputList, headers)
            updateTableauDB(outputList, list[1])

    if csvFiles:
        zipFile = ("{}_{}.zip").format(zipFileName, utils.getCurrentDateTime())
        utils.zipFile(csvFiles, zipFile, reportsFolderPath,
                      defaultConfig['ZipPassword'])
        sendEmail(emailSubject, zipFile, emailTo)

    logger.info("Processing [" + emailSubject + "] complete")


def generateMegaPopReport(zipFileName, startDate, endDate, groupId, emailSubject, emailTo=''):

    logger.info('********************')
    logger.info("Processing [" + emailSubject + "] ...")

    csvFiles.clear()

    groupIdList_1 = ['MPP']
    groupIdStr_1 = ', '.join([(group_Id) for group_Id in groupIdList_1])

    groupIdList_2 = ['GSDT8']
    groupIdStr_2 = ', '.join([(group_Id) for group_Id in groupIdList_2])

    priority1 = ['Circuit Configuration']
    priority2 = ['GSDT Co-ordination OS LLC', 'GSDT Co-ordination Work']

    actList_1 = ['Activate E-Access',
                 'Activate EVPL',
                 'Activate RMS/TOPS - MP',
                 'Cease IaaS Connectivity',
                 'Cessation of PE Port',
                 'Cessation of UTM',
                 'Change C+ IP',
                 'Circuit Configuration',
                 'Config PE Port',
                 'Config UTM',
                 'De-Activate E-Access',
                 'De-Activate RMS on PE',
                 'End to End PNOC Test',
                 'Extranet Config',
                 'GSDT Co-ordination Work',
                 'm2m EVC Provisioning',
                 'mLink EVC Provisioning',
                 'mLink EVC Termination',
                 'Pre-Configuration on PE',
                 'Re-config E-Access',
                 'Reconfiguration',
                 'Recovery - PNOC Work',
                 'SD-WAN Config',
                 'SD-WAN Svc Provisioning']

    actStr_1 = ', '.join([("'" + activity + "'") for activity in actList_1])

    actList_2 = ['Cease IaaS Connectivity',
                 'Connection of RMS for MP',
                 'GSDT Co-ordination Wk-BQ',
                 'GSDT Co-ordination Work',
                 'GSDT Co-ordination Wrk-BQ']

    actStr_2 = ', '.join([("'" + activity + "'") for activity in actList_2])

    queryArgs = ([[startDate, endDate, groupIdStr_1, actStr_1, groupIdStr_2, actStr_2], 'MPP'],
                 [[startDate, endDate, groupIdStr_2, actStr_2, groupIdStr_1, actStr_1], 'GSDT8'])

    for list in queryArgs:
        if groupId == '' or list[1].casefold() == groupId.casefold():
            sqlquery = ("""
                        SELECT DISTINCT ORD.order_code,
                            ORD.service_number,
                            PRD.network_product_code,
                            PRD.network_product_desc,
                            CUS.name,
                            ORD.order_type,
                            PER.role,
                            ORD.current_crd,
                            ORD.taken_date,
                            CAST(ACT.activity_code AS UNSIGNED) AS activity_code,
                            ACT.name,
                            ACT.due_date,
                            ACT.ready_date,
                            DATE(ACT.exe_date),
                            DATE(ACT.dly_date),
                            ACT.completed_date
                        FROM RestInterface_activity ACT
                            INNER JOIN RestInterface_order ORD ON ORD.id = ACT.order_id
                            LEFT OUTER JOIN RestInterface_customer CUS ON CUS.id = ORD.customer_id
                            LEFT OUTER JOIN RestInterface_person PER ON PER.id = ACT.person_id
                            LEFT OUTER JOIN RestInterface_npp NPP ON ORD.id = NPP.order_id
                            AND NPP.level = 'MainLine'
                            LEFT OUTER JOIN RestInterface_product PRD ON NPP.product_id = PRD.id
                        WHERE ORD.id IN (
                                SELECT DISTINCT ORD.id
                                FROM RestInterface_activity ACT
                                    LEFT OUTER JOIN RestInterface_order ORD ON ORD.id = ACT.order_id
                                    LEFT OUTER JOIN RestInterface_person PER ON PER.id = ACT.person_id
                                WHERE PER.role LIKE '{}%'
                                    AND ACT.completed_date BETWEEN '{}' AND '{}'
                                    AND ACT.name IN ({})
                            )
                            AND (
                                (
                                    PER.role LIKE '{}%'
                                    AND ACT.completed_date BETWEEN '{}' AND '{}'
                                    AND ACT.name IN ({})
                                )
                                OR (
                                    PER.role LIKE '{}%'
                                    AND ACT.name IN ({})
                                )
                            )
                        ORDER BY ORD.order_code,
                            activity_code;
                    """).format(list[0][2], list[0][0], list[0][1], list[0][3], list[0][2], list[0][0], list[0][1], list[0][3], list[0][4], list[0][5])

            csvFile = ("{}_{}.csv").format(list[1], utils.getCurrentDateTime())
            outputList = processList(orionDb.queryToList(
                sqlquery), groupIdList_1, groupIdList_2, priority1, priority2)
            generateReport(csvFile, outputList, headers)
            updateTableauDB(outputList, list[1])

    if csvFiles:
        zipFile = ("{}_{}.zip").format(zipFileName, utils.getCurrentDateTime())
        utils.zipFile(csvFiles, zipFile, reportsFolderPath,
                      defaultConfig['ZipPassword'])
        sendEmail(emailSubject, zipFile, emailTo)

    logger.info("Processing [" + emailSubject + "] complete")


def generateMegaPopReportGrp(zipFileName, startDate, endDate, groupId, emailSubject, emailTo=''):

    logger.info('********************')
    logger.info("Processing [" + emailSubject + "] ...")

    csvFiles.clear()

    groupIdList_1 = ['MPP70', 'MPP71', 'MPP72', 'MPP73', 'MPP74', 'MPP75', 'MPP76', 'MPP77', 'MPP78',
                     'MPP79', 'MPP80', 'MPP81', 'MPP82', 'MPP83', 'MPP84', 'MPP85', 'MPP86', 'MPP87', 'MPP88', 'MPP89']
    groupIdStr_1 = ', '.join([("'" + groupIdList_1 + "'")
                             for groupIdList_1 in groupIdList_1])

    groupIdList_2 = ['GSDT870', 'GSDT871', 'GSDT872', 'GSDT873', 'GSDT874', 'GSDT875', 'GSDT876', 'GSDT877', 'GSDT878',
                     'GSDT879', 'GSDT880', 'GSDT881', 'GSDT882', 'GSDT883', 'GSDT884', 'GSDT885', 'GSDT886', 'GSDT887', 'GSDT888', 'GSDT889']
    groupIdStr_2 = ', '.join([("'" + groupIdList_2 + "'")
                             for groupIdList_2 in groupIdList_2])

    priority1 = ['Circuit Configuration']
    priority2 = ['GSDT Co-ordination OS LLC', 'GSDT Co-ordination Work']

    actList_1 = ['Activate E-Access',
                 'Activate EVPL',
                 'Activate RMS/TOPS - MP',
                 'Cease IaaS Connectivity',
                 'Cessation of PE Port',
                 'Cessation of UTM',
                 'Change C+ IP',
                 'Circuit Configuration',
                 'Config PE Port',
                 'Config UTM',
                 'De-Activate E-Access',
                 'De-Activate RMS on PE',
                 'End to End PNOC Test',
                 'Extranet Config',
                 'GSDT Co-ordination Work',
                 'm2m EVC Provisioning',
                 'mLink EVC Provisioning',
                 'mLink EVC Termination',
                 'Pre-Configuration on PE',
                 'Re-config E-Access',
                 'Reconfiguration',
                 'Recovery - PNOC Work',
                 'SD-WAN Config',
                 'SD-WAN Svc Provisioning']

    actStr_1 = ', '.join([("'" + activity + "'") for activity in actList_1])

    actList_2 = ['Cease IaaS Connectivity',
                 'Connection of RMS for MP',
                 'GSDT Co-ordination Wk-BQ',
                 'GSDT Co-ordination Work',
                 'GSDT Co-ordination Wrk-BQ']

    actStr_2 = ', '.join([("'" + activity + "'") for activity in actList_2])

    queryArgs = ([[startDate, endDate, groupIdStr_1, actStr_1, groupIdStr_2, actStr_2], 'MPP'],
                 [[startDate, endDate, groupIdStr_2, actStr_2, groupIdStr_1, actStr_1], 'GSDT8'])

    for list in queryArgs:
        if groupId == '' or list[1].casefold() == groupId.casefold():
            sqlquery = ("""
                        SELECT DISTINCT ORD.order_code,
                            ORD.service_number,
                            PRD.network_product_code,
                            PRD.network_product_desc,
                            CUS.name,
                            ORD.order_type,
                            PER.role,
                            ORD.current_crd,
                            ORD.taken_date,
                            CAST(ACT.activity_code AS UNSIGNED) AS activity_code,
                            ACT.name,
                            ACT.due_date,
                            ACT.ready_date,
                            DATE(ACT.exe_date),
                            DATE(ACT.dly_date),
                            ACT.completed_date
                        FROM RestInterface_activity ACT
                            INNER JOIN RestInterface_order ORD ON ORD.id = ACT.order_id
                            LEFT OUTER JOIN RestInterface_customer CUS ON CUS.id = ORD.customer_id
                            LEFT OUTER JOIN RestInterface_person PER ON PER.id = ACT.person_id
                            LEFT OUTER JOIN RestInterface_npp NPP ON ORD.id = NPP.order_id
                            AND NPP.level = 'MainLine'
                            LEFT OUTER JOIN RestInterface_product PRD ON NPP.product_id = PRD.id
                        WHERE ORD.id IN (
                                SELECT DISTINCT ORD.id
                                FROM RestInterface_activity ACT
                                    LEFT OUTER JOIN RestInterface_order ORD ON ORD.id = ACT.order_id
                                    LEFT OUTER JOIN RestInterface_person PER ON PER.id = ACT.person_id
                                WHERE PER.role IN ({})
                                    AND ACT.completed_date BETWEEN '{}' AND '{}'
                                    AND ACT.name IN ({})
                            )
                            AND (
                                (
                                    PER.role IN ({})
                                    AND ACT.completed_date BETWEEN '{}' AND '{}'
                                    AND ACT.name IN ({})
                                )
                                OR (
                                    PER.role IN ({})
                                    AND ACT.name IN ({})
                                )
                            )
                        ORDER BY ORD.order_code,
                            activity_code;
                    """).format(list[0][2], list[0][0], list[0][1], list[0][3], list[0][2], list[0][0], list[0][1], list[0][3], list[0][4], list[0][5])

            csvFile = ("{}_{}.csv").format(list[1], utils.getCurrentDateTime())
            outputList = processList(orionDb.queryToList(
                sqlquery), groupIdList_1, groupIdList_2, priority1, priority2)
            generateReport(csvFile, outputList, headers)
            updateTableauDB(outputList, list[1])

    if csvFiles:
        zipFile = ("{}_{}.zip").format(zipFileName, utils.getCurrentDateTime())
        utils.zipFile(csvFiles, zipFile, reportsFolderPath,
                      defaultConfig['ZipPassword'])
        sendEmail(emailSubject, zipFile, emailTo)

    logger.info("Processing [" + emailSubject + "] complete")


def generateSingnetReport(zipFileName, startDate, endDate, groupId, emailSubject, emailTo=''):

    logger.info('********************')
    logger.info("Processing [" + emailSubject + "] ...")

    csvFiles.clear()

    groupIdList_1 = ['SGX1', 'SGX3']
    # groupIdStr_1 = ', '.join([("'" + groupId + "'") for groupId in groupId])

    groupIdList_2 = ['GSDT7']
    # groupIdStr_2 = ', '.join([(group_Id) for group_Id in groupIdList_2])

    priority1 = ['Cease SG Cct @PubNet ', 'Cease SingNet Svc',
                 'Provision SingNet Svc', 'Modify SingNet Svc', 'Circuit Configuration']
    priority2 = []

    actList_1 = ['Activate E-Access',
                 'Cease SG Cct @PubNet ',
                 'Cease SingNet Svc',
                 'Circuit Configuration',
                 'Comn SgNet PubNet Wk',
                 'Comn SingNet PubNet Wk',
                 'De-Activate E-Access',
                 'DNS Set-Up',
                 'GSDT Co-ordination Work',
                 'IP Verification - MegaPOP',
                 'Modify Microsoft Direct',
                 'Modify SingNet Svc',
                 'Provision SingNet Svc',
                 'Reconfiguration',
                 'Recovery - PNOC Work',
                 'SN Evolve Static IP Work']

    actStr_1 = ', '.join([("'" + activity + "'") for activity in actList_1])

    actList_2 = ['Circuit Configuration',
                 'GSDT Coordination',
                 'GSDT Co-ordination WK-BQ',
                 'GSDT Co-ordination Work',
                 'GSDT Co-ordination Wrk-BQ']

    actStr_2 = ', '.join([("'" + activity + "'") for activity in actList_2])

    queryArgs = ([[startDate, endDate, "PER.role LIKE 'SGX1%' OR PER.role LIKE 'SGX3%'", actStr_1, "PER.role LIKE 'GSDT7%'", actStr_2], 'SGX1'],
                 [[startDate, endDate, "PER.role LIKE 'GSDT7%'", actStr_2, "PER.role LIKE 'SGX1%' OR PER.role LIKE 'SGX3%'", actStr_1], 'GSDT7'])

    for list in queryArgs:
        if groupId == '' or list[1].casefold() == groupId.casefold():
            sqlquery = ("""
                        SELECT
                            DISTINCT ORD.order_code,
                            ORD.service_number,
                            PRD.network_product_code,
                            PRD.network_product_desc,
                            CUS.name,
                            ORD.order_type,
                            PER.role,
                            ORD.current_crd,
                            ORD.taken_date,
                            CAST(ACT.activity_code AS UNSIGNED) AS activity_code,
                            ACT.name,
                            ACT.due_date,
                            ACT.ready_date,
                            DATE(ACT.exe_date),
                            DATE(ACT.dly_date),
                            ACT.completed_date
                        FROM
                            RestInterface_activity ACT
                            INNER JOIN RestInterface_order ORD ON ORD.id = ACT.order_id
                            LEFT OUTER JOIN RestInterface_customer CUS ON CUS.id = ORD.customer_id
                            LEFT OUTER JOIN RestInterface_person PER ON PER.id = ACT.person_id
                            LEFT OUTER JOIN RestInterface_npp NPP ON ORD.id = NPP.order_id
                            AND NPP.level = 'MainLine'
                            LEFT OUTER JOIN RestInterface_product PRD ON NPP.product_id = PRD.id
                        WHERE
                            ORD.id IN (
                                SELECT
                                    DISTINCT ORD.id
                                FROM
                                    RestInterface_activity ACT
                                    LEFT OUTER JOIN RestInterface_order ORD ON ORD.id = ACT.order_id
                                    LEFT OUTER JOIN RestInterface_person PER ON PER.id = ACT.person_id
                                WHERE
                                    ({})
                                    AND ACT.completed_date BETWEEN '{}'
                                    AND '{}'
                                    AND ACT.name IN ({})
                            )
                            AND (
                                (
                                    ({})
                                    AND ACT.completed_date BETWEEN '{}'
                                    AND '{}'
                                    AND ACT.name IN ({})
                                )
                                OR (
                                    ({})
                                    AND ACT.name IN ({})
                                )
                            )
                        ORDER BY
                            ORD.order_code,
                            activity_code;
                    """).format(list[0][2], list[0][0], list[0][1], list[0][3], list[0][2], list[0][0], list[0][1], list[0][3], list[0][4], list[0][5])

            csvFile = ("{}_{}.csv").format(list[1], utils.getCurrentDateTime())
            outputList = processList(orionDb.queryToList(
                sqlquery), groupIdList_1, groupIdList_2, priority1, priority2)
            generateReport(csvFile, outputList, headers)
            updateTableauDB(outputList, list[1])

    if csvFiles:
        zipFile = ("{}_{}.zip").format(zipFileName, utils.getCurrentDateTime())
        utils.zipFile(csvFiles, zipFile, reportsFolderPath,
                      defaultConfig['ZipPassword'])
        sendEmail(emailSubject, zipFile, emailTo)

    logger.info("Processing [" + emailSubject + "] complete")


def generateStixReport(zipFileName, startDate, endDate, emailSubject, emailTo=''):

    logger.info('********************')
    logger.info("Processing [" + emailSubject + "] ...")

    csvFiles.clear()

    groupId = ['GSDT9']
    groupIdStr = ', '.join([(groupId) for groupId in groupId])

    actList = ['GSDT Co-ordination OS LLC', 'GSDT Co-ordination Work']
    actStr = ', '.join([("'" + activity + "'") for activity in actList])

    sqlquery = ("""
                    SELECT DISTINCT ORD.order_code,
                        ORD.service_number,
                        PRD.network_product_code,
                        PRD.network_product_desc,
                        CUS.name,
                        ORD.order_type,
                        PER.role,
                        ORD.current_crd,
                        ORD.taken_date,
                        CAST(ACT.activity_code AS UNSIGNED) AS activity_code,
                        ACT.name,
                        ACT.due_date,
                        ACT.ready_date,
                        DATE(ACT.exe_date),
                        DATE(ACT.dly_date),
                        ACT.completed_date
                    FROM RestInterface_activity ACT
                        INNER JOIN RestInterface_order ORD ON ORD.id = ACT.order_id
                        LEFT OUTER JOIN RestInterface_customer CUS ON CUS.id = ORD.customer_id
                        LEFT OUTER JOIN RestInterface_person PER ON PER.id = ACT.person_id
                        LEFT OUTER JOIN RestInterface_npp NPP ON ORD.id = NPP.order_id
                        AND NPP.level = 'MainLine'
                        LEFT OUTER JOIN RestInterface_product PRD ON NPP.product_id = PRD.id
                    WHERE PER.role LIKE '{}%'
                        AND ACT.completed_date BETWEEN '{}' AND '{}'
                        AND ACT.name IN ({})
                    ORDER BY ORD.order_code,
                        activity_code;
                """).format(groupIdStr, startDate, endDate, actStr)

    csvFile = ("{}_{}.csv").format('GSDT9', utils.getCurrentDateTime())
    outputList = processList(
        orionDb.queryToList(sqlquery), groupId, '', [], [])
    generateReport(csvFile, outputList, headers2)
    updateTableauDB(outputList, 'GSDT9')

    if csvFiles:
        zipFile = ("{}_{}.zip").format(zipFileName, utils.getCurrentDateTime())
        utils.zipFile(csvFiles, zipFile, reportsFolderPath,
                      defaultConfig['ZipPassword'])
        sendEmail(emailSubject, zipFile, emailTo)

    logger.info("Processing [" + emailSubject + "] complete")


def generateInternetReport(zipFileName, startDate, endDate, emailSubject, emailTo=''):

    logger.info('********************')
    logger.info("Processing [" + emailSubject + "] ...")

    csvFiles.clear()

    groupId = ['GSDT_PS21', 'GSDT_PS23']
    groupIdStr = ', '.join([("'" + groupId + "'") for groupId in groupId])

    actList = ['GSDT Co-ordination Work', 'GSDT GI Coordination Work']
    actStr = ', '.join([("'" + activity + "'") for activity in actList])

    sqlquery = ("""
                    SELECT DISTINCT ORD.order_code,
                        ORD.service_number,
                        PRD.network_product_code,
                        PRD.network_product_desc,
                        CUS.name,
                        ORD.order_type,
                        PER.role,
                        ORD.current_crd,
                        ORD.taken_date,
                        CAST(ACT.activity_code AS UNSIGNED) AS activity_code,
                        ACT.name,
                        ACT.due_date,
                        ACT.ready_date,
                        DATE(ACT.exe_date),
                        DATE(ACT.dly_date),
                        ACT.completed_date
                    FROM RestInterface_activity ACT
                        INNER JOIN RestInterface_order ORD ON ORD.id = ACT.order_id
                        LEFT OUTER JOIN RestInterface_customer CUS ON CUS.id = ORD.customer_id
                        LEFT OUTER JOIN RestInterface_person PER ON PER.id = ACT.person_id
                        LEFT OUTER JOIN RestInterface_npp NPP ON ORD.id = NPP.order_id
                        AND NPP.level = 'MainLine'
                        LEFT OUTER JOIN RestInterface_product PRD ON NPP.product_id = PRD.id
                    WHERE PER.role IN ({})
                        AND ACT.completed_date BETWEEN '{}' AND '{}'
                        AND ACT.name IN ({})
                    ORDER BY ORD.order_code,
                        activity_code;
                """).format(groupIdStr, startDate, endDate, actStr)

    csvFile = ("{}_{}.csv").format(
        'GSDT_PS21_GSDT_PS23', utils.getCurrentDateTime())
    outputList = processList(
        orionDb.queryToList(sqlquery), groupId, '', [], [])
    generateReport(csvFile, outputList, headers2)
    updateTableauDB(outputList, 'GSDT_PS23')

    if csvFiles:
        zipFile = ("{}_{}.zip").format(zipFileName, utils.getCurrentDateTime())
        utils.zipFile(csvFiles, zipFile, reportsFolderPath,
                      defaultConfig['ZipPassword'])
        sendEmail(emailSubject, zipFile, emailTo)

    logger.info("Processing [" + emailSubject + "] complete")


def generateSDWANReport(zipFileName, startDate, endDate, emailSubject, emailTo=''):

    logger.info('********************')
    logger.info("Processing [" + emailSubject + "] ...")

    csvFiles.clear()

    groupId = ['GSP_SDN_TM', 'GSDT_TM']
    groupIdStr = ', '.join([("'" + groupId + "'") for groupId in groupId])

    actList = ['GSDT Co-ordination Wk-BQ',
               'GSDT Co-ordination Wrk-BQ', 'GSP-TM Coordination Work']
    actStr = ', '.join([("'" + activity + "'") for activity in actList])

    sqlquery = ("""
                    SELECT DISTINCT ORD.order_code,
                        ORD.service_number,
                        PRD.network_product_code,
                        PRD.network_product_desc,
                        CUS.name,
                        ORD.order_type,
                        PER.role,
                        ORD.current_crd,
                        ORD.taken_date,
                        CAST(ACT.activity_code AS UNSIGNED) AS activity_code,
                        ACT.name,
                        ACT.due_date,
                        ACT.ready_date,
                        DATE(ACT.exe_date),
                        DATE(ACT.dly_date),
                        ACT.completed_date
                    FROM RestInterface_activity ACT
                        INNER JOIN RestInterface_order ORD ON ORD.id = ACT.order_id
                        LEFT OUTER JOIN RestInterface_customer CUS ON CUS.id = ORD.customer_id
                        LEFT OUTER JOIN RestInterface_person PER ON PER.id = ACT.person_id
                        LEFT OUTER JOIN RestInterface_npp NPP ON ORD.id = NPP.order_id
                        AND NPP.level = 'MainLine'
                        LEFT OUTER JOIN RestInterface_product PRD ON NPP.product_id = PRD.id
                    WHERE PER.role IN ({})
                        AND ACT.completed_date BETWEEN '{}' AND '{}'
                        AND ACT.name IN ({})
                    ORDER BY ORD.order_code,
                        activity_code;
                """).format(groupIdStr, startDate, endDate, actStr)

    csvFile = ("{}_{}.csv").format(
        'GSP_SDN_TM_GSDT_TM', utils.getCurrentDateTime())
    outputList = processList(
        orionDb.queryToList(sqlquery), groupId, '', [], [])
    generateReport(csvFile, outputList, headers2)
    updateTableauDB(outputList, 'GSP_SDN_TM_GSDT_TM')

    if csvFiles:
        zipFile = ("{}_{}.zip").format(zipFileName, utils.getCurrentDateTime())
        utils.zipFile(csvFiles, zipFile, reportsFolderPath,
                      defaultConfig['ZipPassword'])
        sendEmail(emailSubject, zipFile, emailTo)

    logger.info("Processing [" + emailSubject + "] complete")


def sendEmail(subject, attachment, emailTo):

    emailBodyText = """
        Hello,

        Please see attached ORION report.


        Thanks you and best regards,
        Orion Team
    """

    emailBodyhtml = """\
        <html>
        <p>Hello,</p>
        <p>Please see attached ORION report.</p>
        <p>&nbsp;</p>
        <p>Thank you and best regards,</p>
        <p>Orion Team</p>
        </html>
        """

    # Enable/Disable email
    if defaultConfig.getboolean('SendEmail'):
        try:
            emailClient = EmailClient()
            emailClient.subject = emailClient.addTimestamp(subject)

            # Check the [DEFAULT]>EmailInfo option from config.ini
            # If (EmailInfo = EmailTest), use receiverTo under [Email] config
            # This is for production environment
            if defaultConfig['EmailInfo'] == 'Email':
                emailClient.receiverTo = emailConfig["receiverTo"] + \
                    ';' + emailTo
            # If (EmailInfo = EmailTest), use receiverTo under [EmailTest] config
            # This is for test environment
            else:
                emailClient.receiverTo = emailConfig["receiverTo"]

            emailClient.receiverCc = emailConfig["receiverCc"]
            emailClient.emailBodyText = emailBodyText
            emailClient.emailBodyHtml = emailBodyhtml
            emailClient.attachFile(os.path.join(reportsFolderPath, attachment))

            if utils.getPlatform() == 'Windows':
                emailClient.win32comSend()
            else:
                emailClient.server = emailConfig['server']
                emailClient.port = emailConfig['port']
                emailClient.sender = emailConfig['sender']
                emailClient.emailFrom = emailConfig["from"]
                emailClient.smtpSend()

        except Exception as e:
            logger.error("Failed to send email.")
            raise Exception(e)

from scripts import utils
import constants as const
import logging.config
import logging
import os
from scripts.DBConnection import DBConnection
from scripts.EmailClient import EmailClient
import pandas as pd
import numpy as np
import sqlalchemy as db
from models import SdoBase

logger = logging.getLogger(__name__)
defaultConfig = None
emailConfig = None
dbConfig = None
csvFiles = []
reportsFolderPath = None
orionDb = None
tableauDb = None


def initialize(config):
    global defaultConfig, emailConfig, dbConfig, reportsFolderPath, orionDb, tableauDb
    defaultConfig = config['DEFAULT']
    emailConfig = config[defaultConfig['EmailInfo']]
    dbConfig = config[defaultConfig['DatabaseEnv']]
    reportsFolderPath = os.path.join(
        os.getcwd(), defaultConfig['ReportsFolder'])

    orionDb = DBConnection(dbConfig['host'], dbConfig['port'],
                           dbConfig['orion_db'], dbConfig['orion_user'], dbConfig['orion_pwd'])
    orionDb.connect()

    tableauDb = DBConnection(dbConfig['host'], dbConfig['port'],
                             dbConfig['tableau_db'], dbConfig['tableau_user'], dbConfig['tableau_pwd'])
    tableauDb.connect()
    # tableauDb.createTablesFromMetadata(SdoBase)


def updateTableauDB(dataframe, report_id):
    # Allow Tableaue DB update
    if defaultConfig.getboolean('UpdateTableauDB'):
        try:
            logger.info(
                'Inserting records to ' + dbConfig['tableau_db'] + '.' + defaultConfig['TableauTable'] + ' for ' + report_id.lower() + ' ...')

            # Get a list of public holidays from Tableaue DB
            t_GSP_holidays = tableauDb.getTableMetadata('t_GSP_holidays')
            # SELECT * FROM t_GSP_holidays
            query = db.select([t_GSP_holidays])
            publicHolidays = tableauDb.queryToList2(query)
            df_holidays = pd.DataFrame(
                data=publicHolidays, columns=['Holiday', 'Date'])
            HOLIDAYS = df_holidays['Date'].values.tolist()

            df = pd.DataFrame(dataframe)

            # Counts the number of valid days between begindates and enddates, not including the day of enddates.
            # Weekends (Sat/Sun) and Public Holidays are excluded
            # Add 3 new columns (COM_Date_SS, COM_Date_RI and COM_Date_TI)
            for index, row in df.iterrows():
                if not pd.isnull(row["COM_Date_SS"]):
                    startDate = row["COM_Date_SS"]
                    endDate = row["CRDNew"]

                    if endDate >= startDate:
                        df_bdays = pd.bdate_range(
                            start=startDate, end=endDate, freq='C', holidays=HOLIDAYS)
                        df.at[index, "SS_to_CRD"] = len(df_bdays)
                    else:
                        # Need to switch start & end date and explicitly set the count to negative
                        # as pd.bdate_range() does not calculate negative business days
                        df_bdays = pd.bdate_range(
                            start=endDate, end=startDate, freq='C', holidays=HOLIDAYS)
                        df.at[index, "SS_to_CRD"] = len(df_bdays)*(-1)

                if not pd.isnull(row["COM_Date_RI"]):
                    startDate = row["COM_Date_RI"]
                    endDate = row["CRDNew"]

                    if endDate >= startDate:
                        df_bdays = pd.bdate_range(
                            start=startDate, end=endDate, freq='C', holidays=HOLIDAYS)
                        df.at[index, "RI_to_CRD"] = len(df_bdays)
                    else:
                        # Need to switch start & end date and explicitly set the count to negative
                        # as pd.bdate_range() does not calculate negative business days
                        df_bdays = pd.bdate_range(
                            start=endDate, end=startDate, freq='C', holidays=HOLIDAYS)
                        df.at[index, "RI_to_CRD"] = len(df_bdays)*(-1)

                if not pd.isnull(row["COM_Date_TI"]):
                    startDate = row["CRDNew"]
                    endDate = row["COM_Date_TI"]

                    if endDate >= startDate:
                        df_bdays = pd.bdate_range(
                            start=startDate, end=endDate, freq='C', holidays=HOLIDAYS)
                        df.at[index, "TI_to_CRD"] = len(df_bdays)
                    else:
                        # Need to switch start & end date and explicitly set the count to negative
                        # as pd.bdate_range() does not calculate negative business days
                        df_bdays = pd.bdate_range(
                            start=endDate, end=startDate, freq='C', holidays=HOLIDAYS)
                        df.at[index, "TI_to_CRD"] = len(df_bdays)*(-1)

            # add new columns
            df["report_id"] = report_id.lower()
            df["update_time"] = pd.Timestamp.now()

            # set columns to datetime type
            df[const.TABLEAU_DATE_COLUMNS] = df[const.TABLEAU_DATE_COLUMNS].apply(
                pd.to_datetime)

            # set empty values to null
            df.replace('', None)
            # insert records to DB
            tableauDb.insertDataframeToTable(df, defaultConfig['TableauTable'])

            # logger.info("TableauDB Updated for " + report_id.lower())

        except Exception as err:
            logger.info("Failed processing DB " + dbConfig['tableau_db'] + ' at ' +
                        dbConfig['tableau_user'] + '@' + dbConfig['host'] + ':' + dbConfig['port'] + '.')
            logger.exception(err)

            raise Exception(err)


def sendEmail(subject, attachment):

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
            emailClient.subject = emailClient.addTimestamp2(subject)
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


def generateSdoSingnetReport(fileName, reportDate, emailSubject):

    logger.info('********************')
    logger.info("Processing [" + emailSubject + "] ...")

    productCodeList = [
        'SGN0051',
        'SGN0119',
        'SGN0157',
        'SGN0160',
        'SGN0170',
        'SGN0340',
        'SGN2004'
    ]

    productCodeInstance = ['SGN0170', 'SGN2004']

    df_tableauWorkorders = getWorkOrdersFromTableau(
        'GSDT7', reportDate, productCodeList)
    df_rawReport = createNewReportDf(df_tableauWorkorders['Workorder_no'])
    df_rawReport = addParamAndSvcnoColToDf(
        df_rawReport, productCodeInstance, const.PARAMETERS_TO_SEARCH)
    df_rawReport = addOrderInfoColToDf(df_rawReport)

    # Add Site survey (SS) activities to df_rawReport
    ss_activitiesMap = [
        {"productCodes": ['SGN0170', 'SGN2004', 'SGN0051', 'SGN0157'],
         "activities": ['Site Survey - A End']},
        {"productCodes": ['SGN0160'],
         "activities": ['Check HSD Resources']},
        {"productCodes": ['SGN0119', 'SGN0340'],
         "activities": ['Site Survey - A End', 'Site Survey', 'Check HSD Resources']}
    ]

    df_ss = createActivityDf(df_rawReport, ss_activitiesMap, const.SS_COLUMNS)
    df_rawReport = pd.merge(df_rawReport, df_ss, how='left')

    # Add Routing info (RI) activities to df_rawReport
    ri_activitiesMap = [
        {"productCodes": ['SGN0170', 'SGN2004', 'SGN0051', 'SGN0157'],
         "activities": ['Cct Allocate-ETE Routing', 'Circuit Allocation']},
        {"productCodes": ['SGN0160'],
         "activities": ['TNP/HSD Activities']},
        {"productCodes": ['SGN0119', 'SGN0340'],
         "activities": ['Cct Allocate-ETE Routing', 'Circuit Allocation', 'TNP/HSD Activities']}
    ]

    df_ri = createActivityDf(df_rawReport, ri_activitiesMap, const.RI_COLUMNS)
    df_rawReport = pd.merge(df_rawReport, df_ri, how='left')

    # Add Testing and Installation (TI) activities to df_rawReport
    ti_activitiesMap = [
        {"productCodes": ['SGN0170', 'SGN2004', 'SGN0051', 'SGN0157'],
         "activities": ['CPE Instln & Testingg', 'End-To-End Test - A End']},
        {"productCodes": ['SGN0160'],
         "activities": ['E-To-E Test (PNOC)']},
        {"productCodes": ['SGN0119', 'SGN0340'],
         "activities": ['CPE Instln & Testingg', 'DWFM Installation Work', 'E-To-E Test (PNOC)', 'End-To-End Test - A End']}
    ]

    df_ti = createActivityDf(df_rawReport, ti_activitiesMap, const.TI_COLUMNS)
    df_rawReport = pd.merge(df_rawReport, df_ti, how='left')
    df_finalReport = df_rawReport[const.FINAL_COLUMNS]

    # Set emply cells to NULL only for the ProjectManager column
    df_finalReport['ProjectManager'].replace(np.nan, 'NULL', inplace=True)

    # Write to CSV
    csvFiles = []
    csvFile = ("{}_{}.csv").format(fileName, utils.getCurrentDateTime2())
    logger.info("Generating report " + csvFile + " ...")
    csvFiles.append(csvFile)
    csvfilePath = os.path.join(reportsFolderPath, csvFile)
    df_finalReport.to_csv(csvfilePath, index=False)

    # Insert records to tableau db
    updateTableauDB(df_finalReport, 'SINGNET')

    # Compress files and send email
    if csvFiles:
        attachement = None
        if defaultConfig.getboolean('CompressFiles'):
            zipFile = ("{}_{}.zip").format(
                fileName, utils.getCurrentDateTime2())
            utils.zipFile(csvFiles, zipFile, reportsFolderPath,
                          defaultConfig['ZipPassword'])
            attachement = zipFile
        else:
            attachement = csvFile
        sendEmail(emailSubject, attachement)

    logger.info("Processing [" + emailSubject + "] complete")


def generateSdoMegaPopReport(fileName, reportDate, emailSubject):

    logger.info('********************')
    logger.info("Processing [" + emailSubject + "] ...")

    productCodeList = [
        'ELK0052',
        'ELK0053',
        'ELK0055',
        'ELK0089',
        'GEL0001',
        'GEL0018',
        'GEL0023',
        'GEL0036'
    ]

    productCodeInstance = ['GEL0001', 'GEL0018', 'GEL0023', 'GEL0036']

    df_tableauWorkorders = getWorkOrdersFromTableau(
        'GSDT8', reportDate, productCodeList)
    df_rawReport = createNewReportDf(df_tableauWorkorders['Workorder_no'])
    df_rawReport = addParamAndSvcnoColToDf(
        df_rawReport, productCodeInstance, const.PARAMETERS_TO_SEARCH)
    df_rawReport = addOrderInfoColToDf(df_rawReport)

    # Add Site survey (SS) activities to df_rawReport
    ss_activitiesMap = [
        {"productCodes": ['GEL0001', 'GEL0036', 'ELK0052', 'ELK0053', 'ELK0055', 'ELK0089'],
         "activities": ['Site Survey - A End']},
        {"productCodes": ['GEL0018'],
         "activities": ['Site Survey - A End', 'Site Survey', 'Check & Plan Fiber  - SME', 'Check & Plan Fiber  - ON']},
        {"productCodes": ['GEL0023'],
         "activities": ['Site Survey', 'Check & Plan Fiber  - SME', 'Check & Plan Fiber  - ON']}
    ]

    df_ss = createActivityDf(df_rawReport, ss_activitiesMap, const.SS_COLUMNS)
    df_rawReport = pd.merge(df_rawReport, df_ss, how='left')

    # Add Routing info (RI) activities to df_rawReport
    ri_activitiesMap = [
        {"productCodes": ['GEL0001', 'GEL0036', 'ELK0052', 'ELK0053', 'ELK0055', 'ELK0089'],
         "activities": ['Cct Allocate-ETE Routing', 'Circuit Allocation']},
        {"productCodes": ['GEL0018', 'GEL0023'],
         "activities": ['Cct Allocate-ETE Routing', 'Cct Allocate ETE Rtg - ON']}
    ]

    df_ri = createActivityDf(df_rawReport, ri_activitiesMap, const.RI_COLUMNS)
    df_rawReport = pd.merge(df_rawReport, df_ri, how='left')

    # Add Testing and Installation (TI) activities to df_rawReport
    ti_activitiesMap = [
        {"productCodes": ['GEL0001', 'GEL0036', 'ELK0052', 'ELK0053', 'ELK0055', 'ELK0089'],
         "activities": ['CPE Instln & Testing', 'End-To-End Test - A End']},
        {"productCodes": ['GEL0018', 'GEL0023'],
         "activities": ['DWFM Installation Work', 'CPE Instln & Testing']}
    ]

    df_ti = createActivityDf(df_rawReport, ti_activitiesMap, const.TI_COLUMNS)
    df_rawReport = pd.merge(df_rawReport, df_ti, how='left')
    df_finalReport = df_rawReport[const.FINAL_COLUMNS]

    # Set emply cells to NULL only for the ProjectManager column
    df_finalReport['ProjectManager'].replace(np.nan, 'NULL', inplace=True)

    # Write to CSV
    csvFiles = []
    csvFile = ("{}_{}.csv").format(fileName, utils.getCurrentDateTime2())
    logger.info("Generating report " + csvFile + " ...")
    csvFiles.append(csvFile)
    csvfilePath = os.path.join(reportsFolderPath, csvFile)
    utils.dataframeToCsv(df_finalReport, csvfilePath)

    # Insert records to tableau db
    updateTableauDB(df_finalReport, 'MEGAPOP')

    # Compress files and send email
    if csvFiles:
        attachement = None
        if defaultConfig.getboolean('CompressFiles'):
            zipFile = ("{}_{}.zip").format(
                fileName, utils.getCurrentDateTime2())
            utils.zipFile(csvFiles, zipFile, reportsFolderPath,
                          defaultConfig['ZipPassword'])
            attachement = zipFile
        else:
            attachement = csvFile
        sendEmail(emailSubject, attachement)

    logger.info("Processing [" + emailSubject + "] complete")


def getWorkOrdersFromTableau(reportId, reportDate, productCodeList):
    query = (""" 
                SELECT
                    DISTINCT Workorder_no
                FROM
                    t_GSP_ip_svcs
                WHERE
                    report_id IN ('{}')
                    AND Order_Type = 'Provide'
                    AND Product_Code IN (
                        {}
                    )
                    AND DATE(update_time) = '{}'; 
            """).format(reportId, utils.listToString(productCodeList), reportDate)

    result = tableauDb.queryToList(query)
    return pd.DataFrame(data=result, columns=['Workorder_no'])


def createNewReportDf(df_Workorders):
    query = (""" 
                SELECT
                    DISTINCT ORD.id AS OrderId,
                    ORD.order_code AS OrderCode,
                    ORD.service_number AS ServiceNumber,
                    REPLACE(
                        REPLACE(
                            REPLACE(ORD.service_number, 'ELITE', ''),
                            'ETH',
                            ''
                        ),
                        'GWL',
                        ''
                    ) AS ServiceNumberUpd,
                    PRD.network_product_code AS ProductCode,
                    ORD.current_crd AS CRD,
                    CUS.name AS CustomerName,
                    ORD.taken_date AS OrderCreated,
                    ORD.order_type AS OrderType,
                    (
                        SELECT
                            GROUP_CONCAT(
                                CONCAT_WS(" ", family_name, given_name)
                                ORDER BY
                                    id DESC SEPARATOR " / "
                            )
                        FROM
                            RestInterface_contactdetails
                        WHERE
                            order_id = ORD.id
                            AND contact_type = "Project Manager"
                        GROUP BY
                            order_id
                    ) AS ProjectManager
                FROM
                    RestInterface_order ORD
                    LEFT JOIN RestInterface_npp NPP ON NPP.order_id = ORD.id
                    AND NPP.level = 'Mainline'
                    AND NPP.status <> 'Cancel'
                    LEFT JOIN RestInterface_product PRD ON PRD.id = NPP.product_id
                    LEFT JOIN RestInterface_customer CUS ON CUS.id = ORD.customer_id
                WHERE
                    ORD.order_code IN ({})
                    AND ORD.order_type = 'Provide'; 
            """).format(utils.listToString(df_Workorders.to_list()))

    result = orionDb.queryToList(query)
    df = pd.DataFrame(data=result, columns=['OrderId', 'OrderCode', 'ServiceNumber', 'ServiceNumberUpd',
                      'ProductCode', 'CRD', 'CustomerName', 'OrderCreated', 'OrderType', 'ProjectManager'])
    return df


def addParamAndSvcnoColToDf(dataframe, productCodes, parameter_names):

    df = pd.DataFrame(dataframe)

    # Add new ParameterName and ParameterValue columns to df
    df_instance = df[df['ProductCode'].isin(productCodes)]
    parameterList = getParametersInfo(
        df_instance['ServiceNumberUpd'], parameter_names)
    df_parameters = pd.DataFrame(data=parameterList, columns=[
                                 'ServiceNumberUpd', 'ParameterName', 'ParameterValue'])
    df = pd.merge(df, df_parameters, how='left')

    # Add new ServiceNoNew column
    df['ServiceNoNew'] = None

    # Copy df['ServiceNumberUpd'] values to df['ServiceNoNew] where df['ServiceNumberUpd'] == df_nonInstance['ServiceNumberUpd']
    df_nonInstance = df[~df['ProductCode'].isin(productCodes)]
    df.loc[df['ServiceNumberUpd'].isin(df_nonInstance['ServiceNumberUpd'].to_list(
    )), 'ServiceNoNew'] = df_nonInstance['ServiceNumberUpd']

    # Copy df_parameters['ParameterValue'] values to df['ServiceNoNew] where df['ServiceNumberUpd'] == df_parameters['ParameterValue']
    df.set_index('ServiceNumberUpd', inplace=True)
    df_parameters.set_index('ServiceNumberUpd', inplace=True)
    df_parameters.rename(
        columns={'ParameterValue': 'ServiceNoNew'}, inplace=True)
    df.update(df_parameters)
    df.reset_index(inplace=True)

    return df


def getParametersInfo(df_serviceNo, parameter_names):
    serviceNoList = utils.listToString(df_serviceNo.to_list())
    parameterNamesList = utils.listToString(parameter_names)

    query = (""" 
            SELECT
                DISTINCT ORD.service_number AS ServiceNumberUpd,
                PAR.parameter_name AS ParameterName,
                PAR.parameter_value AS ParameterValue
            FROM
                RestInterface_order ORD
                LEFT JOIN RestInterface_npp NPP ON NPP.order_id = ORD.id
                AND NPP.level = 'Mainline'
                AND NPP.status <> 'Cancel'
                LEFT JOIN RestInterface_parameter PAR ON PAR.npp_id = NPP.id
                AND PAR.parameter_name IN ({})
            WHERE
                ORD.order_type = 'Provide'
                AND ORD.service_number IN ({});
        """).format(parameterNamesList, serviceNoList)

    return orionDb.queryToList(query)


def addOrderInfoColToDf(dataframe):
    df = pd.DataFrame(dataframe)
    serviceNoList = df['ServiceNoNew'].dropna().to_list()

    query = (""" 
            SELECT
                DISTINCT service_number AS ServiceNoNew,
                id AS OrderIdNew,
                order_code AS OrderCodeNew,
                current_crd AS CRDNew
            FROM
                RestInterface_order
            WHERE
                order_type = 'Provide'
                AND order_status <> 'Cancelled'
                AND service_number IN ({});
        """).format(utils.listToString(serviceNoList))

    result = orionDb.queryToList(query)
    df_orderInfo = pd.DataFrame(
        data=result, columns=['ServiceNoNew', 'OrderIdNew', 'OrderCodeNew', 'CRDNew'])

    # There are cases where a service number can have multiple orders.
    # Choose the order with the latest CRD.
    # Drop duplicate using ServiceNoNew by keeping the latest CRDNew.
    df_sorted = df_orderInfo.sort_values(
        by=['ServiceNoNew', 'CRDNew'])
    df_rmDuplicates = df_sorted.drop_duplicates(
        subset=['ServiceNoNew'], keep='last')

    df = pd.merge(df, df_rmDuplicates, how='left')

    return df


def createActivityDf(df_rawReport, activitiesMap, actColumns):
    df_rawReport = pd.DataFrame(df_rawReport)
    workOrderList = df_rawReport['OrderCodeNew'].dropna().to_list()

    # Merge activities into 1 unique list
    uniqueActList = []
    for activityMap in activitiesMap:
        if len(uniqueActList) == 0:
            uniqueActList = activityMap['activities']
        else:
            uniqueActList = list(
                set(uniqueActList + activityMap['activities']))

    query = (""" 
            SELECT
                ORD.order_code AS OrderCodeNew,
                PER.role AS GroupId,
                CAST(ACT.activity_code AS UNSIGNED) AS step_no,
                ACT.name AS ActivityName,
                ACT.due_date,
                ACT.status,
                ACT.ready_date,
                DATE(ACT.exe_date) AS exe_date,
                DATE(ACT.dly_date) AS dly_date,
                ACT.completed_date
            FROM
                RestInterface_order ORD
                JOIN RestInterface_activity ACT ON ACT.order_id = ORD.id
                LEFT JOIN RestInterface_person PER ON PER.id = ACT.person_id
            WHERE
                ACT.name IN ({})
                AND ORD.order_code IN ({})
            ORDER BY
                OrderCodeNew,
                step_no;
        """).format(utils.listToString(uniqueActList), utils.listToString(workOrderList))

    result = orionDb.queryToList(query)
    df_actInfo = pd.DataFrame(data=result, columns=['OrderCodeNew', 'GroupId', 'step_no', 'ActivityName',
                              'due_date', 'status', 'ready_date', 'exe_date', 'dly_date', 'completed_date'])
    df_actFinal = removeDuplicates(df_rawReport, df_actInfo, activitiesMap)
    df_actFinal.columns = actColumns

    return df_actFinal


def removeDuplicates(df_rawReport, df_actInfo, activitiesMap):

    df_rawReport = pd.DataFrame(df_rawReport)
    df_actInfo = pd.DataFrame(df_actInfo)
    df_actFinal = pd.DataFrame(columns=df_actInfo.columns)

    for activityMap in activitiesMap:
        # Get the df_rawReport records only if the product codes are included in the activityMap's product codes
        df_products = df_rawReport[df_rawReport['ProductCode'].isin(
            activityMap['productCodes'])]
        # Get the df_actInfo records only if the OrderCodeNew is in the df_products records
        df_act = df_actInfo[df_actInfo['OrderCodeNew'].isin(
            df_products['OrderCodeNew'].to_list())]
        # Get the df_act records only if the activities are included in the activityMap's activities
        df_valid_act = df_act[df_act['ActivityName'].isin(
            activityMap['activities'])]

        if not df_valid_act.empty:
            actPriority = activityMap['activities']

            # If there are duplicates, select the activity with the highest step_no
            # Drop duplicate OrderCodeNew with same ActivityName by keeping the highest step_no column
            df_sorted = df_valid_act.sort_values(
                by=['OrderCodeNew', 'step_no'])
            df_rmDuplicates = df_sorted.drop_duplicates(
                subset=['OrderCodeNew', 'ActivityName'], keep='last')

            # If there are duplicates, select 1 activity based from the sequence in the actPriority list
            # Drop duplicate OrderCodeNew with diff ActivityName by keeping 1 ActivityName from the actPriority list
            df_actPriority = pd.DataFrame(df_rmDuplicates)
            df_actPriority['ActivityName'] = pd.Categorical(
                values=df_actPriority['ActivityName'], categories=actPriority)
            df_actPrioritySorted = df_actPriority.sort_values(
                by=['OrderCodeNew', 'ActivityName'])
            df_actPriorityRmDup = df_actPrioritySorted.drop_duplicates(
                subset=['OrderCodeNew'], keep='first')

            df_actFinal = pd.concat([df_actFinal, df_actPriorityRmDup])

    return df_actFinal

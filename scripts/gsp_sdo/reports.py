from scripts import utils
import logging.config
import logging
import os
from scripts.DBConnection import DBConnection
from scripts.EmailClient import EmailClient
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)
defaultConfig = None
emailConfig = None
dbConfig = None
csvFiles = []
reportsFolderPath = None
orionDb = None
tableauDb = None


def loadConfig(config):
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


def generateReport(csvfile, querylist, headers):
    logger.info("Generating report " + csvfile + " ...")
    utils.write_to_csv(csvfile, querylist, headers, reportsFolderPath)
    csvFiles.append(csvfile)


def sendEmail(subject, attachment):

    emailBodyText = """
        Hello,

        Please see attached ORION report.


        Thanks you and best regards,
        Orion Team
    """

    emailBodyhtml = """\
        <html>
        <p>Helllo,</p>
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
            logger.error(e)


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

    df_tableauWorkorders = getWorkOrdersFromTableau(
        'GSDT7', reportDate, productCodeList)
    df_rawReport = createNewReportDf(df_tableauWorkorders['Workorder_no'])
    df_rawReport = addParamAndSvcnoColToDf(df_rawReport, ['SGN0170', 'SGN2004'], [
        'FTTHNo', 'MetroENo', 'GigawaveNo'])
    df_rawReport = addOrderInfoColToDf(df_rawReport)

    ss_activitiesMap = [
        {"productCodes": ['SGN0170', 'SGN2004', 'SGN0051', 'SGN0157'],
         "activities": ['Site Survey - A End']},
        {"productCodes": ['SGN0160'],
         "activities": ['Check HSD Resources']},
        {"productCodes": ['SGN0119', 'SGN0340'],
         "activities": ['Site Survey - A End', 'Site Survey', 'Check HSD Resources']}
    ]

    createActivityDf(df_rawReport, ss_activitiesMap)

    # print(df_rawReport)
    # df_rawReport.to_csv('{}.csv'.format(fileName))

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

    df_tableauWorkorders = getWorkOrdersFromTableau(
        'GSDT8', reportDate, productCodeList)
    df_rawReport = createNewReportDf(df_tableauWorkorders['Workorder_no'])
    df_rawReport = addParamAndSvcnoColToDf(df_rawReport, ['GEL0001', 'GEL0018', 'GEL0023', 'GEL0036'], [
        'FTTHNo', 'MetroENo', 'GigawaveNo'])
    df_rawReport = addOrderInfoColToDf(df_rawReport)

    ss_activities = [
        {"productCodes": ['GEL0001', 'GEL0036', 'ELK0052', 'ELK0053', 'ELK0055', 'ELK0089'],
         "activities": ['Site Survey - A End']},
        {"productCodes": ['GEL0018'],
         "activities": ['Site Survey - A End', 'Site Survey', 'Check & Plan Fiber  - SME', 'Check & Plan Fiber  - ON']},
        {"productCodes": ['GEL0023'],
         "activities": ['Site Survey', 'Check & Plan Fiber  - SME', 'Check & Plan Fiber  - ON']}
    ]

    createActivityDf(df_rawReport, ss_activities)

    # print(df_rawReport)
    # df_rawReport.to_csv('{}.csv'.format(fileName))

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
    return pd.DataFrame(result)


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
    return pd.DataFrame(result)


def addParamAndSvcnoColToDf(dataframe, productCodes, parameter_names):

    df = pd.DataFrame(dataframe)

    # Add new ParameterName and ParameterValue columns to df
    df_instance = df[df['ProductCode'].isin(productCodes)]
    parameterList = getParametersInfo(
        df_instance['ServiceNumberUpd'], parameter_names)
    df_parameters = pd.DataFrame(parameterList)
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
                AND service_number IN ({});
        """).format(utils.listToString(serviceNoList))

    result = orionDb.queryToList(query)
    df_orderInfo = pd.DataFrame(result)
    df = pd.merge(df, df_orderInfo, how='left')

    return df


def createActivityDf(df_rawReport, activitiesMap):
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
    df_actInfo = pd.DataFrame(result)

    removeDuplicates(df_rawReport, df_actInfo, activitiesMap)

    return df_actInfo


def removeDuplicates(df_rawReport, df_actInfo, activitiesMap):

    df_rawReport = pd.DataFrame(df_rawReport)
    df_actInfo = pd.DataFrame(df_actInfo)

    for activityMap in activitiesMap:
        df_products = df_rawReport[df_rawReport['ProductCode'].isin(
            activityMap['productCodes'])]
        df_act = df_actInfo[df_actInfo['OrderCodeNew'].isin(
            df_products['OrderCodeNew'].to_list())]

        if not df_act.empty:
            actPriority = activityMap['activities']

            # Drop duplicate OrderCodeNew with same ActivityName by keeping the highest step_no column
            df_sorted = df_act.sort_values(by=['OrderCodeNew', 'step_no'])
            print(df_sorted)
            df_rmDuplicates = df_sorted.drop_duplicates(
                subset=['OrderCodeNew', 'ActivityName'], keep='last')

            # Drop duplicate OrderCodeNew with diff ActivityName by keeping 1 ActivityName from the actPriority list
            df_actPriority = pd.DataFrame(df_rmDuplicates)
            df_actPriority['ActivityName'] = pd.Categorical(
                values=df_actPriority['ActivityName'], categories=actPriority)
            df_actPrioritySorted = df_actPriority.sort_values(
                by=['OrderCodeNew', 'ActivityName'])
            df_actPriorityRmDup = df_actPrioritySorted.drop_duplicates(
                subset=['OrderCodeNew'], keep='first')
            print(df_actPriorityRmDup)

    return None

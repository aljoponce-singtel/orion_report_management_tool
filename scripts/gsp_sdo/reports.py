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

    ss_activities = [
        {"productCodes": ['SGN0170', 'SGN2004', 'SGN0051', 'SGN0157'],
         "activities": ['site survey - a-end', 'site survey - a end']},
        {"productCodes": ['SGN0160'],
         "activities": ['check hsd resources']},
        {"productCodes": ['SGN0119', 'SGN0340'],
         "activities": ['site survey - a-end', 'site survey - a end', 'site survey', 'check hsd resources']}
    ]

    createActivityDf(df_rawReport, ss_activities)
    # createActivityDf(None, ss_activities)

    print(df_rawReport)
    # df_rawReport.to_csv('{}.csv'.format(fileName))

    logger.info("Processing [" + emailSubject + "] complete")


def createActivityDf(dataframe, activities):
    df = pd.DataFrame(dataframe)
    workOrderList = df['OrderCodeNew'].dropna().to_list()

    # Merge activities into 1 unique list
    uniqueActList = []
    for ss_activity in activities:
        if len(uniqueActList) == 0:
            uniqueActList = ss_activity['activities']
        else:
            uniqueActList = list(
                set(uniqueActList + ss_activity['activities']))

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
    # """).format(utils.listToString(workOrderList))
    # """).format(utils.listToString(uniqueActList), testGetOrders())

    result = orionDb.queryToList(query)
    df_actInfo = pd.DataFrame(result)

    # Drop duplicate records and keep the bigger number in step_no column
    df_actInfo = df_actInfo.drop_duplicates(
        subset=['OrderCodeNew'], keep='last')

    # df = pd.merge(df, df_orderInfo, how='left')

    print(df_actInfo)

    return df_actInfo


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


def testGetSingnetOrders():
    stringList = """
                    'YGI1065001'    , 'YKM1278001', 'YKR8731001', 'YKR9088002'
                    , 'YKT3095001'    , 'YKU3804001', 'YKU4002001', 'YKU4059001'
                    , 'YKU4059002'    , 'YKU4059003', 'YKU4059004', 'YKV6672002'
                    , 'YKW1591001'    , 'YKZ0651002', 'YKZ0651004', 'YLA6353001'
                    , 'YLF3848001'    , 'YLG8823003', 'YLH4193001', 'YLH5590001'
                    , 'YLH8127001'    , 'YLF3848002', 'YLI7090003', 'YLN0462003'
                    , 'YLN0683001'    , 'YLN9396002', 'YLO5550001', 'YLO5578001'
                    , 'YLO5578002'    , 'YLP2015003', 'YLP3877001', 'YLP6649001'
                    , 'YLQ3592001'    , 'YLR4857001', 'YLR8929001', 'YLS0441001'
                    , 'YLS0514001'    , 'YLT2834003', 'YLT2834005', 'YLU0831002'
                    , 'YLU5783001'    , 'YLU9799003', 'YLV0544003', 'YLV8229001'
                    , 'YLV9237001'    , 'YLV9431001', 'YLV9955004', 'YLW2584002'
                    , 'YLW3469003'    , 'YLW3860001', 'YLW4149001', 'YLW5991001'
                    , 'YLW6308001'    , 'YLW6344003', 'YLW6512001', 'YLW9794001'
                    , 'YLX1979003'    , 'YLX3480001', 'YLX3889001', 'YLX6041001'
                    , 'YLX6238001'    , 'YLX6492001', 'YLX6584001', 'YLX6731001'
                    , 'YLX6776001'    , 'YLX7029001', 'YLX7714002', 'YLX9807003'
                    , 'YLX9853002'    , 'YLX9895001', 'YLY2547003', 'YLY2547004'
                    , 'YLY3670001'    , 'YLY5267001', 'YLX4585001', 'YLY7195001'
                    , 'YLY7648002'    , 'YLY7689001', 'YLY7750003', 'YLY8023001'
                    , 'YLY8461003'    , 'YLZ1243003', 'YLZ1363002', 'YLZ1790001'
                    , 'YLZ1973002'    , 'YLZ6932001', 'YLZ7475003', 'YMA0745001'
                    , 'YMA0653003'    , 'YMA0919001', 'YMA1037002', 'YMA1388003'
                    , 'YMA2163001'    , 'YMA2589001', 'YMA9690003', 'YMA9761001'
                    , 'YMB2410003'    , 'YMB2410004', 'YMB3316001', 'YMB3856001'
                    , 'YMC0596001'    , 'YMC0707001', 'YMC0630003', 'YMC0752004'
                    , 'YMC1634001'    , 'YMC1838003', 'YMC2347003', 'YMC2760001'
                    , 'YMC5743003'    , 'YMC9946003', 'YMD4272001', 'YMD4401003'
                    , 'YMD7706003'    , 'YMD7644001', 'YMD8203001', 'YMD9826001'
                    , 'YME0512001'    , 'YME1055001', 'YME4165001', 'YME5054002'
                    , 'YME5014003'    , 'YME5388003', 'YME6259001', 'YMF1526001'
                    , 'YMF6456001'    , 'YMF8784001', 'YMF7664003', 'YMF9155003'
                    , 'YMF9364003'    , 'YMF9427001', 'YGI3137001', 'YMG4667001'
                    , 'YMG4735003'    , 'YMG4921001', 'YMG7423003', 'YMH1355003'
                    , 'YMH1902001'    , 'YMH1929003', 'YMH2572003', 'YMH2615001'
                    , 'YMH2879001'    , 'YMH3832001', 'YMH3306001', 'YMH3306002'
                    , 'YMH7632002'    , 'YMH8758001', 'YMH9272003', 'YMI2687001'
                    , 'YBU0520002'    , 'YMS9943001', 'YMT6734003', 'YMV0468001'
                    , 'XZE5805001'    , 'YJF7609001'
                """

    return stringList

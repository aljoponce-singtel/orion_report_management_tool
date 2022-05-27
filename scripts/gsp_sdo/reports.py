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
    addOrderInfoColToDf(df_rawReport, ['SGN0170', 'SGN2004'], [
        'FTTHNo', 'MetroENo', 'GigawaveNo'])

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


def addOrderInfoColToDf(dataframe, productCodes, parameter_names):

    df = pd.DataFrame(dataframe)

    # add new ParameterName and ParameterValue columns to df
    df_instance = df[df['ProductCode'].isin(productCodes)]
    parameterList = getParametersInfo(
        df_instance['ServiceNumberUpd'], parameter_names)
    df_parameters = pd.DataFrame(parameterList)

    df = pd.merge(df, df_parameters, how='left')

    df['ServiceNoNew'] = None

    # indexes = df.loc[df['ServiceNumberUpd'].isin(df_parameters['ServiceNumberUpd'].values)].index
    # df.at[indexes, 'ServiceNoNew'] = df_parameters['ParameterValue'].values

    # df['ServiceNoNew'] = df_parameters[df['ServiceNumberUpd'] == df_parameters['ServiceNumberUpd']]['ParameterValue']

    # df = df.set_index('ServiceNumberUpd')
    # df_parameters.set_index('ServiceNumberUpd')
    # df['ServiceNoNew'] = np.where(df_parameters.loc[df.index]['ParameterValue'])

    df_nonInstance = df[~df['ProductCode'].isin(productCodes)]
    # df['ServiceNoNew'].isin(df_nonInstance['ServiceNumberUpd'].values) = df['ServiceNumberUpd']
    # print(df_nonInstance['ServiceNumberUpd'].to_list())
    # df[df['ServiceNumberUpd'].isin(df_nonInstance['ServiceNumberUpd'].to_list()), 'ServiceNoNew'] = df['ServiceNumberUpd']

    # df.isin(df_nonInstance['ServiceNumberUpd'].to_list())['ServiceNoNew'] = df_nonInstance['ServiceNumberUpd']

    df.loc[df['ServiceNumberUpd'].isin(df_nonInstance['ServiceNumberUpd'].to_list()), 'ServiceNoNew'] = df_nonInstance['ServiceNumberUpd']

    print(df)

    df.set_index('ServiceNumberUpd',inplace=True)
    df_parameters.set_index('ServiceNumberUpd',inplace=True)
    df_parameters.rename(columns={'ParameterValue':'ServiceNoNew'}, inplace=True)
    df.update(df_parameters)
    df.reset_index(inplace=True)

    print(df)
    # print(df_parameters)

    # df_nonInstance = df[~df['ProductCode'].isin(productCodes)]
    # df['ServiceNoNew'] = df['ServiceNumberUpd']
    # df[df['ServiceNumberUpd'].isin(df_nonInstance['ServiceNumberUpd'].values), 'ServiceNoNew'] = df['ServiceNumberUpd']

    # df_nonInstance = df[~df['ProductCode'].isin(productCodes)]
    # df_nonInstance.set_index('ServiceNumberUpd',inplace=True)
    # df_nonInstance.rename(columns={'ServiceNumberUpd':'ServiceNoNew'}, inplace=True)
    # df.update(df_parameters)
    # df.reset_index(inplace=True)
    #df['ServiceNoNew'] = df

    # print(df)

    # df.to_csv("sdo_singnet.csv")

    # df_nonInstSvcNo = df_nonInstance['ServiceNumberUpd']
    # nonInstSvcNoList = getOrdersUsingServiceNo(
    #     utils.listToString(df_nonInstSvcNo.to_list()))

    # print(pd.DataFrame(nonInstSvcNoList))


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


def getOrdersUsingServiceNo(serviceNo):
    query = (""" 
            SELECT
                DISTINCT order_code, service_number
            FROM
                RestInterface_order
            WHERE
                service_number IN ({}); 
        """).format(serviceNo)

    return orionDb.queryToList(query)

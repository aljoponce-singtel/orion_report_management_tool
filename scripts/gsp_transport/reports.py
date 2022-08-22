import os
import sys
import logging.config
import logging
import pandas as pd
import numpy as np
import constants as const
from scripts import utils
from scripts.DBConnection import DBConnection
from scripts.EmailClient import EmailClient

# # getting the name of the directory
# # where this file is present.
# current = os.path.dirname(os.path.realpath(__file__))
# # Getting the parent directory name
# # where the current directory is present.
# parent = os.path.dirname(current)
# # adding the parent directory to
# # the sys.path.
# sys.path.append(parent)

# import utils
# from DBConnection import DBConnection
# from EmailClient import EmailClient

logger = logging.getLogger(__name__)
defaultConfig = None
emailConfig = None
dbConfig = None
csvFiles = []
reportsFolderPath = None
orionDb = None


def loadConfig(config):
    global defaultConfig, emailConfig, dbConfig, reportsFolderPath, orionDb
    defaultConfig = config['DEFAULT']
    emailConfig = config[defaultConfig['EmailInfo']]
    dbConfig = config[defaultConfig['DatabaseEnv']]
    reportsFolderPath = os.path.join(
        os.getcwd(), defaultConfig['ReportsFolder'])

    orionDb = DBConnection(dbConfig['host'], dbConfig['port'],
                           dbConfig['orion_db'], dbConfig['orion_user'], dbConfig['orion_pwd'])
    orionDb.connect()


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


def generateTransportReport(fileName, reportDate, emailSubject):
    createTempTable()
    df = pd.DataFrame(getComQueues())
    df_finalReport = pd.DataFrame(columns=const.FINAL_COLUMNS)
    df_orders = df[['Service', 'OrderCode', 'CRD',
                   'ServiceNumber', 'OrderStatus', 'OrderType', 'ProductCode']]

    for index, row in df_orders.drop_duplicates().iterrows():
        df_activities = df[df['OrderCode'] == row['OrderCode']]

        service = row['Service']
        orderCode = row['OrderCode']
        crd = row['CRD']
        serviceNumber = row['ServiceNumber']
        orderStatus = row['OrderStatus']
        orderType = row['OrderType']
        productCode = row['ProductCode']

        if df_orders.at[index, 'Service'] == 'Diginet':
            coordGroupId, coordActName, coordActStatus, coordActDueDate, coordActCOMDate = getActRecord(
                df_activities, ['GSDT Co-ordination Wrk-BQ'])

            if df_orders.at[index, 'OrderType'] == 'Provide' or df_orders.at[index, 'OrderType'] == 'Change':
                preConfigGroupId, preConfigActName, preConfigStatus, preConfigDueDate, preConfigCOMDate = getActRecord(
                    df_activities, ['Circuit Creation'])

            if df_orders.at[index, 'OrderType'] == 'Cease':
                preConfigGroupId, preConfigActName, preConfigStatus, preConfigDueDate, preConfigCOMDate = getActRecord(
                    df_activities, ['Node & Cct Del (DN-ISDN)', 'Node & Cct Deletion (DN)'])

        if df_orders.at[index, 'Service'] == 'MetroE':
            coordGroupId, coordActName, coordActStatus, coordActDueDate, coordActCOMDate = getActRecord(
                df_activities, ['GSDT Co-ordination Wrk-BQ'])

            if df_orders.at[index, 'OrderType'] == 'Provide':
                preConfigGroupId, preConfigActName, preConfigStatus, preConfigDueDate, preConfigCOMDate = getActRecord(
                    df_activities, ['Circuit Creation'])

            if df_orders.at[index, 'OrderType'] == 'Change':
                preConfigGroupId, preConfigActName, preConfigStatus, preConfigDueDate, preConfigCOMDate = getActRecord(
                    df_activities, ['Circuit Creation', 'Change Speed Configure'])

            if df_orders.at[index, 'OrderType'] == 'Cease':
                preConfigGroupId, preConfigActName, preConfigStatus, preConfigDueDate, preConfigCOMDate = getActRecord(
                    df_activities, ['Node & Circuit Deletion'])

        if df_orders.at[index, 'Service'] == 'MegaPop (CE)':
            coordGroupId, coordActName, coordActStatus, coordActDueDate, coordActCOMDate = getActRecord(
                df_activities, [])

            if df_orders.at[index, 'OrderType'] == 'Provide' or df_orders.at[index, 'OrderType'] == 'Change':
                preConfigGroupId, preConfigActName, preConfigStatus, preConfigDueDate, preConfigCOMDate = getActRecord(
                    df_activities, ['Circuit Creation'])

            if df_orders.at[index, 'OrderType'] == 'Cease':
                preConfigGroupId, preConfigActName, preConfigStatus, preConfigDueDate, preConfigCOMDate = getActRecord(
                    df_activities, ['Node & Circuit Deletion'])

        if df_orders.at[index, 'Service'] == 'Gigawave':
            coordGroupId, coordActName, coordActStatus, coordActDueDate, coordActCOMDate = getActRecord(
                df_activities, ['GSDT Co-ordination Work'])

            if df_orders.at[index, 'OrderType'] == 'Provide':
                preConfigGroupId, preConfigActName, preConfigStatus, preConfigDueDate, preConfigCOMDate = getActRecord(
                    df_activities, ['Circuit Creation'])

            if df_orders.at[index, 'OrderType'] == 'Cease':
                preConfigGroupId, preConfigActName, preConfigStatus, preConfigDueDate, preConfigCOMDate = getActRecord(
                    df_activities, ['Circuit Removal from NMS'])

        reportData = [
            service,
            orderCode,
            crd,
            serviceNumber,
            orderStatus,
            orderType,
            productCode,
            preConfigGroupId,
            preConfigActName,
            preConfigStatus,
            preConfigDueDate,
            preConfigCOMDate,
            coordGroupId,
            coordActName,
            coordActStatus,
            coordActDueDate,
            coordActCOMDate
        ]

        # add new data (df_toAdd) to df_report
        df_toAdd = pd.DataFrame(
            data=[reportData], columns=const.FINAL_COLUMNS)
        df_finalReport = pd.concat([df_finalReport, df_toAdd])

    # Write to CSV
    csvFiles = []
    csvFile = ("{}_{}.csv").format(fileName, utils.getCurrentDateTime2())

    if defaultConfig.getboolean('CreateReport') != False:
        logger.info("Generating report " + csvFile + " ...")
        csvFiles.append(csvFile)
        csvfilePath = os.path.join(reportsFolderPath, csvFile)
        utils.dataframeToCsv(df_finalReport, csvfilePath)


def getActRecord(df, activities):
    df_activities = pd.DataFrame(df)
    df_activities = df_activities[df_activities['ActName'].isin(activities)]

    # If there are multiple records, keep only 1 based on priority
    if len(df_activities) > 1:
        # If there are multiple records of the same activity name,
        # keep the activity with the highest step_no
        df_sorted = df_activities.sort_values(by=['ActStepNo', 'ActName'])
        df_dropped_duplicates = df_sorted.drop_duplicates(
            subset=['ActName'], keep='last')

        # If there are multiple records of different activity name,
        # select the 1st activity based from the priority sequence in [activities]

        # Configure 'ActName' column to follow priority sequence from [activities]
        df_actPriority = pd.DataFrame(df_dropped_duplicates)
        df_actPriority['ActName'] = pd.Categorical(
            values=df_actPriority['ActName'], categories=activities)
        # Sort rows by 'ActName' priority
        df_actPrioritySorted = df_actPriority.sort_values(
            by=['ActName'])
        # Keep only 1 activity based from priority (1st row)
        df_activity = df_actPrioritySorted.head(1)

        # print(df_activity[['Service', 'OrderCode',
        #                    'OrderType', 'ActStepNo', 'ActName']])

        df_activities = df_activity

    actGroupId = df_activities['GroupID'].values[0] if np.size(
        df_activities['GroupID'].values) else None
    actName = df_activities['ActName'].values[0] if np.size(
        df_activities['ActName'].values) else None
    actStatus = df_activities['ActStatus'].values[0] if np.size(
        df_activities['ActStatus'].values) else None
    actDueDate = df_activities['ActDueDate'].values[0] if np.size(
        df_activities['ActDueDate'].values) else None
    actComDate = df_activities['ActComDate'].values[0] if np.size(
        df_activities['ActComDate'].values) else None

    return actGroupId, actName, actStatus, actDueDate, actComDate


def createTempTable():
    query = """ 
                CREATE TEMPORARY TABLE COM_QUEUES
                SELECT
                    ORD.id
                FROM
                    RestInterface_order ORD
                    JOIN RestInterface_activity ACT ON ACT.order_id = ORD.id
                    LEFT JOIN RestInterface_person PER ON PER.id = ACT.person_id
                    LEFT JOIN RestInterface_npp NPP ON NPP.order_id = ORD.ID
                    AND NPP.level = 'Mainline'
                    AND NPP.status <> 'Cancel'
                    LEFT JOIN RestInterface_product PRD ON PRD.id = NPP.product_id
                WHERE
                    (
                        (
                            PRD.network_product_code LIKE 'DGN%'
                            AND ORD.order_type IN ('Provide', 'Change', 'Cease')
                            AND (
                                PER.role LIKE 'ODC_%'
                                OR PER.role LIKE 'RDC_%'
                                OR PER.role LIKE 'GSPSG_%'
                            )
                            AND ACT.name = 'GSDT Co-ordination Wrk-BQ'
                            AND ACT.status = 'COM'
                        )
                        OR (
                            PRD.network_product_code LIKE 'DME%'
                            AND ORD.order_type IN ('Provide', 'Change', 'Cease')
                            AND (
                                PER.role LIKE 'ODC_%'
                                OR PER.role LIKE 'RDC_%'
                                OR PER.role LIKE 'GSPSG_%'
                            )
                            AND ACT.name = 'GSDT Co-ordination Wrk-BQ'
                            AND ACT.status = 'COM'
                        )
                        OR (
                            PRD.network_product_code = 'ELK0052'
                            AND (
                                (
                                    ORD.order_type IN ('Provide', 'Change')
                                    AND (
                                        PER.role LIKE 'ODC_%'
                                        OR PER.role LIKE 'RDC_%'
                                        OR PER.role LIKE 'GSPSG_%'
                                    )
                                    AND ACT.name = 'Circuit Creation'
                                    AND ACT.status = 'COM'
                                )
                                OR (
                                    ORD.order_type = 'Cease'
                                    AND (
                                        PER.role LIKE 'ODC_%'
                                        OR PER.role LIKE 'RDC_%'
                                        OR PER.role LIKE 'GSPSG_%'
                                    )
                                    AND ACT.name = 'Node & Circuit Deletion'
                                    AND ACT.status = 'COM'
                                )
                            )
                        )
                        OR (
                            PRD.network_product_code LIKE 'GGW%'
                            AND (
                                (
                                    ORD.order_type = 'Provide'
                                    AND PER.role = 'GSP_LTC_GW'
                                    AND ACT.name = 'GSDT Co-ordination Work'
                                    AND ACT.status = 'COM'
                                )
                                OR (
                                    ORD.order_type = 'Cease'
                                    AND PER.role = 'GSDT31'
                                    AND ACT.name = 'GSDT Co-ordination Work'
                                    AND ACT.status = 'COM'
                                )
                            )
                        )
                    )
                    AND ACT.completed_date BETWEEN '2022-07-01'
                    AND '2022-07-31';
            """

    orionDb.queryWithoutResult(query)


def getComQueues():
    query = """ 
                SELECT
                    DISTINCT (
                        CASE
                            WHEN PRD.network_product_code LIKE 'DGN%' THEN 'Diginet'
                            WHEN PRD.network_product_code LIKE 'DME%' THEN 'MetroE'
                            WHEN PRD.network_product_code = 'ELK0052' THEN 'MegaPop (CE)'
                            WHEN PRD.network_product_code LIKE 'GGW%' THEN 'Gigawave'
                            ELSE 'Service'
                        END
                    ) AS Service,
                    ORD.order_code,
                    ORD.current_crd,
                    ORD.service_number,
                    ORD.order_status,
                    ORD.order_type,
                    PRD.network_product_code,
                    PER.role,
                    CAST(ACT.activity_code AS UNSIGNED) AS step_no,
                    ACT.name,
                    ACT.status,
                    ACT.due_date,
                    ACT.completed_date
                FROM
                    RestInterface_order ORD
                    JOIN RestInterface_activity ACT ON ACT.order_id = ORD.id
                    LEFT JOIN RestInterface_person PER ON PER.id = ACT.person_id
                    LEFT JOIN RestInterface_npp NPP ON NPP.order_id = ORD.ID
                    AND NPP.level = 'Mainline'
                    AND NPP.status <> 'Cancel'
                    LEFT JOIN RestInterface_product PRD ON PRD.id = NPP.product_id
                WHERE
                    ORD.id IN (
                        SELECT
                            id
                        FROM
                            COM_QUEUES
                    )
                    AND (
                        (
                            PRD.network_product_code LIKE 'DGN%'
                            AND (
                                (
                                    ORD.order_type IN ('Provide', 'Change')
                                    AND (
                                        PER.role LIKE 'ODC_%'
                                        OR PER.role LIKE 'RDC_%'
                                        OR PER.role LIKE 'GSPSG_%'
                                    )
                                    AND (
                                        (
                                            ACT.name = 'GSDT Co-ordination Wrk-BQ'
                                            AND ACT.status = 'COM'
                                            AND ACT.completed_date BETWEEN '2022-07-01'
                                            AND '2022-07-31'
                                        )
                                        OR ACT.name = 'Circuit Creation'
                                    )
                                )
                                OR (
                                    ORD.order_type = 'Cease'
                                    AND (
                                        PER.role LIKE 'ODC_%'
                                        OR PER.role LIKE 'RDC_%'
                                        OR PER.role LIKE 'GSPSG_%'
                                    )
                                    AND (
                                        (
                                            ACT.name = 'GSDT Co-ordination Wrk-BQ'
                                            AND ACT.status = 'COM'
                                            AND ACT.completed_date BETWEEN '2022-07-01'
                                            AND '2022-07-31'
                                        )
                                        OR ACT.name IN (
                                            'Node & Cct Del (DN-ISDN)',
                                            'Node & Cct Deletion (DN)'
                                        )
                                    )
                                )
                            )
                        )
                        OR (
                            PRD.network_product_code LIKE 'DME%'
                            AND (
                                (
                                    ORD.order_type IN ('Provide')
                                    AND (
                                        PER.role LIKE 'ODC_%'
                                        OR PER.role LIKE 'RDC_%'
                                        OR PER.role LIKE 'GSPSG_%'
                                    )
                                    AND (
                                        (
                                            ACT.name = 'GSDT Co-ordination Wrk-BQ'
                                            AND ACT.status = 'COM'
                                            AND ACT.completed_date BETWEEN '2022-07-01'
                                            AND '2022-07-31'
                                        )
                                        OR ACT.name = 'Circuit Creation'
                                    )
                                )
                                OR (
                                    ORD.order_type IN ('Change')
                                    AND (
                                        (
                                            (
                                                PER.role LIKE 'ODC_%'
                                                OR PER.role LIKE 'RDC_%'
                                                OR PER.role LIKE 'GSPSG_%'
                                            )
                                            AND ACT.name = 'GSDT Co-ordination Wrk-BQ'
                                            AND ACT.status = 'COM'
                                            AND ACT.completed_date BETWEEN '2022-07-01'
                                            AND '2022-07-31'
                                        )
                                    )
                                    OR (
                                        (
                                            PER.role LIKE 'ODC_%'
                                            OR PER.role LIKE 'RDC_%'
                                            OR PER.role LIKE 'GSP%'
                                        )
                                        AND ACT.name IN ('Circuit Creation', 'Change Speed Configure')
                                    )
                                )
                                OR (
                                    ORD.order_type = 'Cease'
                                    AND (
                                        PER.role LIKE 'ODC_%'
                                        OR PER.role LIKE 'RDC_%'
                                        OR PER.role LIKE 'GSPSG_%'
                                    )
                                    AND (
                                        (
                                            ACT.name = 'GSDT Co-ordination Wrk-BQ'
                                            AND ACT.status = 'COM'
                                            AND ACT.completed_date BETWEEN '2022-07-01'
                                            AND '2022-07-31'
                                        )
                                        OR ACT.name = 'Node & Circuit Deletion'
                                    )
                                )
                            )
                        )
                        OR (
                            PRD.network_product_code = 'ELK0052'
                            AND (
                                (
                                    ORD.order_type IN ('Provide', 'Change')
                                    AND (
                                        PER.role LIKE 'ODC_%'
                                        OR PER.role LIKE 'RDC_%'
                                        OR PER.role LIKE 'GSPSG_%'
                                    )
                                    AND ACT.name = 'Circuit Creation'
                                    AND ACT.status = 'COM'
                                    AND ACT.completed_date BETWEEN '2022-07-01'
                                    AND '2022-07-31'
                                )
                                OR (
                                    ORD.order_type = 'Cease'
                                    AND (
                                        PER.role LIKE 'ODC_%'
                                        OR PER.role LIKE 'RDC_%'
                                        OR PER.role LIKE 'GSPSG_%'
                                    )
                                    AND ACT.name = 'Node & Circuit Deletion'
                                    AND ACT.status = 'COM'
                                    AND ACT.completed_date BETWEEN '2022-07-01'
                                    AND '2022-07-31'
                                )
                            )
                        )
                        OR (
                            PRD.network_product_code LIKE 'GGW%'
                            AND (
                                (
                                    ORD.order_type = 'Provide'
                                    AND (
                                        (
                                            PER.role = 'GSP_LTC_GW'
                                            AND ACT.name = 'GSDT Co-ordination Work'
                                            AND ACT.status = 'COM'
                                            AND ACT.completed_date BETWEEN '2022-07-01'
                                            AND '2022-07-31'
                                        )
                                        OR (
                                            (
                                                PER.role LIKE 'ODC_%'
                                                OR PER.role LIKE 'RDC_%'
                                                OR PER.role LIKE 'GSPSG_%'
                                            )
                                            AND ACT.name = 'Circuit Creation'
                                        )
                                    )
                                )
                                OR (
                                    ORD.order_type = 'Cease'
                                    AND (
                                        (
                                            PER.role = 'GSDT31'
                                            AND ACT.name = 'GSDT Co-ordination Work'
                                            AND ACT.status = 'COM'
                                            AND ACT.completed_date BETWEEN '2022-07-01'
                                            AND '2022-07-31'
                                        )
                                        OR (
                                            PER.role = 'GSP_LTC_GW'
                                            AND ACT.name = 'Circuit Removal from NMS'
                                        )
                                    )
                                )
                            )
                        )
                    )
                ORDER BY
                    Service,
                    ORD.order_type DESC,
                    ACT.name,
                    step_no,
                    PER.role,
                    ORD.order_code;
            """

    result = orionDb.queryToList(query)
    return pd.DataFrame(data=result, columns=const.DRAFT_COLUMNS)

from scripts import utils
import logging.config
import logging
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

    productCodeListStr = ', '.join(
        [("'" + productCode + "'") for productCode in productCodeList])

    tableauOrderList = queryTableauTable(
        'GSDT7', reportDate, productCodeListStr)

    logger.info("Processing [" + emailSubject + "] complete")


def queryTableauTable(reportId, reportDate, productCodes):
    sqlquery = (""" 
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
            """).format(reportId, productCodes, reportDate)

    return tableauDb.queryToList(sqlquery)

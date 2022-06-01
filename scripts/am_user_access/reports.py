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


def sendEmail(startDate, endDate, subject, attachment, emailBody):

    emailBodyText = """
        Hello,

        Please see attached ORION report.


        Thanks you and best regards,
        Orion Team
    """

    emailBodyhtml = ("""\
        <html>
        <p>Hello,</p>
        <p>Please see AM weekly access report from {} to {}.</p>
        <p>{}</p>
        <p>&nbsp;</p>
        <p>Thank you and best regards,</p>
        <p>Orion Team</p>
        </html>
        """).format(startDate, endDate, emailBody)

    # Enable/Disable email
    if defaultConfig.getboolean('SendEmail'):
        try:
            emailClient = EmailClient()
            emailClient.subject = emailClient.addTimestamp2(subject)
            emailClient.receiverTo = emailConfig["receiverTo"]
            emailClient.receiverCc = emailConfig["receiverCc"]
            # emailClient.emailBodyText = emailBodyText
            emailClient.emailBodyHtml = emailBodyhtml

            if attachment:
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


def generateAmUserReport(startDate, endDate, emailSubject):

    logger.info('********************')
    logger.info("Processing [" + emailSubject + "] ...")

    df_report = pd.DataFrame(getAmWeeklyAccess(startDate, endDate))
    # Start index at 1 for table presentation in email
    df_report.index += 1
    sendEmail(startDate, endDate, emailSubject, None, df_report.to_html())

    logger.info("Processing [" + emailSubject + "] complete")


def getAmWeeklyAccess(startDate, endDate):

    logger.info("Getting data from DB ...")

    query = (""" 
                SELECT
                    DISTINCT USR.username
                FROM
                    RestInterface_user USR
                WHERE
                    NOT (
                        USR.username = 'admin'
                        OR USR.username = 'gspuser@singtel.com'
                        OR USR.username = 'projectmanager@singtel.com'
                        OR USR.username = 'executivemanager@singtel.com'
                        OR USR.username = 'productmanager@singtel.com'
                        OR USR.username = 'accountmanager@singtel.com'
                        OR USR.username = 'queueowner@singtel.com'
                        OR USR.username = 'opsmanager'
                        OR USR.username = 'mluser@singtel.com'
                        OR USR.username = 'aljo.ponce@singtel.com'
                        OR USR.username = 'jiangxu@ncs.com.sg'
                        OR USR.username = 'adelinethk@singtel.com'
                        OR USR.username = 'jacob.toh@singtel.com'
                        OR USR.username = 'yuchen.liu@singtel.com'
                        OR USR.username = 'weiwang.thang@singtel.com'
                    )
                    AND USR.team = 'Account Manager'
                    AND DATE(USR.last_login) BETWEEN '{}'
                    AND '{}'
                ORDER BY
                    USR.username; 
            """).format(startDate, endDate)

    result = orionDb.queryToList(query)
    return result

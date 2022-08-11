import os
import sys
import logging.config
import logging
import pandas as pd
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


def createTempTable():
    query = """ 
                CREATE TEMPORARY TABLE COM_QUEUES
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
                    PRD.network_product_code,
                    ORD.order_type,
                    ORD.order_code,
                    PER.role,
                    ACT.name,
                    ACT.status,
                    ACT.completed_date,
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
                    AND '2022-07-31'
                ORDER BY
                    Service,
                    ORD.order_type DESC,
                    PER.role,
                    ORD.order_code,
                    ACT.name;
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
                    ACT.name,
                    ACT.status,
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
                    (
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
                                            OR ACT.name = 'Circuit creation'
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
                                            OR ACT.name = 'Node & Cct Del (DN-ISDN)'
                                        )
                                    )
                                )
                            )
                            OR (
                                PRD.network_product_code LIKE 'DME%'
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
                                            OR ACT.name = 'Circuit creation'
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
                                                AND ACT.name = 'Circuit creation'
                                            )
                                        )
                                    )
                                    OR (
                                        ORD.order_type = 'Cease'
                                        AND (
                                            PER.role = 'GSDT31'
                                            AND ACT.name = 'GSDT Co-ordination Work'
                                            AND ACT.status = 'COM'
                                            AND ACT.completed_date BETWEEN '2022-07-01'
                                            AND '2022-07-31'
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
                    PER.role,
                    ORD.order_code;
            """

    result = orionDb.queryToList(query)
    return pd.DataFrame(data=result, columns=const.DRAFT_COLUMNS)

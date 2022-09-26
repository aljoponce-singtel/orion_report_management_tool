import os
import sys
import logging.config
import logging
import pandas as pd
import numpy as np
import constants as const
from sqlalchemy import select, case, and_, or_, null, func
from sqlalchemy.types import Integer
from scripts import utils
from scripts.DBConnection import DBConnection
from scripts.EmailClient import EmailClient

logger = logging.getLogger(__name__)
defaultConfig = None
emailConfig = None
dbConfig = None
debugConfig = None
csvFiles = []
reportsFolderPath = None
orionDb = None


def initialize(config):
    global defaultConfig, emailConfig, dbConfig, debugConfig, reportsFolderPath, orionDb
    defaultConfig = config['DEFAULT']
    emailConfig = config[defaultConfig['EmailInfo']]
    dbConfig = config[defaultConfig['DatabaseEnv']]
    reportsFolderPath = os.path.join(
        os.getcwd(), defaultConfig['ReportsFolder'])
    debugConfig = config['DEBUG']

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
    if debugConfig.getboolean('SendEmail'):
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


def generateWarRoomReport(fileName, startDate, endDate, emailSubject):

    # query = "SELECT COUNT(id) FROM RestInterface_person WHERE role = 'GIP_KR'"

    person_table = orionDb.getTableMetadata('RestInterface_person')
    query = select([func.count()]).select_from(
        person_table).where(person_table.c.role == 'GIP_KR')

    result = orionDb.queryToList(query)
    df = pd.DataFrame(data=result)

    print(df)

    return

    query = (""" 
                SELECT DISTINCT
                    ORD.id,
                    ORD.order_code,
                    ORD.order_type,
                    ORD.order_status,
                    ORD.order_priority,
                    ORD.business_sector,
                    ORD.current_crd,
                    ORD.initial_crd,
                    ORD.taken_date,
                    ORD.sde_received_date,
                    ORD.arbor_service_type,
                    ORD.service_number,
                    ORD.service_type,
                    CUS.name,
                    ORD.assignee,
                    PRJ.project_code,
                    ORD.circuit_id,
                    ORD.product_description,
                    ACT.name,
                    ACT.due_date,
                    ACT.status Act_Status,
                    ACT.predecessor_list,
                    ACT.activity_code,
                    ACT.ready_date,
                    ACT.completed_date,
                    PER.role,
                    CKT.circuit_code,
                    PAR.parameter_name,
                    PAR.parameter_value
                FROM
                    RestInterface_order ORD
                    JOIN RestInterface_activity ACT ON ACT.order_id = ORD.id
                    LEFT JOIN RestInterface_project PRJ ON ORD.project_id = PRJ.id
                    LEFT JOIN RestInterface_circuit CKT ON ORD.circuit_id = CKT.id
                    LEFT JOIN RestInterface_person PER ON PER.id = ACT.id
                    LEFT JOIN RestInterface_customer CUS ON CUS.id = ORD.customer_id
                    LEFT JOIN RestInterface_npp NPP ON NPP.order_id = ORD.id
                    AND NPP.level = 'Mainline'
                    AND NPP.status != 'Cancel'
                    LEFT JOIN RestInterface_parameter PAR ON PAR.npp_id = NPP.id
                    AND PAR.parameter_name = 'Type'
                    AND PAR.parameter_value IN ('1', '2', '010', '020')
                WHERE
                    ORD.order_status IN (
                        'Submitted',
                        'PONR',
                        'Pending Cancellation',
                        'Completed'
                    )
                    AND ORD.current_crd <= DATE_ADD(now(), INTERVAL 3 MONTH)
                    AND ACT.tag_name = 'Pegasus';
            """)

    result = orionDb.queryToList(query)
    df = pd.DataFrame(data=result)

    # Write to CSV
    csvFiles = []
    csvFile = ("{}_{}.csv").format(fileName, utils.getCurrentDateTime2())

    if debugConfig.getboolean('CreateReport') != False:
        logger.info("Generating report " + csvFile + " ...")
        csvFiles.append(csvFile)
        csvfilePath = os.path.join(reportsFolderPath, csvFile)
        utils.dataframeToCsv(df, csvfilePath)

    return


def generateWarRoomNonGovReport(fileName, startDate, endDate, emailSubject):

    return

    df_order_id = getTransportOrders(startDate, endDate)
    df = pd.DataFrame(getTransportRecords(
        df_order_id['order_id'].to_list(), startDate, endDate))
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

    if debugConfig.getboolean('CreateReport') != False:
        logger.info("Generating report " + csvFile + " ...")
        csvFiles.append(csvFile)
        csvfilePath = os.path.join(reportsFolderPath, csvFile)
        utils.dataframeToCsv(df_finalReport, csvfilePath)

        # Compress files and send email
        if csvFiles:
            attachement = None
            if debugConfig.getboolean('CompressFiles'):
                zipFile = ("{}_{}.zip").format(
                    fileName, utils.getCurrentDateTime2())
                utils.zipFile(csvFiles, zipFile, reportsFolderPath,
                              defaultConfig['ZipPassword'])
                attachement = zipFile
            else:
                attachement = csvFile
            sendEmail(emailSubject, attachement)

    logger.info("Processing [" + emailSubject + "] complete")


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


def getTransportOrders(startDate, endDate):

    # store variables to upper_case variables for better readability in the query
    START_DATE = startDate
    END_DATE = endDate

    order_table = orionDb.getTableMetadata('RestInterface_order', 'ord')
    activity_table = orionDb.getTableMetadata('RestInterface_activity', 'act')
    person_table = orionDb.getTableMetadata('RestInterface_person', 'per')
    npp_table = orionDb.getTableMetadata('RestInterface_npp', 'npp')
    product_table = orionDb.getTableMetadata('RestInterface_product', 'prd')

    # see gsp_transport/sql/getTransportOrders.sql for the raw MySQL query
    query = (
        select([order_table.c.id])
        .distinct()
        .select_from(order_table)
        .join(activity_table,
              activity_table.c.order_id == order_table.c.id)
        .outerjoin(person_table,
                   person_table.c.id == activity_table.c.person_id)
        .outerjoin(npp_table,
                   and_(
                       npp_table.c.order_id == order_table.c.id,
                       npp_table.c.level == 'Mainline',
                       npp_table.c.status != 'Cancel'
                   ))
        .outerjoin(product_table,
                   product_table.c.id == npp_table.c.product_id)
        .where(
            and_(
                or_(
                    and_(
                        product_table.c.network_product_code.like('DGN%'),
                        order_table.c.order_type.in_(
                            ['Provide', 'Change', 'Cease']),
                        or_(
                            person_table.c.role.like('ODC_%'),
                            person_table.c.role.like('RDC_%'),
                            person_table.c.role.like('GSPSG_%')
                        ),
                        activity_table.c.name == 'GSDT Co-ordination Wrk-BQ',
                        activity_table.c.status == 'COM'
                    ).self_group(),
                    and_(
                        product_table.c.network_product_code.like('DME%'),
                        order_table.c.order_type.in_(
                            ['Provide', 'Change', 'Cease']),
                        or_(
                            person_table.c.role.like('ODC_%'),
                            person_table.c.role.like('RDC_%'),
                            person_table.c.role.like('GSPSG_%')
                        ),
                        activity_table.c.name == 'GSDT Co-ordination Wrk-BQ',
                        activity_table.c.status == 'COM'
                    ).self_group(),
                    and_(
                        product_table.c.network_product_code == 'ELK0052',
                        or_(
                            and_(
                                order_table.c.order_type.in_(
                                    ['Provide', 'Change']),
                                or_(
                                    person_table.c.role.like('ODC_%'),
                                    person_table.c.role.like('RDC_%'),
                                    person_table.c.role.like('GSPSG_%'),
                                ),
                                activity_table.c.name == 'Circuit Creation',
                                activity_table.c.status == 'COM'
                            ).self_group(),
                            and_(
                                order_table.c.order_type == 'Cease',
                                or_(
                                    person_table.c.role.like('ODC_%'),
                                    person_table.c.role.like('RDC_%'),
                                    person_table.c.role.like('GSPSG_%'),
                                ),
                                activity_table.c.name == 'Node & Circuit Deletion',
                                activity_table.c.status == 'COM'
                            ).self_group()
                        )
                    ).self_group(),
                    and_(
                        or_(
                            and_(
                                order_table.c.order_type == 'Provide',
                                person_table.c.role == 'GSP_LTC_GW',
                                activity_table.c.name == 'GSDT Co-ordination Work',
                                activity_table.c.status == 'COM'
                            ).self_group(),
                            and_(
                                order_table.c.order_type == 'Cease',
                                person_table.c.role == 'GSDT31',
                                activity_table.c.name == 'GSDT Co-ordination Work',
                                activity_table.c.status == 'COM'
                            ).self_group(),
                        )
                    ).self_group()
                ),
                activity_table.c.completed_date.between(START_DATE, END_DATE)
            )
        )
    )

    orionDb.logFullQuery(query)

    result = orionDb.queryToList(query)
    return pd.DataFrame(data=result, columns=['order_id'])


def getTransportRecords(order_id_list, startDate, endDate):

    # store variables to upper_case variables for better readability in the query
    ORDER_ID_LIST = order_id_list
    START_DATE = startDate
    END_DATE = endDate

    order_table = orionDb.getTableMetadata('RestInterface_order', 'ord')
    activity_table = orionDb.getTableMetadata('RestInterface_activity', 'act')
    person_table = orionDb.getTableMetadata('RestInterface_person', 'per')
    npp_table = orionDb.getTableMetadata('RestInterface_npp', 'npp')
    product_table = orionDb.getTableMetadata('RestInterface_product', 'prd')

    # see gsp_transport/sql/getTransportRecords.sql for the raw MySQL query
    query = (
        select([
            case(
                (product_table.c.network_product_code.like('DGN%'), 'Diginet'),
                (product_table.c.network_product_code.like('DME%'), 'MetroE'),
                (product_table.c.network_product_code == 'ELK0052', 'MegaPop (CE)'),
                (product_table.c.network_product_code.like('GGW%'), 'Gigawave'),
                else_=null()
            ).label('service'),
            order_table.c.order_code,
            order_table.c.current_crd,
            order_table.c.service_number,
            order_table.c.order_status,
            order_table.c.order_type,
            product_table.c.network_product_code,
            person_table.c.role,
            activity_table.c.activity_code.cast(Integer).label('step_no'),
            activity_table.c.name,
            activity_table.c.status,
            activity_table.c.due_date,
            activity_table.c.completed_date
        ])
        .distinct()
        .select_from(order_table)
        .join(activity_table,
              activity_table.c.order_id == order_table.c.id)
        .outerjoin(person_table,
                   person_table.c.id == activity_table.c.person_id)
        .outerjoin(npp_table,
                   and_(
                       npp_table.c.order_id == order_table.c.id,
                       npp_table.c.level == 'Mainline',
                       npp_table.c.status != 'Cancel'
                   ))
        .outerjoin(product_table,
                   product_table.c.id == npp_table.c.product_id)
        .where(
            and_(
                order_table.c.id.in_(ORDER_ID_LIST),
                or_(
                    and_(
                        product_table.c.network_product_code.like('DGN%'),
                        or_(
                            and_(
                                order_table.c.order_type.in_(
                                    ['Provide', 'Change']),
                                or_(
                                    person_table.c.role.like('ODC_%'),
                                    person_table.c.role.like('RDC_%'),
                                    person_table.c.role.like('GSPSG_%')
                                ),
                                and_(
                                    or_(
                                        and_(
                                            activity_table.c.name == 'GSDT Co-ordination Wrk-BQ',
                                            activity_table.c.status == 'COM',
                                            activity_table.c.completed_date.between(
                                                START_DATE, END_DATE)
                                        ).self_group(),
                                        activity_table.c.name == 'Circuit Creation'
                                    )
                                )
                            ),
                            and_(
                                order_table.c.order_type == 'Cease',
                                or_(
                                    person_table.c.role.like('ODC_%'),
                                    person_table.c.role.like('RDC_%'),
                                    person_table.c.role.like('GSPSG_%')
                                ),
                                and_(
                                    or_(
                                        and_(
                                            activity_table.c.name == 'GSDT Co-ordination Wrk-BQ',
                                            activity_table.c.status == 'COM',
                                            activity_table.c.completed_date.between(
                                                START_DATE, END_DATE)
                                        ).self_group(),
                                        activity_table.c.name.in_(
                                            ['Node & Cct Del (DN-ISDN)', 'Node & Cct Deletion (DN)'])
                                    )
                                )
                            ).self_group()
                        )
                    ).self_group(),
                    and_(
                        product_table.c.network_product_code.like('DME%'),
                        or_(
                            and_(
                                order_table.c.order_type == 'Provide',
                                or_(
                                    person_table.c.role.like('ODC_%'),
                                    person_table.c.role.like('RDC_%'),
                                    person_table.c.role.like('GSPSG_%')
                                ),
                                and_(
                                    or_(
                                        and_(
                                            activity_table.c.name == 'GSDT Co-ordination Wrk-BQ',
                                            activity_table.c.status == 'COM',
                                            activity_table.c.completed_date.between(
                                                START_DATE, END_DATE)
                                        ).self_group(),
                                        activity_table.c.name == 'Circuit Creation'
                                    )
                                )
                            ).self_group(),
                            and_(
                                order_table.c.order_type == 'Change',
                                or_(
                                    and_(
                                        or_(
                                            person_table.c.role.like('ODC_%'),
                                            person_table.c.role.like('RDC_%'),
                                            person_table.c.role.like('GSPSG_%')
                                        ),
                                        activity_table.c.name == 'GSDT Co-ordination Wrk-BQ',
                                        activity_table.c.status == 'COM',
                                        activity_table.c.completed_date.between(
                                            START_DATE, END_DATE),
                                    ).self_group(),
                                    and_(
                                        or_(
                                            person_table.c.role.like('ODC_%'),
                                            person_table.c.role.like('RDC_%'),
                                            person_table.c.role.like('GSP_%')
                                        ),
                                        activity_table.c.name.in_(
                                            ['Circuit Creation', 'Change Speed Configure'])
                                    ).self_group()
                                )
                            ).self_group(),
                            and_(
                                order_table.c.order_type == 'Cease',
                                or_(
                                    person_table.c.role.like('ODC_%'),
                                    person_table.c.role.like('RDC_%'),
                                    person_table.c.role.like('GSPSG_%')
                                ),
                                and_(
                                    or_(
                                        and_(
                                            activity_table.c.name == 'GSDT Co-ordination Wrk-BQ',
                                            activity_table.c.status == 'COM',
                                            activity_table.c.completed_date.between(
                                                START_DATE, END_DATE)
                                        ).self_group(),
                                        activity_table.c.name == 'Node & Circuit Deletion'
                                    )
                                )
                            ).self_group()
                        )
                    ).self_group(),
                    and_(
                        product_table.c.network_product_code == 'ELK0052',
                        or_(
                            and_(
                                order_table.c.order_type.in_(
                                    ['Provide', 'Change']),
                                or_(
                                    person_table.c.role.like('ODC_%'),
                                    person_table.c.role.like('RDC_%'),
                                    person_table.c.role.like('GSPSG_%')
                                ),
                                activity_table.c.name == 'Circuit Creation',
                                activity_table.c.status == 'COM',
                                activity_table.c.completed_date.between(
                                    START_DATE, END_DATE)
                            ).self_group(),
                            and_(
                                order_table.c.order_type == 'Cease',
                                or_(
                                    person_table.c.role.like('ODC_%'),
                                    person_table.c.role.like('RDC_%'),
                                    person_table.c.role.like('GSPSG_%')
                                ),
                                activity_table.c.name == 'Node & Circuit Deletion',
                                activity_table.c.status == 'COM',
                                activity_table.c.completed_date.between(
                                    START_DATE, END_DATE)
                            ).self_group()
                        )
                    ).self_group(),
                    and_(
                        product_table.c.network_product_code.like('GGW%'),
                        or_(
                            and_(
                                order_table.c.order_type == 'Provide',
                                or_(
                                    and_(
                                        person_table.c.role == 'GSP_LTC_GW',
                                        activity_table.c.name == 'GSDT Co-ordination Work',
                                        activity_table.c.status == 'COM',
                                        activity_table.c.completed_date.between(
                                            START_DATE, END_DATE)
                                    ).self_group(),
                                    and_(
                                        or_(
                                            person_table.c.role.like('ODC_%'),
                                            person_table.c.role.like('RDC_%'),
                                            person_table.c.role.like('GSPSG_%')
                                        ),
                                        activity_table.c.name == 'Circuit Creation',
                                    )
                                )
                            ).self_group(),
                            and_(
                                order_table.c.order_type == 'Cease',
                                or_(
                                    and_(
                                        person_table.c.role == 'GSDT31',
                                        activity_table.c.name == 'GSDT Co-ordination Work',
                                        activity_table.c.status == 'COM',
                                        activity_table.c.completed_date.between(
                                            START_DATE, END_DATE)
                                    ).self_group(),
                                    and_(
                                        person_table.c.role == 'GSP_LTC_GW',
                                        activity_table.c.name == 'Circuit Removal from NMS',
                                    ).self_group()
                                )
                            ).self_group()
                        )
                    ).self_group()
                )
            )
        )
        .order_by(
            'service',
            order_table.c.order_type.desc(),
            activity_table.c.name,
            'step_no',
            person_table.c.role,
            order_table.c.order_code
        )
    )

    orionDb.logFullQuery(query)

    result = orionDb.queryToList(query)
    return pd.DataFrame(data=result, columns=const.DRAFT_COLUMNS)

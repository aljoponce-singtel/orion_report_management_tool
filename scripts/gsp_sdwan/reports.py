from scripts import utils
import constants as const
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


def initialize(config):
    global defaultConfig, emailConfig, dbConfig, reportsFolderPath, orionDb
    defaultConfig = config['DEFAULT']
    emailConfig = config[defaultConfig['EmailInfo']]
    dbConfig = config[defaultConfig['DatabaseEnv']]
    reportsFolderPath = os.path.join(
        os.getcwd(), defaultConfig['ReportsFolder'])

    orionDb = DBConnection(dbConfig['dbapi'], dbConfig['host'], dbConfig['port'],
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
            raise Exception(e)


def generateSDWANReport(zipFileName, startDate, endDate, emailSubject):

    logger.info('********************')
    logger.info("Processing [" + emailSubject + "] ...")

    columns = [
        'OrderCode',
        'NetworkProductCode',
        'GroupID',
        'CRD',
        'TakenDate',
        'OrderType',
        'ServiceNumber',
        'ProjectCode',
        'CustomerName',
        'AEndAddress',
        'FamilyName',
        'GivenName',
        'ContactType',
        'EmailAddress',
        'ParameterName',
        'ParameterValue'
    ]

    orderTypes = ['Provide', 'Change']

    contactTypes = ['AM',
                    'SDE',
                    'Project Manager',
                    'A-end-Cust']

    productCodes = ['SDW0002',
                    'SDW0024',
                    'SDW0025',
                    'SDW0026',
                    'CNP0213']

    parameters = ['CircuitRef1',
                  'CircuitRef2',
                  'CircuitRef3',
                  'CircuitRef4',
                  'CustCircuitTy1',
                  'CustCircuitTy2',
                  'CustCircuitTy3',
                  'CustCircuitTy4',
                  'MainEquipModel',
                  'OriginCtry',
                  'OriginCity',
                  'EquipmentVendorPONo',
                  'EquipmentVendor',
                  'InstallationPartnerPONo',
                  'InstallationPartner',
                  'SIInstallPartner',
                  'SIMaintPartner',
                  'CSDWSIInstall',
                  'CSDWSIMaint',
                  'MainSLA']

    sqlquery = ("""
                    SELECT
                        DISTINCT ORD.order_code AS OrderCode,
                        PRD.network_product_code AS NetworkProductCode,
                        PER.role AS GroupID,
                        ORD.current_crd AS CRD,
                        ORD.taken_date AS TakenDate,
                        ORD.order_type AS OrderType,
                        ORD.service_number AS ServiceNumber,
                        PRJ.project_code AS ProjectCode,
                        CUS.name AS CustomerName,
                        SITE.location AS AEndAddress,
                        CON.given_name AS FamilyName,
                        CON.family_name AS GivenName,
                        CON.contact_type AS ContactType,
                        CON.email_address AS EmailAddress,
                        PAR.parameter_name AS ParameterName,
                        PAR.parameter_value AS ParameterValue
                    FROM
                        RestInterface_order ORD
                        JOIN RestInterface_activity ACT ON ORD.id = ACT.order_id
                        LEFT JOIN RestInterface_person PER ON ACT.person_id = PER.id
                        LEFT JOIN RestInterface_project PRJ ON ORD.project_id = PRJ.id
                        LEFT JOIN RestInterface_customer CUS ON ORD.customer_id = CUS.id
                        LEFT JOIN RestInterface_site SITE ON ORD.site_id = SITE.id
                        LEFT JOIN RestInterface_contactdetails CON ON ORD.id = CON.order_id
                        AND CON.contact_type IN ({})
                        LEFT JOIN RestInterface_npp NPP ON ORD.id = NPP.order_id
                        AND NPP.level = 'Mainline'
                        AND NPP.status <> 'Cancel'
                        LEFT JOIN RestInterface_parameter PAR ON NPP.id = PAR.npp_id
                        AND PAR.parameter_name IN ({})
                        LEFT JOIN RestInterface_product PRD ON NPP.product_id = PRD.id
                    WHERE
                        ORD.order_type IN ({})
                        AND PRD.network_product_code IN ({})
                        AND ORD.taken_date BETWEEN '{}'
                        AND '{}'
                    ORDER BY
                        OrderCode,
                        ContactType,
                        ParameterName;
                """).format(utils.listToString(contactTypes), utils.listToString(parameters), utils.listToString(orderTypes), utils.listToString(productCodes), startDate, endDate)

    csvFile = ("{}_{}.csv").format('SDWAN', utils.getCurrentDateTime())
    df_report = process(orionDb.queryToList(sqlquery), columns)

    # Write to CSV
    csvFiles = []
    csvFile = ("{}_{}.csv").format('SDWAN', utils.getCurrentDateTime())
    csvFiles.append(csvFile)
    csvfilePath = os.path.join(reportsFolderPath, csvFile)
    utils.dataframeToCsv(df_report, csvfilePath)

    if csvFiles:
        attachement = None
        if defaultConfig.getboolean('CompressFiles'):
            zipFile = ("{}_{}.zip").format(
                zipFileName, utils.getCurrentDateTime())
            utils.zipFile(csvFiles, zipFile, reportsFolderPath,
                          defaultConfig['ZipPassword'])
            attachement = zipFile
        else:
            attachement = csvFile
        sendEmail(emailSubject, attachement)

    logger.info("Processing [" + emailSubject + "] complete")


def process(queryList, columns):

    df_report = pd.DataFrame(columns=const.REPORT_COLUMNS)
    df = pd.DataFrame(queryList, columns=columns)

    for order in df['OrderCode'].unique():
        # add new data (df_toAdd) to df_report
        df_order = df[df['OrderCode'] == order]
        df_toAdd = processUniqueOrders(df_order, const.REPORT_COLUMNS) if defaultConfig.getboolean(
            'ProcessUniqueOrders') else processDuplicateOrders(df_order, const.REPORT_COLUMNS)
        df_report = pd.concat([df_report, df_toAdd])

    return df_report


def processDuplicateOrders(df_order, reportColumns):
    df_order = pd.DataFrame(df_order)

    # Add values to columns
    orderCode = dfUniqueValue(df_order['OrderCode'])
    productCode = dfUniqueValue(df_order['NetworkProductCode'])
    crd = dfUniqueValue(df_order['CRD'])
    takenDate = dfUniqueValue(df_order['TakenDate'])
    serviceNumber = dfUniqueValue(df_order['ServiceNumber'])
    projectCode = dfUniqueValue(df_order['ProjectCode'])
    customerName = dfUniqueValue(df_order['CustomerName'])
    aEndAddress = dfUniqueValue(df_order['AEndAddress'])
    circuitRef1 = dfParameterValue(df_order, 'CircuitRef1')
    circuitRef2 = dfParameterValue(df_order, 'CircuitRef2')
    circuitRef3 = dfParameterValue(df_order, 'CircuitRef3')
    circuitRef4 = dfParameterValue(df_order, 'CircuitRef4')
    custCircuitTy1 = dfParameterValue(df_order, 'CustCircuitTy1')
    custCircuitTy2 = dfParameterValue(df_order, 'CustCircuitTy2')
    custCircuitTy3 = dfParameterValue(df_order, 'CustCircuitTy3')
    custCircuitTy4 = dfParameterValue(df_order, 'CustCircuitTy4')
    mainEquipModel = dfParameterValue(df_order, 'MainEquipModel')
    originCtry = dfParameterValue(df_order, 'OriginCtry')
    originCity = dfParameterValue(df_order, 'OriginCity')
    equipmentVendorPONo = dfParameterValue(df_order, 'EquipmentVendorPONo')
    equipmentVendor = dfParameterValue(df_order, 'EquipmentVendor')
    installationPartnerPONo = dfParameterValue(
        df_order, 'InstallationPartnerPONo')
    installationPartner = dfParameterValue(df_order, 'InstallationPartner')
    sIInstallPartner = dfParameterValue(df_order, 'SIInstallPartner')
    sIMaintPartner = dfParameterValue(df_order, 'SIMaintPartner')
    cSDWSIInstall = dfParameterValue(df_order, 'CSDWSIInstall')
    cSDWSIMaint = dfParameterValue(df_order, 'CSDWSIMaint')
    mainSLA = dfParameterValue(df_order, 'MainSLA')

    # Create a dataframe to store multiple group records for the same group ID.
    df_group = pd.DataFrame(columns=[
        'GroupID', 'AM_ContactName', 'AM_ContactEmail', 'SDE_ContactName', 'SDE_ContactEmail', 'PM_ContactName', 'PM_ContactEmail', 'AEndCus_ContactName', 'AEndCus_ContactEmail'])

    # Loop through each group IDs to get the contact information
    group_id_list = df_order['GroupID'].unique().tolist()

    for groupId in group_id_list:

        df_group_toAdd = pd.DataFrame()
        df_group_toAdd['GroupID'] = [groupId]

        # Add contact information to group for each contact type
        df_group_toAdd = createDfGroup(
            df_order, df_group_toAdd, groupId, 'AM', 'AM_ContactName', 'AM_ContactEmail')
        df_group_toAdd = createDfGroup(
            df_order, df_group_toAdd, groupId, 'SDE', 'SDE_ContactName', 'SDE_ContactEmail')
        df_group_toAdd = createDfGroup(
            df_order, df_group_toAdd, groupId, 'Project Manager', 'PM_ContactName', 'PM_ContactEmail')
        df_group_toAdd = createDfGroup(
            df_order, df_group_toAdd, groupId, 'A-end-Cust', 'AEndCus_ContactName', 'AEndCus_ContactEmail')

        # Add the merged contact details (df_group_toAdd) to df_group
        df_group = pd.concat(
            [df_group, df_group_toAdd], ignore_index=True)

    df_order_report = pd.DataFrame(columns=reportColumns)

    for ind in df_group.index:

        reportData = [
            orderCode,
            productCode,
            df_group['GroupID'][ind],
            crd,
            takenDate,
            serviceNumber,
            projectCode,
            customerName,
            aEndAddress,
            df_group['AM_ContactName'][ind],
            df_group['AM_ContactEmail'][ind],
            df_group['SDE_ContactName'][ind],
            df_group['SDE_ContactEmail'][ind],
            df_group['PM_ContactName'][ind],
            df_group['PM_ContactEmail'][ind],
            df_group['AEndCus_ContactName'][ind],
            df_group['AEndCus_ContactEmail'][ind],
            circuitRef1,
            circuitRef2,
            circuitRef3,
            circuitRef4,
            custCircuitTy1,
            custCircuitTy2,
            custCircuitTy3,
            custCircuitTy4,
            mainEquipModel,
            originCtry,
            originCity,
            equipmentVendorPONo,
            equipmentVendor,
            installationPartnerPONo,
            installationPartner,
            sIInstallPartner,
            sIMaintPartner,
            cSDWSIInstall,
            cSDWSIMaint,
            mainSLA
        ]

        # add new data (df_order_report_toAdd) to df_order_report
        df_order_report_toAdd = pd.DataFrame(
            data=[reportData], columns=reportColumns)
        df_order_report = pd.concat([df_order_report, df_order_report_toAdd])

    return df_order_report


# Creates a dataframe to store multiple contact information for the same contact type
def createDfGroup(df_order, df_group_toAdd, groupId, contactType, contactNameCol, contactEmailCol):

    df_order = pd.DataFrame(df_order)
    df_contact = df_order[df_order['ContactType'] == contactType][[
        'FamilyName', 'GivenName', 'EmailAddress']].drop_duplicates()

    # Flag to handle multiple contact information for the same contact type
    isMultiContacts = False
    df_temp = None

    for ind in df_contact.index:
        # Get tje contact name and email
        contactName, contactEmail = dfContactNameEmail(
            df_contact, ind)
        df_contacts_toAdd = pd.DataFrame(data=[[groupId, contactName, contactEmail]], columns=[
            'GroupID', contactNameCol, contactEmailCol])
        df_merged = df_group_toAdd

        if not isMultiContacts:
            df_merged = pd.merge(df_merged, df_contacts_toAdd, on='GroupID')
            df_temp = df_group_toAdd
        else:
            df_temp = pd.merge(
                df_temp, df_contacts_toAdd, on='GroupID')
            df_merged = pd.concat([df_merged, df_temp], ignore_index=True)

        df_group_toAdd = pd.DataFrame()
        df_group_toAdd = pd.concat(
            [df_group_toAdd, df_merged], ignore_index=True)
        isMultiContacts = True

    return df_group_toAdd


def processUniqueOrders(df_order, reportColumns):

    # Add values to columns
    orderCode = dfUniqueValue(df_order['OrderCode'])
    productCode = dfUniqueValue(df_order['NetworkProductCode'])
    groupID = dfValuesToList(df_order['GroupID'])
    crd = dfUniqueValue(df_order['CRD'])
    takenDate = dfUniqueValue(df_order['TakenDate'])
    serviceNumber = dfUniqueValue(df_order['ServiceNumber'])
    projectCode = dfUniqueValue(df_order['ProjectCode'])
    customerName = dfUniqueValue(df_order['CustomerName'])
    aEndAddress = dfUniqueValue(df_order['AEndAddress'])
    amContactName, amContactEmail = dfContactInformation(df_order, 'AM')
    sdeContactName, sdeContactEmail = dfContactInformation(df_order, 'SDE')
    pmContactName, pmContactEmail = dfContactInformation(
        df_order, 'Project Manager')
    aEndCusContactName, aEndCusContactEmail = dfContactInformation(
        df_order, 'A-end-Cust')
    circuitRef1 = dfParameterValue(df_order, 'CircuitRef1')
    circuitRef2 = dfParameterValue(df_order, 'CircuitRef2')
    circuitRef3 = dfParameterValue(df_order, 'CircuitRef3')
    circuitRef4 = dfParameterValue(df_order, 'CircuitRef4')
    custCircuitTy1 = dfParameterValue(df_order, 'CustCircuitTy1')
    custCircuitTy2 = dfParameterValue(df_order, 'CustCircuitTy2')
    custCircuitTy3 = dfParameterValue(df_order, 'CustCircuitTy3')
    custCircuitTy4 = dfParameterValue(df_order, 'CustCircuitTy4')
    mainEquipModel = dfParameterValue(df_order, 'MainEquipModel')
    originCtry = dfParameterValue(df_order, 'OriginCtry')
    originCity = dfParameterValue(df_order, 'OriginCity')
    equipmentVendorPONo = dfParameterValue(df_order, 'EquipmentVendorPONo')
    equipmentVendor = dfParameterValue(df_order, 'EquipmentVendor')
    installationPartnerPONo = dfParameterValue(
        df_order, 'InstallationPartnerPONo')
    installationPartner = dfParameterValue(df_order, 'InstallationPartner')
    sIInstallPartner = dfParameterValue(df_order, 'SIInstallPartner')
    sIMaintPartner = dfParameterValue(df_order, 'SIMaintPartner')
    cSDWSIInstall = dfParameterValue(df_order, 'CSDWSIInstall')
    cSDWSIMaint = dfParameterValue(df_order, 'CSDWSIMaint')
    mainSLA = dfParameterValue(df_order, 'MainSLA')

    reportData = [
        orderCode,
        productCode,
        groupID,
        crd,
        takenDate,
        serviceNumber,
        projectCode,
        customerName,
        aEndAddress,
        amContactName,
        amContactEmail,
        sdeContactName,
        sdeContactEmail,
        pmContactName,
        pmContactEmail,
        aEndCusContactName,
        aEndCusContactEmail,
        circuitRef1,
        circuitRef2,
        circuitRef3,
        circuitRef4,
        custCircuitTy1,
        custCircuitTy2,
        custCircuitTy3,
        custCircuitTy4,
        mainEquipModel,
        originCtry,
        originCity,
        equipmentVendorPONo,
        equipmentVendor,
        installationPartnerPONo,
        installationPartner,
        sIInstallPartner,
        sIMaintPartner,
        cSDWSIInstall,
        cSDWSIMaint,
        mainSLA
    ]

    # add new data (df_toAdd) to df_report
    df_toAdd = pd.DataFrame(data=[reportData], columns=reportColumns)

    return df_toAdd


def dfValuesToList(df):
    list = df.unique().tolist()
    list.sort()
    return('; '.join(filter(None, list)))


def dfUniqueValue(df):
    if df.empty:
        return None
    else:
        return df.unique()[0]


def dfContactNameEmail(df, ind):
    contactName = df['FamilyName'][ind] + ', ' + df['GivenName'][ind]
    contactEmail = df['EmailAddress'][ind]

    return contactName, contactEmail


def dfContactInformation(df, contactType):
    df_contact = df[df['ContactType'] == contactType][[
        'FamilyName', 'GivenName', 'EmailAddress']].drop_duplicates()

    contactNameList = []
    contactEmailList = []

    for ind in df_contact.index:
        contactNameList.append(
            df_contact['FamilyName'][ind] + ', ' + df_contact['GivenName'][ind])
        contactEmailList.append(
            df_contact['EmailAddress'][ind])

    contactNames = '; '.join(filter(None, contactNameList))
    contactEmails = '; '.join(filter(None, contactEmailList))

    return contactNames, contactEmails


def dfParameterValue(df, parameterName):
    df_parameterValue = df[df['ParameterName']
                           == parameterName][['ParameterValue']]

    return dfUniqueValue(df_parameterValue['ParameterValue'])

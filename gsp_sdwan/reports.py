import utils
import configparser
import logging.config
import logging
import sys
import os
import smtplib
import csv
from datetime import datetime
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email import encoders
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import pymysql

logger = logging.getLogger()
config = configparser.ConfigParser()
config.read('gsp_sdwan/config_sdwan.ini')
defaultConfig = config['DEFAULT']
emailConfig = config[defaultConfig['EmailInfo']]
dbConfig = config[defaultConfig['DatabaseEnv']]
engine = None
conn = None
csvFiles = []
reportsFolderPath = os.path.join(os.getcwd(), "reports")
pymysql.install_as_MySQLdb()


def dbConnect():
    global engine, conn

    try:
        engine = create_engine(
            'mysql://{}:{}@{}:{}/{}'.format(dbConfig['orion_user'], dbConfig['orion_pwd'], dbConfig['host'], dbConfig['port'], dbConfig['orion_db']))
        conn = engine.connect()

        # logger.info("Connected to DB " + dbConfig['orion_db'] + ' at ' +
        #                    dbConfig['orion_user'] + '@' + dbConfig['host'] + ':' + dbConfig['port'])

    except Exception as err:
        logger.info("Failed to connect to DB " + dbConfig['orion_db'] + ' at ' +
                    dbConfig['orion_user'] + '@' + dbConfig['host'] + ':' + dbConfig['port'] + '.')
        logger.error(err)
        raise Exception(err)


def dbDisconnect():
    conn.close()


def dbQueryToList(sqlQuery):
    dataset = conn.execute(text(sqlQuery)).fetchall()
    return dataset


def printRecords(records):

    for record in records:
        print(record)


def generateReport(csvfile, querylist, headers):
    logger.info("Generating report " + csvfile + " ...")
    utils.write_to_csv(csvfile, querylist, headers, reportsFolderPath)
    csvFiles.append(csvfile)


def report_attach(zipfile):

    zipfilePath = os.path.join(reportsFolderPath, zipfile)
    part = MIMEBase('application', "octet-stream")
    part.set_payload(open(zipfilePath, "rb").read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition',
                    'attachment; filename="%s"' % os.path.basename(zipfilePath))
    return part


def setEmailSubject(subject):
    today_datetime = datetime.now()
    day = today_datetime.strftime('%d').lstrip('0')
    hour = today_datetime.strftime('%I').lstrip('0')
    ampm = today_datetime.strftime('%p').lower()
    year = today_datetime.strftime('%Y')
    month = today_datetime.strftime('%b').lower()
    subject = "[{}] {}{}{} {}{}".format(
        subject, year, month, day, hour, ampm)

    return subject


def sendEmail(subject, attachment, email):
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
            logger.info(
                'Sending email with subject "{}" ...'.format(subject))

            receiverTo = emailConfig["receiverTo"] if defaultConfig[
                'EmailInfo'] == 'EmailTest' else emailConfig["receiverTo"] + ';' + email
            receiverCc = emailConfig["receiverCc"]
            sender = emailConfig["sender"]

            if utils.getPlatform() == 'Windows':
                import win32com.client
                outlook = win32com.client.Dispatch('outlook.application')

                mail = outlook.CreateItem(0)
                mail.To = receiverTo
                mail.CC = receiverCc
                mail.Subject = subject
                mail.HTMLBody = emailBodyhtml
                mail.Body = emailBodyText
                mail.Attachments.Add(os.path.join(
                    reportsFolderPath, attachment))
                mail.Send()
            else:
                # Turn these into plain/html MIMEText objects
                # part1 = MIMEText(emailBodyText, "plain")
                part2 = MIMEText(emailBodyhtml, "html")

                message = MIMEMultipart()
                # message.attach(MIMEText(body,"html"))
                message.attach(report_attach(attachment))

                # Add HTML/plain-text parts to MIMEMultipart message
                # The email client will try to render the last part first
                # message.attach(part1)
                message.attach(part2)
                message['Subject'] = subject
                message['From'] = emailConfig["from"]
                message['To'] = receiverTo
                message['CC'] = receiverCc
                receiver = receiverTo + ";" + receiverCc
                smtpObj = smtplib.SMTP(emailConfig["smtpServer"])
                smtpObj.sendmail(sender, receiver.split(";"),
                                 message.as_string())
                smtpObj.quit()

            # logger.info("Email sent.")

        except Exception as e:
            logger.error("Failed to send email.")
            logger.error(e)


def generateSDWANReport(zipFileName, startDate, endDate, emailSubject, emailTo):

    logger.info("Processing [" + emailSubject + "] ...")

    dbConnect()
    csvFiles.clear()

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

    orderTypes = ', '.join([("'" + item + "'")
                            for item in ['Provide',
                                         'Change']])

    contactTypes = ', '.join([("'" + item + "'")
                              for item in ['AM',
                                           'SDE',
                                           'Project Manager',
                                           'A-end-Cust']])

    productCodes = ', '.join([("'" + item + "'")
                              for item in ['SDW0002',
                                           'SDW0024',
                                           'SDW0025',
                                           'SDW0026',
                                           'CNP0213']])

    parameters = ', '.join([("'" + item + "'")
                            for item in ['CircuitRef1',
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
                                         'MainSLA']])

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
                """).format(contactTypes, parameters, orderTypes, productCodes, startDate, endDate)

    csvFile = ("{}_{}.csv").format('SDWAN', utils.getCurrentDateTime())
    outputList, reportColumns = processList(dbQueryToList(sqlquery), columns)
    generateReport(csvFile, outputList, reportColumns)
    dbDisconnect()

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
        sendEmail(setEmailSubject(emailSubject), attachement, emailTo)

    logger.info("Processing [" + emailSubject + "] complete")


def processList(queryList, columns):

    reportColumns = ['OrderCode',
                     'NetworkProductCode',
                     'GroupID',
                     'CRD',
                     'TakenDate',
                     'ServiceNumber',
                     'ProjectCode',
                     'CustomerName',
                     'AEndAddress',
                     'AM_ContactName',
                     'AM_ContactEmail',
                     'SDE_ContactName',
                     'SDE_ContactEmail',
                     'PM_ContactName',
                     'PM_ContactEmail',
                     'AEndCus_ContactName',
                     'AEndCus_ContactEmail',
                     'CircuitRef1',
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
                     'MainSLA'
                     ]

    df_report = pd.DataFrame(columns=reportColumns)
    df = pd.DataFrame(queryList, columns=columns)

    for order in df['OrderCode'].unique():
        # add new data (df_toAdd) to df_report
        df_order = df[df['OrderCode'] == order]
        df_toAdd = processUniqueOrders(df_order, reportColumns) if defaultConfig.getboolean(
            'ProcessUniqueOrders') else processDuplicateOrders(df_order, reportColumns)
        df_report = pd.concat([df_report, df_toAdd])

    return df_report.values.tolist(), reportColumns


def processDuplicateOrders(df_order, reportColumns):
    # Add values to columns
    orderCode = dfUniqueValue(df_order['OrderCode'])
    productCode = dfUniqueValue(df_order['NetworkProductCode'])
    # groupID = dfValuesToList(df_order['GroupID'])
    crd = dfUniqueValue(df_order['CRD'])
    takenDate = dfUniqueValue(df_order['TakenDate'])
    serviceNumber = dfUniqueValue(df_order['ServiceNumber'])
    projectCode = dfUniqueValue(df_order['ProjectCode'])
    customerName = dfUniqueValue(df_order['CustomerName'])
    aEndAddress = dfUniqueValue(df_order['AEndAddress'])
    # amContactName, amContactEmail = dfContactInformation(df_order, 'AM')
    # sdeContactName, sdeContactEmail = dfContactInformation(df_order, 'SDE')
    # pmContactName, pmContactEmail = dfContactInformation(
    #     df_order, 'Project Manager')
    # aEndCusContactName, aEndCusContactEmail = dfContactInformation(
    #     df_order, 'A-end-Cust')
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

    df_report = pd.DataFrame(columns=reportColumns)

    for groupID in df_order['GroupID'].unique().tolist():

        df_contact = df_order[df_order['ContactType'] == 'AM'][[
            'FamilyName', 'GivenName', 'EmailAddress']].drop_duplicates()
        for ind in df_contact.index:
            amContactName, amContactEmail = dfContactNameEmail(
                df_contact, ind)

            df_contact = df_order[df_order['ContactType'] == 'SDE'][[
                'FamilyName', 'GivenName', 'EmailAddress']].drop_duplicates()
            for ind in df_contact.index:
                sdeContactName, sdeContactEmail = dfContactNameEmail(
                    df_contact, ind)

                df_contact = df_order[df_order['ContactType'] == 'Project Manager'][[
                    'FamilyName', 'GivenName', 'EmailAddress']].drop_duplicates()
                for ind in df_contact.index:
                    pmContactName, pmContactEmail = dfContactNameEmail(
                        df_contact, ind)

                    df_contact = df_order[df_order['ContactType'] == 'A-end-Cust'][[
                        'FamilyName', 'GivenName', 'EmailAddress']].drop_duplicates()
                    for ind in df_contact.index:
                        aEndCusContactName, aEndCusContactEmail = dfContactNameEmail(
                            df_contact, ind)

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
                        df_toAdd = pd.DataFrame(
                            data=[reportData], columns=reportColumns)
                        df_report = pd.concat([df_report, df_toAdd])

    return df_report


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

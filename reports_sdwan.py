import sys
import logging
import re
import csv
import os
import smtplib
import calendar
import configparser
from datetime import datetime, timedelta
from zipfile import ZipFile
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email import encoders
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import pymysql
pymysql.install_as_MySQLdb()

config = configparser.ConfigParser()
config.read('config_sdwan.ini')
defaultConfig = config['DEFAULT']
emailConfig = config[defaultConfig['EmailInfo']]
dbConfig = config[defaultConfig['DatabaseEnv']]
engine = None
conn = None
updateTableau = False
csvFiles = []
reportsFolderPath = os.path.join(os.getcwd(), "reports")
logsFolderPath = os.path.join(os.getcwd(), "logs")

logging.basicConfig(handlers=[logging.FileHandler(filename=os.path.join(logsFolderPath, "reports_sdwan.log"),
                                                  encoding='utf-8', mode='a+')],
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    datefmt="%F %a %T",
                    level=logging.INFO)


def dbConnect():
    global engine, conn

    try:
        engine = create_engine(
            'mysql://{}:{}@{}:{}/{}'.format(dbConfig['orion_user'], dbConfig['orion_pwd'], dbConfig['host'], dbConfig['port'], dbConfig['orion_db']))
        conn = engine.connect()

        # printAndLogMessage("Connected to DB " + dbConfig['orion_db'] + ' at ' +
        #                    dbConfig['orion_user'] + '@' + dbConfig['host'] + ':' + dbConfig['port'])

    except Exception as err:
        printAndLogMessage("Failed to connect to DB " + dbConfig['orion_db'] + ' at ' +
                           dbConfig['orion_user'] + '@' + dbConfig['host'] + ':' + dbConfig['port'] + '.')
        printAndLogError(err)
        raise Exception(err)


def dbDisconnect():
    conn.close()


def dbQueryToList(sqlQuery):
    dataset = conn.execute(text(sqlQuery)).fetchall()
    return dataset


def write_to_csv(csv_file, dataset, headers):

    csvfilePath = os.path.join(reportsFolderPath, csv_file)

    with open(csvfilePath, 'w', newline='') as csvfile:
        spamwriter = csv.writer(
            csvfile,
            delimiter=',',
            quoting=csv.QUOTE_NONNUMERIC)
        spamwriter.writerow(headers)
        spamwriter.writerows(dataset)

        csvFiles.append(csv_file)


def printRecords(records):

    for record in records:
        print(record)


def generateReport(csvfile, querylist, headers):
    printAndLogMessage("Generating report " + csvfile + " ...")
    write_to_csv(csvfile, querylist, headers)


def zip_csvFile(csvFiles, zipfile):
    with ZipFile(zipfile, 'w') as zipObj:
        for csv in csvFiles:
            csvfilePath = csv
            zipObj.write(csvfilePath)
            os.remove(csvfilePath)


def os_zip_csvFile(csvFiles, zipfile):
    csvfiles = ' '.join(csvFiles)
    os.system("zip -e %s %s -P %s" %
              (zipfile, csvfiles, defaultConfig['ZipPassword']))
    for csv in csvFiles:
        os.remove(csv)


def zip_file(csvFiles, zipfile, folderPath):

    printAndLogMessage("Creating " + zipfile + " for " +
                       ', '.join(csvFiles) + ' ...')
    os.chdir(folderPath)

    if getPlatform() == "Linux":
        os_zip_csvFile(csvFiles, zipfile)
    elif getPlatform() == "Windows":
        zip_csvFile(csvFiles, zipfile)
    else:
        raise Exception("OS " + os + " not supported.")

    os.chdir('../')


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
            printAndLogMessage(
                'Sending email with subject "{}" ...'.format(subject))

            receiverTo = emailConfig["receiverTo"] if defaultConfig[
                'EmailInfo'] == 'EmailTest' else emailConfig["receiverTo"] + ';' + email
            receiverCc = emailConfig["receiverCc"]
            sender = emailConfig["sender"]

            if getPlatform() == 'Windows':
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

            # printAndLogMessage("Email sent.")

        except Exception as e:
            printAndLogError("Failed to send email.")
            printAndLogError(e)


def getPlatform():
    platforms = {
        'linux': 'Linux',
        'linux1': 'Linux',
        'linux2': 'Linux',
        'darwin': 'OS X',
        'win32': 'Windows'
    }

    if sys.platform not in platforms:
        return sys.platform

    return platforms[sys.platform]


def printAndLogMessage(message):
    print(message)
    logging.info(message)


def printAndLogError(error):
    print("ERROR: {}".format(error))
    logging.error(error)


def getCurrentDateTime():
    return datetime.now().strftime("%d%m%y_%H%M")


def generateSDWANReport(zipFileName, startDate, endDate, emailSubject, emailTo):

    printAndLogMessage("Processing [" + emailSubject + "] ...")

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
                                           'SDW0026']])

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

    csvFile = ("{}_{}.csv").format('SDWAN', getCurrentDateTime())
    outputList, reportColumns = processList(dbQueryToList(sqlquery), columns)
    generateReport(csvFile, outputList, reportColumns)

    dbDisconnect()

    if csvFiles:
        zipFile = ("{}_{}.zip").format(zipFileName, getCurrentDateTime())
        zip_file(csvFiles, zipFile, reportsFolderPath)
        sendEmail(setEmailSubject(emailSubject), zipFile, emailTo)

    printAndLogMessage("Processing [" + emailSubject + "] complete")


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
            'ProcessUniqueOrders') else None
        df_report = pd.concat([df_report, df_toAdd])

    return df_report.values.tolist(), reportColumns


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


def main():
    printAndLogMessage("==========================================")
    printAndLogMessage("START of script - " +
                       datetime.now().strftime("%a %m/%d/%Y, %H:%M:%S"))
    printAndLogMessage("Running script in " + getPlatform())
    today_date = datetime.now().date()
    global updateTableau

    if defaultConfig.getboolean('GenReportManually'):
        # updateTableau = True
        startDate = '2022-02-14'
        endDate = '2022-02-20'

        generateSDWANReport('sdwan_report', startDate,
                            endDate, "SDWAN Report", '')

    else:

        #-- START --#
        # If the day falls on a Monday
        # start date = date of the previous Monday (T-7)
        # end date = date of the previous Sunday (T-1)

        # TEST DATA
        # today_date = datetime.now().date().replace(day=7)
        # print("date today: " + str(today_date))

        if today_date.isoweekday() == 1:  # Monday
            startDate = str(today_date - timedelta(days=7))
            endDate = str(today_date - timedelta(days=1))
            print("start date: " + str(startDate))
            print("end date: " + str(endDate))

        #-- END --#

    printAndLogMessage("END of script - " +
                       datetime.now().strftime("%a %m/%d/%Y, %H:%M:%S"))


if __name__ == '__main__':
    main()

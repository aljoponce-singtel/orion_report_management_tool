#!/usr/bin/env python3

from string import Template
import mysql.connector
from mysql.connector import errorcode
import smtplib
import base64
import array
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import csv
import gzip
import shutil
import os
from datetime import datetime


def run_query(query_string):
    db = mysql.connector.connect(
        user='o2puserp', password='Qsw123!du', host='172.26.144.143', database='o2pprod')
    # prepare a cursor object using cursor() method
    # cursor = db.cursor(dictionary=True)
    cursor = db.cursor()
    # execute SQL query using execute() method.
    cursor.execute(query_string)
    datas = cursor.fetchall()
    db.close()
    return datas


def write_to_csv_detail(csv_file, dataset):
    header = ['OrderNo', 'CustomerName', 'BRN', 'ProductDescription', 'OrderType', 'OrderActionType', 'OrderStatus', 'Assignee', 'ServiceActionType',
              'ServiceType', 'Sector', 'InitialCRD', 'CloseDate', 'CommissionDate', 'OrdCreationDate', 'ProjectID', 'ContactType', 'FirstName',
              'LastName', 'WorkPhoneNo', 'MobileNo', 'Email', 'Address']
    with open(csv_file, 'w', newline='') as csvfile:
        spamwriter = csv.writer(csvfile, delimiter=',',
                                quoting=csv.QUOTE_NONNUMERIC)
        spamwriter.writerow(header)
        spamwriter.writerows(dataset)


def gzip_csvfile(csv_file, zipfile):
    with open(csv_file, 'rb') as f_in:
        with gzip.open(zipfile, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    os.remove(csv_file)


def os_zip(csvfile, zipfile):
    os.system("zip -e -j %s %s -P QwE123rT" % (zipfile, csvfile))
    os.remove(csvfile)


def pol_query():
    query_detail = """
        SELECT DISTINCT
                        ORD.order_code           AS OrderNo
                  , CUS.name                 AS CustomerName
                  , CUS_BRN.brn              AS BRN
                  , PRD.network_product_desc AS ProductDescription
                  , ORD.order_type           AS OrderType
                  , ORD.ord_action_type      AS OrderActionType
                  , ORD.order_status         AS OrderStatus
                  , ORD.assignee             AS Assignee
                  , ORD.service_action_type  AS ServiceActionType
                  , ORD.arbor_disp           AS ServiceType
                  , ORD.business_sector      AS Sector
                  , ORD.initial_crd          AS InitialCRD
                  , ORD.close_date           AS CloseDate
                  , ORD.current_crd          AS CommissionDate
                  , ORD.taken_date           AS OrdCreationDate
                  , PRJ.project_code         AS ProjectID
                  , CON.contact_type         AS ContactType
                  , CON.family_name          AS FirstName
                  , CON.given_name           AS LastName
                  , CON.work_phone_no        AS WorkPhoneNo
                  , CON.mobile_no            AS MobileNo
                  , CON.email_address        AS Email
                  , SITE.location            AS Address
                FROM
                        RestInterface_order ORD
                        LEFT JOIN
                                RestInterface_project PRJ
                                ON
                                        ORD.project_id = PRJ.id
                        LEFT JOIN
                                RestInterface_customerbrnmapping CUS_BRN
                                ON
                                        ORD.customer_brn_id = CUS_BRN.id
                        LEFT JOIN
                                RestInterface_customer CUS
                                ON
                                        CUS_BRN.customer_id = CUS.id
                        LEFT JOIN
                                RestInterface_contactdetails CON
                                ON
                                        ORD.id = CON.order_id
                                        AND CON.contact_type IN ('A-end-Cust', 'Clarification-Cust', 'Maintenance-Cust', 'Technical-Cust')
                        LEFT JOIN
                                RestInterface_npp NPP
                                ON
                                        ORD.id        = NPP.order_id
                                        AND NPP.level = 'MainLine'
                        LEFT JOIN
                                RestInterface_product PRD
                                ON
                                        NPP.product_id = PRD.id
                        LEFT JOIN
                                RestInterface_site SITE
                                ON
                                        SITE.id = ORD.site_id
                WHERE
                        ORD.order_type              <> 'Cease'
                        AND ORD.order_status         = 'Closed'
                        AND ORD.close_date IS NOT NULL
                        AND ORD.close_date           > DATE_FORMAT(DATE_SUB(NOW(), INTERVAL 8 DAY), '%Y-%m-%d')
                        AND ORD.close_date           < DATE_FORMAT(NOW(), '%Y-%m-%d')
                ORDER BY
                        CloseDate
                  , OrderNo
                  , ContactType
                  , FirstName
                  , LastName
                ;
    """
    return query_detail


def pol_report_attach(gzipfile):
    part = MIMEBase('application', "octet-stream")
    part.set_payload(open(gzipfile, "rb").read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition',
                    'attachment; filename="%s"' % os.path.basename(gzipfile))
    return part


def sendEmail(attachment, body):
    emailBodyText = """\Hi All,

Please see attached ORION report.


Thanks you and best regards,
Orion Team"""

    emailBodyhtml = """\
        <html>
        <p>Hi All,</p>
        <p>Please see attached ORION report.</p>
        <p>&nbsp;</p>
        <p>Thanks you and best regards,</p>
        <p>Orion Team</p>
        </html>
        """
    # Turn these into plain/html MIMEText objects
    # part1 = MIMEText(emailBodyText, "plain")
    part2 = MIMEText(emailBodyhtml, "html")

    message = MIMEMultipart()
    # message.attach(MIMEText(body,"html"))
    message.attach(attachment)

    # Add HTML/plain-text parts to MIMEMultipart message
    # The email client will try to render the last part first
    # message.attach(part1)
    message.attach(part2)

    today_datetime = datetime.now()
    day = today_datetime.strftime('%d').lstrip('0')
    hour = today_datetime.strftime('%I').lstrip('0')
    ampm = today_datetime.strftime('%p').lower()
    year = today_datetime.strftime('%Y')
    month = today_datetime.strftime('%b').lower()
    subject = "[Transactional CE Survey Listings for OFD and OF Biz] {}{}{} {}{}".format(
        year, month, day, hour, ampm)
    sender = "orion@ncs.com.sg"
    receiver = ''
    receiverTo = 'christian.lim@singtel.com;zuliaj@singtel.com'
    receiverCc = 'aljo.ponce@singtel.com;jiangxu.jiang@singtel.com'
    #receiverTo = 'aljo.ponce@singtel.com'
    #receiverCc = ''
    message['Subject'] = subject
    message['From'] = "orion@singtel.com;orion@ncs.com.sg"
    message['To'] = receiverTo
    message['CC'] = receiverCc
    receiver = receiverTo + ";" + receiverCc

    try:
        smtpObj = smtplib.SMTP('gddsspsmtp.gebgd.org')
        smtpObj.sendmail(sender, receiver.split(";"), message.as_string())
        smtpObj.quit()
        # print(message.as_string())
        print("Successfully sent email")
    except Exception as e:
        print("Error: unable to send email : ")
        print(e)


def main():
    print("python main function")

    # today = date.today().strftime("%d/%m/%Y")
    # today_value = datetime.now().strftime("%Y%m%d%H%M%S")
    today_value = datetime.now().strftime("%d%m%y_%H%M")
    # csv_file="/app/o2p/ossadmin/pol/report_%s.csv"%(today_value)
    csv_file = "/app/o2p/ossadmin/christian/report_OFD_OFBiz/OFD_OFBiz_%s.csv" % (
        today_value)
    #gzip_file = csv_file+".gz"
    zip_file = csv_file + ".zip"
    print("Starting query database for POL : " +
          datetime.now().strftime("%Y%m%d%H%M%S"))
    dataset = run_query(pol_query())
    print("Query database for POL completed: " +
          datetime.now().strftime("%Y%m%d%H%M%S"))

    write_to_csv_detail(csv_file, dataset)
    #gzip_csvfile(csv_file, gzip_file)
    os_zip(csv_file, zip_file)
    #attachment = pol_report_attach(gzip_file)
    attachment = pol_report_attach(zip_file)
    print("Starting sending email : "+datetime.now().strftime("%Y%m%d%H%M%S"))
    sendEmail(attachment, "")
    print("Sending email completed : "+datetime.now().strftime("%Y%m%d%H%M%S"))


if __name__ == '__main__':
    main()

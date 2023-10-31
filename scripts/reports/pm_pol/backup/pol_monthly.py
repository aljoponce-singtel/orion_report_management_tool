#!/usr/bin/env python3

from jinja2 import Template
from jinja2 import Environment, FileSystemLoader
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
import calendar
from datetime import datetime, timedelta


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
    header = ['OrderNumber', 'OrderType', 'OrderStatus', 'OrderPriority', 'CurrentCRD', 'InitialCRD', 'TakenDate',
              'SDERcvdDate', 'ArborSvcType', 'ArborSvcDisp', 'OrderActionType', 'ProductDescription', 'ProjectManager',
              'CustomerName', 'ProjectID', 'SvcNumber', 'SvcActionType']
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


def pol_query(startDate, endDate):
    query_detail = ("""
        SELECT DISTINCT
            ORD.order_code
          , ORD.order_type
          , ORD.order_status
          , ORD.order_priority
          , ORD.current_crd
          , ORD.initial_crd
          , ORD.taken_date
          , ORD.sde_received_date
          , ORD.arbor_service_type
          , ORD.arbor_disp
          , ORD.ord_action_type
          , PRD.network_product_desc
          , (
                SELECT
                        CONCAT(given_name, ", ", family_name)
                FROM
                    RestInterface_contactdetails
                WHERE
                    order_id         = ORD.id
                    AND contact_type = 'Project Manager'
                LIMIT 1
            )
            AS project_manager , CUS.name , PRJ.project_code , ORD.service_number , ORD.service_action_type
        FROM
            RestInterface_order ORD
            LEFT JOIN
                RestInterface_customer CUS
                ON
                    CUS.id = ORD.customer_id
            LEFT JOIN
                RestInterface_project PRJ
                ON
                    PRJ.id = ORD.project_id
            LEFT JOIN
                RestInterface_npp NPP
                ON
                    NPP.order_id    = ORD.id
                    AND NPP.level   = 'Mainline'
                    AND NPP.status <> 'Cancel'
            LEFT JOIN
                RestInterface_product PRD
                ON
                    PRD.id = NPP.product_id
        WHERE
            ORD.business_sector NOT LIKE 'Enterprise Sales (Government%'
            -- AND ORD.assignee           = 'PM'
            AND ORD.taken_date BETWEEN '{}' AND '{}'
        ORDER BY
            ORD.order_code
        ;
        """).format(startDate, endDate)
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
        <p>Please have the attached ORION report.</p>
        <p>&nbsp;</p>
        <p>Thanks and Regards</p>
        <p>Muhammad <u>Sidd</u>ique</p>
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
    subject = "[ORION Monthly POL Report] {}{}{} {}{}".format(
        year, month, day, hour, ampm)
    sender = "orion@ncs.com.sg"
    receiver = ''
    receiverTo = 'mdsiddique@singtel.com;ngcaroline@singtel.com;g-ttcoe@singtel.com;EdmsOutlook@edmssingtel.onmicrosoft.com;nadianurul@singtel.com'
    receiverCc = 'jiangxu.jiang@singtel.com;aljo.ponce@singtel.com'
    # receiverTo = 'aljo.ponce@singtel.com'
    # receiverCc = ''
    message['Subject'] = subject
    message['From'] = "orion@singtel.com;orion@ncs.com.sg"
    message['To'] = receiverTo
    message['CC'] = receiverCc
    receiver = receiverTo + ";" + receiverCc

    try:
        #smtpObj = smtplib.SMTP('172.26.144.162')
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
    # csv_file = "/app/o2p/ossadmin/pol/backup/POL_PM_Managed_%s.csv" % (today_value)
    csv_file = "/app/o2p/ossadmin/test/backup/POL_Monthly_PM_Managed_%s.csv" % (
        today_value)
    #gzip_file = csv_file+".gz"
    zip_file = csv_file + ".zip"
    print("Starting query database for POL : " +
          datetime.now().strftime("%Y%m%d%H%M%S"))

    today_date = datetime.now().date()
    previousMonth = (today_date.replace(day=1) -
                     timedelta(days=1)).replace(day=today_date.day)
    startDate = str(previousMonth.replace(day=1))
    lastDay = calendar.monthrange(
        previousMonth.year, previousMonth.month)[1]
    endDate = str(previousMonth.replace(day=lastDay))
    print("start date: " + str(startDate))
    print("end date: " + str(endDate))

    dataset = run_query(pol_query(startDate, endDate))
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

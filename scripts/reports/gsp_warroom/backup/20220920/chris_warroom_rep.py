#!/usr/bin/env python3

from jinja2 import Template
from jinja2 import Environment, FileSystemLoader
from string import Template
import mysql.connector
from mysql.connector import errorcode
from mysql.connector.cursor import MySQLCursor
import smtplib
import base64
import array
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.base import MIMEBase
from email import encoders
import csv
import gzip
import zipfile
import shutil
import os
from datetime import datetime

def run_query(query_string, query_string2):
    db = mysql.connector.connect(
        user='o2puserp', password='Qsw123!du', host='172.26.144.143', database='o2pprod')
    # prepare a cursor object using cursor() method
    # cursor = db.cursor(dictionary=True)
    cursor = db.cursor()
    cursor2 = MySQLCursor(db)
    # execute SQL query using execute() method.
    cursor.execute(query_string)
    cursor2.execute(query_string2)
    datas = cursor.fetchall()
    cursor.close()
    cursor2.close()
    db.close()
    return datas


def write_to_csv_detail(csv_file, dataset):
    header = ['order_code','CUS_name' ,'order_type','order_status','order_priority','business_sector','current_crd','initial_crd','taken_date','sde_received_date','arbor_service_type','service_number','service_type','project_code','Prod_Desc', 'ACT_name','ACT_due_date','ACT_status' ,'Activity_code','ACT.ready_date','ACT.completed_date','Department','group_ID','CircuitTie', 'ED/PD Diversity', 'Exchange Code A', 'Exchange Code B']
    #header = ['order_code','CUS_name' ,'order_type','order_status','order_priority','business_sector','current_crd','initial_crd','taken_date','sde_received_date','arbor_service_type','service_number','service_type','project_code','Prod_Desc', 'ACT_name','ACT_due_date','ACT_status' ,'Activity_code','ACT.ready_date','ACT.completed_date','Department','group_ID','A_exchange_code', 'A_address', 'B_exchange_code', 'B_Address', 'CircuitTie', 'ED/PD Diversity']

    with open(csv_file, 'w', newline='') as csvfile:
        spamwriter = csv.writer(csvfile, delimiter=',',quoting=csv.QUOTE_NONNUMERIC)
        spamwriter.writerow(header)
        spamwriter.writerows(dataset)

def gzip_csvfile(csv_file, zipfile):
    with open(csv_file, 'rb') as f_in:
        with gzip.open(zipfile, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    os.remove(csv_file)

def zip_csvfile(csv_file, zipfile2):
    zf = zipfile.ZipFile(zipfile2, mode='w')
    try:
        zf.write(csv_file)
    finally:
        zf.close()
    os.remove(csv_file)

def os_zip(csvfile,zipfile):
    os.system("zip -e -j %s %s -P 12345678" % (zipfile, csvfile))
    os.remove(csvfile)
    #os.system('zip -e zipfile csvfile -P 12345678')

def war_query_step1():
    query_detail = """
        CREATE TEMPORARY TABLE temp_gsp_war_orders AS
        SELECT
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
            ORD.customer_id,
            ORD.assignee,
            ORD.project_id,
            ORD.site_id,
            ORD.circuit_id,
            ORD.product_description,
            ACT.name "activity_name",
            ACT.due_date,
            ACT.status Act_Status,
            ACT.predecessor_list,
            ACT.person_id,
            ACT.activity_code,
            ACT.ready_date,
            ACT.completed_date
        FROM
            RestInterface_order ORD
            JOIN RestInterface_activity ACT ON ACT.order_id = ORD.id
        WHERE
            ORD.order_status IN (
                'Submitted',
                'PONR',
                'Pending Cancellation',
                'Completed'
            )
            AND ORD.current_crd <= DATE_ADD(now(), INTERVAL 3 MONTH)
            AND ACT.tag_name = 'Pegasus';
        """
    return query_detail

def war_query_step2():
    query_detail = """
        SELECT
            ORD.order_code,
            CUS.name,
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
            PRJ.project_code,
            ORD.product_description,
            ORD.activity_name,
            ORD.due_date,
            ORD.Act_Status,
            ORD.activity_code,
            ORD.ready_date,
            ORD.completed_date,
            Concat(MTX.businessUnit, '_', MTX.department),
            PER.role group_ID,
            CIR.circuit_code CircuitTie,
            CASE
                WHEN T_ED_PD.parameter_name IS NOT NULL THEN T_ED_PD.parameter_value
                ELSE 'NA'
            END AS Diversity,
            SITE.site_code,
            SITE.site_code_second
        FROM
            temp_gsp_war_orders ORD
            LEFT JOIN RestInterface_project PRJ ON ORD.project_id = PRJ.id
            LEFT JOIN RestInterface_site SIT ON ORD.site_id = SIT.id
            LEFT JOIN RestInterface_circuit CIR ON ORD.circuit_id = CIR.id
            LEFT JOIN (
                SELECT
                    DISTINCT order_id,
                    npp_id,
                    level,
                    status,
                    parameter_name,
                    parameter_value
                FROM
                    RestInterface_npp t_npp,
                    RestInterface_parameter t_par
                WHERE
                    parameter_name = 'Type'
                    AND parameter_value IN ('1', '2', '010', '020')
                    AND order_id IS NOT NULL
                    AND t_npp.id = t_par.npp_id
                    AND t_npp.status <> 'Cancel'
            ) T_ED_PD ON ORD.ID = T_ED_PD.order_id
            LEFT JOIN RestInterface_person PER ON PER.id = ORD.person_id
            LEFT JOIN EscalationMatrix MTX ON MTX.GroupID = PER.role
            LEFT JOIN RestInterface_customer CUS ON CUS.id = ORD.customer_id
            LEFT JOIN RestInterface_site SITE ON SITE.id = ORD.site_id;
        """
    return query_detail

def report_attach(gzipfile):
    part = MIMEBase('application', "octet-stream")
    part.set_payload(open(gzipfile, "rb").read())
    encoders.encode_base64(part)
    part.add_header('Content-Disposition',
                    'attachment; filename="%s"' % os.path.basename(gzipfile))
    return part


def sendEmail(attachment, body):
    emailBodyText = """
    Hi, GSP War Room Team ,

    Please see attached report.


Thanks you and best regards,
Orion Team
    """

    emailBodyhtml = """\
        <html>
        <p>Hi All,</p>
        <p>Please have the attached ORION report.</p>
        <p>&nbsp;</p>
        <p>Thanks and Regards</p>
        <p>Jiang Xu</p>
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
    subject = "[GRP War Room Report] {}{}{} {}{}".format(
        year, month, day, hour, ampm)
    sender = "orion@ncs.com.sg"
    receiver = ''
    receiverTo = 'teokokwee@singtel.com;christian.lim@singtel.com'
    receiverCc = 'jiangxu.jiang@singtel.com;aljo.ponce@singtel.com'
    # receiverTo = 'aljo.ponce@singtel.com'
    # receiverCc = ''
    #receiverTo = 'christian.lim@singtel.com'
    #receiverCc = 'maisarahm@singtel.com;jiangxu.jiang@singtel.com;aljo.ponce@singtel.com'
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
    csv_file = "/app/o2p/ossadmin/christian/report/gsp_warroom_report_%s.csv" % (today_value)
    gzip_file = csv_file+".gz"
    zip_file = csv_file+".zip"
    print("Starting query database for POL : " +
          datetime.now().strftime("%Y%m%d%H%M%S"))
    dataset = run_query(war_query_step1(), war_query_step2())
    print("Query database for GSP war room completed: " +
          datetime.now().strftime("%Y%m%d%H%M%S"))

    write_to_csv_detail(csv_file, dataset)
    #gzip_csvfile(csv_file, gzip_file)
    os_zip(csv_file, zip_file)
    #zip_csvfile(csv_file, zip_file)
    attachment = report_attach(zip_file)
    #attachment = report_attach(gzip_file)
    print("Starting sending email : "+datetime.now().strftime("%Y%m%d%H%M%S"))
    sendEmail(attachment, "")
    print("Sending email completed : "+datetime.now().strftime("%Y%m%d%H%M%S"))


if __name__ == '__main__':
    main()

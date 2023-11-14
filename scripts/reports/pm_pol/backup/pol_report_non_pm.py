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
    header = ['OrderNumber', 'OrderType', 'OrderStatus', 'OrderPriority', 'CurrentCRD', 'InitialCRD', 'TakenDate',
              'SDERcvdDate', 'ArborSvcType', 'CustomerName', 'ProjectID', 'SvcNumber', 'SvcActionType', 'ActivityName', 'ActivityStatus']
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
            ORD.order_code
          , ORD.order_type
          , ORD.order_status
          , ORD.order_priority
          , ORD.current_crd
          , ORD.initial_crd
          , ORD.taken_date
          , ORD.sde_received_date
          , ORD.arbor_service_type
          , CUS.name
          , PRJ.project_code
          , ORD.service_number
          , ORD.service_action_type
          , ACT.name
          , ACT.status
        FROM
            RestInterface_order ORD
            LEFT JOIN
                RestInterface_activity ACT
                ON
                    ORD.id = ACT.order_id
            INNER JOIN
                RestInterface_customerbrnmapping BRN
                ON
                    ORD.customer_brn_id = BRN.id
            INNER JOIN
                RestInterface_customer CUS
                ON
                    BRN.customer_id = CUS.id
            LEFT JOIN
                RestInterface_project PRJ
                ON
                    ORD.project_id = PRJ.id
        WHERE
            ORD.business_sector NOT LIKE 'Enterprise Sales (Government%'
            AND ORD.assignee           = 'Non-PM'
            AND ORD.taken_date         > '2018-09-31'
            AND
            (
                (
                    CUS.name    = "ALIBABA CLOUD(SINGAPORE)PRIVATE LIMITED"
                    OR CUS.name = "ALIBABA.COM SINGAPORE E-COMMERCE PRIVATE LIMITED"
                    OR CUS.name = "SPREADTRUM COMMUNICATIONS (SHANGHAI) CO.. LTD"
                    OR CUS.name = "TIKTOK PTE LTD"
                )
                OR BRN.brn IN ("HRB 3868-001"      , "HRB 3868-RS-001", "199801276M", "C102043106", "774107-X", "197001157D", "T05FC6747K", "197402087W", "201411358G", "C101978032", "201434292D", "200802236M - 0001", "C100276882", "09748794-000-06-11-2", "94-2404110-001", "94-2404110-002", "09748794-001-06-03-9", "C100193065", "310115400075100", "313748", "110000450034724", "94-2404110-0001", "C102042984", "U30007KA1996PTC019630", "011-03-003992-0001", "011-03-003992", "120-81-84429", "S89FC4036J", "310000400623430", "C100001304", "ST002718", "198903166R", "310115400257368", "10000040008460-0001", "C102043793", "1226779-W", "199800781G", "198803778N", "201215881Z", "01.495.218.8-415.000", "01.115.965.4-092.000", "818779068", "01.359.560.8-651.000", "01.772.249.7-314.001", "02.021.861.5-218.001", "01.951.582.4-211.000", "01.109.421.6-092.000", "01.747.595.5-403.001", "01.347.720.3-07", "01.302.104.3-411.000", "01.849.323.9-217.001", "01.329.756.9-216.000", "01.496.563.6-218.000", "02.492.572.0-701.000", "02.167.752.1-218.000", "02.275.220.8-308.000", "02.275.223.2-308.000", "02.181.213.6-308.000", "01.747.595.5-403.002", "01.309.996.5-222.001", "01.000.566.8-092.000", "03.203.481.1-312.001", "01.445.112.4-218.001", "01.001.855.4-092.000", "01.347.720.3-073.000", "01.701.400.2-218.000", "01.792.188.3-216.000", "01.000.083.4-331.000", "01.894.601.2-218.000", "02.484.771.7-725.001", "01.371.502.4-218.001", "01.585.575.2-722.001", "02.061.800.5-332.000", "01.002.870.2-451.000", "01.101.793.6-092.000", "02.275.463.4-725.000", "01.248.407.7-701.000", "01.761.751.5-058.000", "02.327.136.4-701.000", "01.467.072.3-722.000", "01.461.925.8-308.000", "02.180.723.5-308.000", "01.585.574.5-728.000", "01.822.043.4-703.001", "00000002201601180097-001", "2201601180097", "201410446K", "199400619R", "004191324-AUS", "200610851H", "12-395-2384", "14072404481", "199600210G", "200002047W", "199903416C-0001", "C102043140", "53658813", "510109400000942", "U72200KA1989PTC010490", "U72200KA2007PTC043986", "310000400631456", "22660271", "FC009351", "HRB 26115", "8120", "28994042", "804065654", "FC009351", "28994042", "10452281-000-02-09-2", "S80FC2839H-0002", "C102043146", "102-84-01718", "U85110PN1993PLC145950-0001", "91500000693909742Q", "01.353.126.4-091.000", "20800-0007", "C102043803", "198104916R", "199902550E", "198602913H", "198902034E", "C102043166", "197100846H", "200514386K", "T13FC0181A", "1692910-000-04-04-A", "16929109-000-04-04-1", "C100160652", "C100159942", "C102043172", "200108289C", "C100002789", "HR42243-0001", "HR42243-0002", "C102043174", "198502157D", "C100504575", "211-86-08983", "612731-A", "196600305G", "C00001466544", "C101851266", "199003222H", "201410701H", "199302460C", "890343X", "200305058C", "200309059W", "200308451M", "200706686R", "200515736C", "199701358Z", "201420227E", "201109060K", "C102042992", "198900036N-0001", "200413169H", "200106159R", "198904995R", "201109018N", "197800159W", "411043000325", "200803504M", "200723316K", "200009177E", "200102075W", "200611176D", "842006U", "0100-01-102106", "199202169M", "200414570W", "196900456G", "198102209C", "197801869H", "199606698M", "201539069W", "778385-K", "819351-H", "201130791H", "113253330", "30339485-000-07-11-2", "2000102.64", "C102043202", "199904277R", "DW0001KLN", "200500889M", "C100001413", "C00002655004", "31641877", "200010264E - 0001", "36957940-001-06-09-7", "088 790 934", "C00002752586", "541924H", "AK/969937", "3816299", "C101959639", "200910817N-0001", "200910817N-0002", "200404385W", "201609636D", "199607124D-0001", "27927257", "27927257-0001", "C102043208", "199702449H", "199001932K", "200600831E", "S83FC3361G", "S83FC3361G-0001", "12366616", "12366616-0001", "C100002039", "C101851248", "00199603", "13/NH-GP", "AG0/REC/178", "C100008966", "04705511-000-04-07-2", "200309485K", "197200204M", "3100736652335", "11812856-001", "56081472684", "199001506W", "0093479", "S27FC0556D-0001", "197329691", "199907912R", "200504906W", "198102368C", "199502839G", "199303828M", "199303821R", "C102042966", "199601882C", "199506048W", "200308690C", "198105775H", "197800356N", "199206654D", "201917295G", "197502294K", "200200864C", "201202781W", "199206653M", "196900269D", "200002309M", "198100320K", "199306569Z", "C00000849962", "C00000850902", "199900649M", "24047400K", "C00001634637", "C102043235", "197300581H", "199803778Z", "199000355E", "199602570Z", "200007645E", "199605569N", "S96FC5123H", "C00001553350", "201540701G", "PAN AAFCD5584N", "330100500016639", "913100006624264000", "00513603-000-04-08-7", "53017509", "AAGCD5838A", "196800306E-0001", "C102043003", "9201257320", "198703452R", "198703453G", "198600293R", "197802641D", "198600295W", "198600294G", "21955212-000-06-04-8", "200104695D", "197000917M", "197501566C-0006", "197802405G", "09.03.1.64.96422", "200707846K", "196500235G", "198802741D", "514292", "201004959C - 0001", "201004959C-0001", "C102044326", "201431933K", "36395859", "0199-03-009739", "36395859-000-12", "196300440G", "200503404G", "200805222E", "198502184H", "197901376Z", "C102043301", "201012421H", "201629842W", "201728670M", "201132730N", "S83FC3303B", "201308525W", "199000266N", "199900418N", "197300901E", "C102043310", "C100562929", "199503536G", "199601368H", "199609223Z", "199002960D", "T13FC0095K", "199705878D", "C00002697501", "C100000489", "200607239Z", "198601114G", "200600625G", "T14FC0142D", "HRB4724-001", "C102043340", "201525994H", "198304615H", "S83FC3226J", "C00002690882", "T06FC6822E", "197700521G", "199000411E", "S98FC5484F", "199205138M", "199003855G", "199206384W", "197401200M", "197601944R", "T04FC6546E", "199602333M", "T00RF0026B", "21733197-000-02-18-3", "81023858", "30883832-000-04-04-7", "81025528", "N51292", "4403011022600", "0105 5490 16141", "197402104W-RS-001", "C102043029", "197900947D", "S84FC3461C", "11-68724", "11-115964", "197300590K", "198500154W", "32001531586-AUS", "11-124073", "199702709C", "C00001922365", "692", "27AABCD0503B1ZK", "197601586K", "C101851279", "S74FC2367E", "C100664431", "C102035989", "0100-01-095702", "S75FC2479J", "200001347E", "S64FC1600D", "102-81-42945-0002", "102-85-05763", "102-81-42945-0003", "102-81-42945-0004", "102-81-42945", "102-81-42945-0001", "81024328", "102-85-05803_0001", "102-85-05803-0001", "102-85-05803-0002", "116-81-74581", "0110-01-055029", "632 012 100 R.C.S. PARIS", "91310000710939418D", "11.60363", "328418-A", "199001413D-0001", "C102043559", "C101310159", "C101177337", "C101879900-0001", "4011101040731", "200002257Z", "C102044553", "199604708Z", "C00002846016", "201815216Z", "C00002293406", "1757", "146463", "0110-01-021868", "23B.10.070", "14517617-000-05-08-7", "244003A", "37668", "199802706M", "199802706M-002", "C100002041", "199604017Z", "C102046263", "197701907C", "198900342G", "197101351K", "20047", "C102043651", "198400460G", "198300518C", "33391", "197402309C", "200009451W", "200300965K", "197600379E-0001", "197600379E", "200006330W", "198502606C", "198500065G-0001", "C102042973", "C00000851096", "C102043044", "200919211H", "200515140C", "93-338-0206", "200101955G", "957449481", "200610663N", "199501175R", "S93FC4587B", "199604389N", "199300243G", "200504146M", "T08FC7164D", "C00002540842", "198801539M-0001", "198801539M-0002", "C102043665", "L72200MH1989PLC053666", "200107453K", "201911799E", "55-51764", "25156 SO", "199304028M", "199804290C", "200503681E", "199608647K", "389628", "94-2805249", "11068013-000-06-03-A", "199404596R", "C100188535", "198402696M", "C00001776058", "C102043666", "C102044344", "199001339Z", "199706517H", "199706517H-002", "199700474Z", "200105903G", "197700866R", "C102050312", "195400071R", "660159-V", "52916905C", "199903008M", "198101855M", "1964002", "197700849D", "195900017D", "199805318W", "198601745D", "197802688K", "199408293E", "196500260K", "196700396R", "196600126N", "198301152E", "197200276D", "196600262R", "198102911H", "193800020E", "C100001695", "C101851245", "172215 AKASAKA", "C100001406", "198201330D", "190800011G", "C102040226", "192000003W", "199100915E", "200309766Z-0001", "110111-2295841", "200513174K", "C102042974", "200002935N", "0442.652.075", "S62SS0050B", "199706229Z", "C102049129", "197200399R", "199702494W", "504605534", "199805986M", "199603115R", "199705947H", "180111-0689852", "2050074190", "02274260278", "03211930106", "199200682C", "S96FC5246H", "198600139K-0001", "196900413D", "C101634285", "T10FC0157B", "200003934Z", "T10FC0158J", "C101809978", "21238199", "001316827-RS-001", "1316827", "C100031251", "310000400441678", "677253", "107-81-95471", "18329002-003-05-08-5", "199407631H", "67/UBCK-GPDC", "03/UBCK-GPHDQLO", "C100006279", "0100-01-066780", "86385237", "200107687C", "200806024N", "199800274N-0001", "199800274N-0002", "U99999DL1993PLC054135-0001", "JPN-01-00188", "211-81-58436", "3358805", "0104-01-047368", "TWN-01-00203", "200703732N", "200904816K", "C100208167", "HKG-01-00195", "18329002-003-05-06-6", "199002477Z", "201011255E", "17867687-000-12-02-4", "17867687-000-12-02-3", "F06566M", "200516541G", "34936506-000-08-04-6-0001", "C102043060", "U99999DL1993PLC054135-0001", "HKG-01-00196", "HKG-01-00197", "107537001897", "613774", "200708166K", "198100072M", "HKG-01-00222", "T00FC5889H-0002", "HKG-01-00157", "03593595-000-07-03-4", "03593595-000-07-06-8", "200201667C", "199407561M", "199407489K", "199501890H", "199408894D", "199407141H", "199805047C", "200920538E", "200816675N", "197701612R", "199002938M", "201115323H", "200811332E", "200202817D", "201100841E", "200309771W", "199606320M", "200713442C", "201001253E", "199602948G-0001", "C102043761", "200105403N", "201022461W", "201320041M", "91320700558092514N", "91150700797179158X", "913101157687559887", "91320100MA1NL2KT4K", "430100400001853", "91510112582601796T", "91320505753939709H", "91320594716805103D", "91320100070708151D", "91320200329530269R", "91320100608940324G", "91441900794691244W", "9133010072760960XL", "91510100587581456K", "91110302600055056H", "913204126081285000", "916101326732979887", "197700888D",
                               "913703005871737517", "913100007862908460", "91420100584884240W", "91440400754523959C", "199608497E", "201604692N-0001", "004 315 628", "C102043766", "198601884W", "198601884W-0001", "200910817N-0003", "C100005010", "53321974D", "C102043064", "200610380D", "200806228G", "199705391Z", "S90FC4239C", "200811644D", "CN 18723", "200209516D", "199409317W", "C00002648533", "201129804K", "214-86-49528", "200612002M", "110-81-28774", "201608087N", "200403828K", "201510244D", "199004218W", "201132693H", "198804696Z", "197000959E", "201710934Z", "198900760H", "200106625G", "198500561R", "199705514Z", "198500562G", "201306439G", "197300678G", "201725260M", "198903088H", "197201770G-0001", "C102043065", "201711640C", "198905026D", "201820449R", "201136140E", "197400310D", "200603576R", "C100002104", "199204083K", "73/0067", "198402144N", "198601539E", "199506592H", "HRB 17474", "109147000H", "C102001779", "201822113C", "200505476K", "201613242N", "201629708W", "197401891E", "201114431W", "200312665W", "198201025C", "197500236D", "C00001468340", "200007345C", "0003", "155-549", "C00001518818", "C101692798", "01323", "C00002067952", "804089", "200701866W", "199201264R", "200101068C", "200209371R", "200408565G", "200005878M", "197802854W", "199705247G", "C102036697", "197300970D", "201304990E", "198003912M", "C100001914", "C100001696", "199504471E", "199504471E-0001", "C100001700", "199504468H", "200009950Z", "200007746K", "199408833R", "C102043072", "C100001691", "199901848G", "201706515G", "200000023R", "201806089K", "201719595R", "200302108D-0001", "200302108D", "C100002316", "200306959Z", "C100002227", "199504470N", "C100001697", "201418303D", "199403524H", "201524980R", "201806228M", "197402009W", "201224603M", "199004280Z", "198202292D", "200001959G", "C102042977", "199804757Z", "199704861W", "199101735W", "198905369K", "201728722E", "198702333K", "201607143W", "201535120W", "C101767862", "ITS_SHQ8PQ", "1811096", "1800000", "201620102W-001", "FS201413423", "199904275N", "200106385Z", "199701117H", "145516", "C100151272", "198500108W", "52839045K", "198400509D", "52888329A", "C102039834", "199606134Z-0001", "52860153M", "16940355-000-02-02-9", "464942H", "T05FC6694A", "199003990W", "195400080W", "201229045Z", "198002010D", "198600265Z", "194700145C", "195700145G", "198104863M", "41/1962", "196200041R", "C00000029590", "UOBM271809-K", "C102043084", "196600440W", "197100152R", "201302106G", "197201862K", "198600120Z", "197801465G", "198000920N", "13435676-000-02-07-6", "197000447W", "197000447W-0001", "198600323C", "197400040W", "198104982C", "200507506H", "196300438C", "199504162Z", "081 001 194", "199706420K", "199804310N", "ITS_WOR", "C102044362", "U72300TN2001PTC46551", "198904174D", "0100-01-140024", "0100-01-140024-RS-002", "0100-01-140024-RS-003", "200905126W", "200905126W-RS-001", "17454-RS-001", "17454", "U32109MH1999PTC207960", "U32109MH1999PTC207960-RS-001", "0156490", "0156490-RS-001", "1337-RS-001", "1337", "200822057R-RS-001", "200822057R", "0100-01-140024-RS-001", "S81FC3003H", "200719281K-RS-001", "C101939331", "S81FC3003H-RS-001", "F03003C-RS-001", "F03003C", "201115021D", "201115021D-RS-001", "200812992E-088")
            )
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
    subject = "[ORION POL (Non-PM) Report] {}{}{} {}{}".format(
        year, month, day, hour, ampm)
    sender = "orion@ncs.com.sg"
    receiver = ''
    receiverTo = 'mdsiddique@singtel.com;ngcaroline@singtel.com;g-ttcoe@singtel.com;EdmsOutlook@edmssingtel.onmicrosoft.com'
    receiverCc = 'jiangxu.jiang@singtel.com;aljo.ponce@singtel.com'
    #receiverTo = 'aljo.ponce@singtel.com'
    #receiverCc = ''
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
    csv_file = "/app/o2p/ossadmin/pol/backup/POL_Non_PM_Managed_%s.csv" % (
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
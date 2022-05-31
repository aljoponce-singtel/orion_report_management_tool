import os
from datetime import datetime
import smtplib
from email.mime.base import MIMEBase
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email import encoders
import logging

logger = logging.getLogger(__name__)


class EmailClient:
    def __init__(self):
        self.server = None
        self.port = None
        self.subject = None
        self.sender = None
        self.receiverTo = None
        self.receiverCc = None
        self.emailFrom = None
        self.emailBodyText = None
        self.emailBodyHtml = None
        self.attachments = []

    def addTimestamp(self, str):
        today_datetime = datetime.now()
        day = today_datetime.strftime('%d').lstrip('0')
        hour = today_datetime.strftime('%I').lstrip('0')
        ampm = today_datetime.strftime('%p').lower()
        year = today_datetime.strftime('%Y')
        month = today_datetime.strftime('%b').lower()
        str = "[{}] {}{}{} {}{}".format(
            str, year, month, day, hour, ampm)

        return str

    def addTimestamp2(self, str):
        today_datetime = datetime.now()
        day = today_datetime.strftime('%d').lstrip('0')
        hour = today_datetime.strftime('%I').lstrip('0')
        minute = today_datetime.strftime('%M').lstrip('0')
        ampm = today_datetime.strftime('%p')
        year = today_datetime.strftime('%Y')
        month = today_datetime.strftime('%b')
        str = "[{}] {} {}, {} {}:{} {}".format(
            str, month, day, year, hour, minute, ampm)

        return str

    def attachFile(self, attachment):
        logger.info('Attaching file {} ...'.format(
            os.path.basename(attachment)))
        self.attachments.append(attachment)

    def smtpSend(self):
        logger.info('Sending email with subject "{}" ...'.format(self.subject))

        try:

            # Turn these into plain/html MIMEText objects
            # part1 = MIMEText(emailBodyText, "plain")
            part2 = MIMEText(self.emailBodyHtml, "html")

            message = MIMEMultipart()

            for attachment in self.attachments:
                part = MIMEBase('application', "octet-stream")
                part.set_payload(open(attachment, "rb").read())
                encoders.encode_base64(part)
                part.add_header('Content-Disposition',
                                'attachment; filename="%s"' % os.path.basename(attachment))
                message.attach(part)

            # Add HTML/plain-text parts to MIMEMultipart message
            # The email client will try to render the last part first
            # message.attach(part1)
            message.attach(part2)
            message['Subject'] = self.subject
            message['From'] = self.emailFrom
            message['To'] = self.receiverTo
            message['CC'] = self.receiverCc
            receiver = self.receiverTo + ";" + self.receiverCc
            smtpObj = smtplib.SMTP(self.server, self.port)
            smtpObj.sendmail(self.sender, receiver.split(";"),
                             message.as_string())
            smtpObj.quit()

        except Exception as e:
            logger.error("Failed to send email.")
            logger.exception(e)

    def win32comSend(self):
        logger.info('Sending email with subject "{}" ...'.format(self.subject))

        try:
            import win32com.client
            outlook = win32com.client.Dispatch('outlook.application')

            mail = outlook.CreateItem(0)
            mail.To = self.receiverTo
            mail.CC = self.receiverCc
            mail.Subject = self.subject
            mail.HTMLBody = self.emailBodyHtml
            mail.Body = self.emailBodyText

            for attachment in self.attachments:
                mail.Attachments.Add(attachment)

            mail.Send()

        except Exception as e:
            logger.error("Failed to send email.")
            logger.exception(e)

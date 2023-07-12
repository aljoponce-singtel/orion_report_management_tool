import os
import sys
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
        self.receiver_to = None
        self.receiver_cc = None
        self.email_from = None
        self.email_body_text = None
        self.email_body_html = None
        self.attachments = []

    def add_timestamp(self, subject):
        today_datetime = datetime.now()
        day = today_datetime.strftime('%d').lstrip('0')
        hour = today_datetime.strftime('%I').lstrip('0')
        minute = today_datetime.strftime('%M')
        am_pm = today_datetime.strftime('%p')
        year = today_datetime.strftime('%Y')
        month = today_datetime.strftime('%b')
        subject = "[{}] {} {}, {} {}:{} {}".format(
            subject, month, day, year, hour, minute, am_pm)

        return subject

    # Legacy/deprecated function
    def add_timestamp_2(self, subject):
        today_datetime = datetime.now()
        day = today_datetime.strftime('%d').lstrip('0')
        hour = today_datetime.strftime('%I').lstrip('0')
        ampm = today_datetime.strftime('%p').lower()
        year = today_datetime.strftime('%Y')
        month = today_datetime.strftime('%b').lower()
        subject = "[{}] {}{}{} {}{}".format(
            subject, year, month, day, hour, ampm)

        return subject

    def get_platform(self):
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

    def set_email_subject(self, subject):
        self.subject = subject

    def set_email_body_text(self, email_body):
        self.email_body_text = email_body

    def set_email_body_html(self, email_body):
        self.email_body_html = email_body

    def attach_file(self, attachment):
        logger.info('Attaching file {} ...'.format(
            os.path.basename(attachment)))
        self.attachments.append(attachment)

    def smtp_send(self):
        logger.info('Sending email with subject "{}" ...'.format(self.subject))

        try:
            if self.email_body_html:
                # Turn these into plain/html MIMEText objects
                # part1 = MIMEText(emailBodyText, "plain")
                part2 = MIMEText(self.email_body_html, "html")

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
            message['From'] = self.email_from
            message['To'] = self.receiver_to
            message['CC'] = self.receiver_cc
            receiver = self.receiver_to + ";" + self.receiver_cc
            smtp_obj = smtplib.SMTP(self.server, self.port)
            smtp_obj.sendmail(self.sender, receiver.split(";"),
                              message.as_string())
            smtp_obj.quit()

        except Exception as e:
            logger.error("Failed to send email.")
            logger.exception(e)

    def win32com_send(self):
        logger.info('Sending email with subject "{}" ...'.format(self.subject))

        try:
            import win32com.client
            # Requires the Outlook applicaition to be installed in the system
            outlook = win32com.client.Dispatch('outlook.application')

            mail = outlook.CreateItem(0)
            mail.To = self.receiver_to
            mail.CC = self.receiver_cc
            mail.Subject = self.subject

            if self.email_body_html:
                mail.HTMLBody = self.email_body_html
            # if self.email_body_text:
            #     mail.Body = self.email_body_text

            for attachment in self.attachments:
                mail.Attachments.Add(attachment)

            mail.Send()

        except Exception as e:
            logger.error("Failed to send email.")
            logger.exception(e)

    def send(self):

        platform = self.get_platform()

        if platform == 'Linux':
            self.smtp_send()
        elif platform == 'Windows':
            self.win32com_send()
        else:
            raise Exception(f"OS {platform} not supported.")

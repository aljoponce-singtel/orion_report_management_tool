# Import built-in packages
import logging
import os
import sys
import smtplib
from datetime import datetime
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText

logger = logging.getLogger(__name__)


class EmailClient:

    def __init__(self):
        self.server = None
        self.port = None
        self.subject = None
        self.sender = None
        self.email_from = None
        self.email_body_text = None
        self.email_body_html = None
        self.receiver_to_list = []
        self.receiver_cc_list = []
        self.attachments = []
        self.email_att_to_remove = []

     # Calling destructor
    def __del__(self):
        # Remove any temporary email attachments
        for attachment in self.email_att_to_remove:
            logger.debug(f"Removing file {os.path.basename(attachment)} ...")
            os.remove(attachment)
        # Clear the list
        self.email_att_to_remove.clear()

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

    def get_email_subject(self) -> str:
        return self.subject

    def set_email_subject(self, subject):
        self.subject = subject

    def email_list_to_str(self, email_list: list, separator: str = ";"):
        return separator.join(email_list)

    def email_str_to_list(self, email_str: str, separator: str = ";"):
        return email_str.split(separator)

    def add_email_receiver_to(self, email: str):
        self.receiver_to_list.extend(email.split(";"))

    def add_email_receiver_cc(self, email: str):
        self.receiver_cc_list.extend(email.split(";"))

    def remove_email_receiver_to(self, email: str):
        for email_address in self.email_str_to_list(email):
            if email_address in self.receiver_to_list:
                self.receiver_to_list.remove(email_address)

    def remove_email_receiver_cc(self, email: str):
        for email_address in self.email_str_to_list(email):
            if email_address in self.receiver_cc_list:
                self.receiver_cc_list.remove(email_address)

    def add_to_list_of_att_to_remove(self, attachment):
        logger.debug(
            "Temporary files will be removed after sending the email ...")
        # Store the attachment to a list to remove at the end of this script
        self.email_att_to_remove.append(attachment)

    def set_email_body_text(self, email_body):
        self.email_body_text = email_body

    def set_email_body_html(self, email_body):
        self.email_body_html = email_body

    def export_to_html_file(self, html_str, file_path):
        # Write the HTML string to the file
        with open(file_path, "w") as file:
            file.write(html_str)

    def attach_file(self, attachment):
        logger.info('Attaching file {} ...'.format(
            os.path.basename(attachment)))
        self.attachments.append(attachment)

    def get_file_attachments(self, include_path=True):
        if include_path:
            return self.attachments
        else:
            new_attachments = []
            for attachment in self.attachments:
                new_attachments.append(os.path.basename(attachment))
            return new_attachments

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

            logger.warning(self.receiver_to_list)
            logger.warning(self.receiver_cc_list)
            # logger.warning(self.email_list_to_str(self.receiver_to_list))
            # logger.warning(self.email_list_to_str(self.receiver_cc_list))

            # Add HTML/plain-text parts to MIMEMultipart message
            # The email client will try to render the last part first
            # message.attach(part1)
            message.attach(part2)
            message['Subject'] = self.subject
            message['From'] = self.email_from
            message['To'] = self.email_list_to_str(self.receiver_to_list)
            message['CC'] = self.email_list_to_str(self.receiver_cc_list)
            receiver = self.email_list_to_str(
                self.receiver_to_list) + ";" + self.email_list_to_str(self.receiver_cc_list)
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

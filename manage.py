import sys
import importlib
from datetime import datetime
from scripts.EmailClient import EmailClient
from scripts import utils


def main():

    try:
        # Print All arguments
        # sys.argv[0] = This file (manage.py)
        # sys.argv[1] - Report/script folder
        # sys.argv[2] - Main file/module
        # sys.argv[3] - function to call
        print(sys.argv)

        # Add script folder path to python system path
        sys.path.insert(0, './scripts/' + sys.argv[1])
        # Import module
        importModule = importlib.import_module(sys.argv[2])
        # Call function from the imported module
        func = getattr(importModule, sys.argv[3])
        func()

    except Exception as error:
        print(error)

        # Output error to file
        timeStamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        fileName = '{}.error.{}.log'.format(__file__, timeStamp)
        errorFile = open(fileName, 'a')
        errorFile.write(str(error))
        errorFile.close()
        sendEmail(sys.argv[1], fileName)


def sendEmail(report, fileName):

    emailBodyText = ("""
        Hello,

        There was an error generating the "{}" report.
        Please see attached error logs.
        Please see report logs for more information.


        Thanks you and best regards,
        Orion Team
    """).format(report)

    emailBodyhtml = ("""\
        <html>
        <p>Helllo,</p>
        <p>There was an error generating the "{}" report.</p>
        <p>Please see attached error logs.</p>
        <p>Please see report logs for more information.</p>
        <p>&nbsp;</p>
        <p>Thank you and best regards,</p>
        <p>Orion Team</p>
        </html>
        """).format(report)

    try:
        emailClient = EmailClient()
        subject = 'ERROR for Orion Report - {}'.format(report)
        emailClient = EmailClient()
        emailClient.subject = emailClient.addTimestamp2(subject)
        emailClient.receiverTo = 'aljo.ponce@singtel.com'
        emailClient.receiverCc = ''
        emailClient.emailBodyText = emailBodyText
        emailClient.emailBodyHtml = emailBodyhtml
        emailClient.attachFile(fileName)

        if utils.getPlatform() == 'Windows':
            emailClient.win32comSend()
        else:
            emailClient.server = 'gddsspsmtp.gebgd.org'
            emailClient.port = 25
            emailClient.sender = 'orion@ncs.com.sg'
            emailClient.emailFrom = 'orion@singtel.com;orion@ncs.com.sg'
            emailClient.smtpSend()

    except Exception as e:
        print("Failed to send email.")


if __name__ == '__main__':
    main()

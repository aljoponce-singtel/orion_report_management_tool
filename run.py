"""
This script will read the arguments passed from manage.sh.

This is an example of a full command:
$source manage.sh gsp main main

Running manage.sh will run manage.py and pass the last 3 arguments
$manage.py gsp main main

The arguments are described as follows:
    sys.argv[0] - manage.py (this file)
    sys.argv[1] - gsp => scripts/gsp (script folder)
    sys.argv[2] - main => scripts/gsp/main.py (file/module to run/import)
    sys.argv[3] - main => main() (function to call)

The last 2 arguments (sys.argv[2] and sys.argv[3]) are optional.

If the command executed is $manage.py gsp main:
sys.argv[3] will be set to 'main' by default.
There should be a main() fucntion defined inside file from sys.argv[2]

If the command executed is: $manage.py gsp,
sys.argv[2] and sys.argv[3] will both be set to 'main' by default.
There should be a main.py file inside the scripts folder.
There should be a main() fucntion defined inside file from sys.argv[2].

Any exception errors when running the script will inform admin through email.
"""

import sys
import importlib
import traceback
import configparser
from scripts.helpers import EmailClient
from scripts.helpers import utils

config = configparser.ConfigParser()
configFile = 'run.ini'
config.read(configFile)
defaultConfig = config['DEFAULT']
emailConfig = config['Email']


def main():

    try:
        # Print All arguments
        print(sys.argv)

        # Add script folder path to python system path
        sys.path.append('./scripts/reports/' + sys.argv[1])

        # If the main script and/or main function is provided
        if len(sys.argv) > 2:
            # Import module (call script file)
            importModule = importlib.import_module(sys.argv[2])

            if len(sys.argv) > 3:
                # Call the main function from the imported module
                func = getattr(importModule, sys.argv[3])
                func()
            else:
                # Call main() function from the imported module by default
                func = getattr(importModule, 'main')
                func()

        # If only the script folder provided
        else:
            # Import/call main.py script/module by default
            importModule = importlib.import_module('main')
            # Call main() function from the imported module by default
            func = getattr(importModule, 'main')
            func()

    except Exception as error:

        timeStamp = utils.get_current_datetime()
        fileName = '{}.{}.error.log'.format(__file__, timeStamp)

        if defaultConfig.getboolean('create_error_log'):
            # Output error to file
            with open(fileName, 'a') as f:
                f.write(str(error))
                f.write(traceback.format_exc())

        if defaultConfig.getboolean('send_email'):
            sendEmail(sys.argv[1], fileName)


def sendEmail(report, fileName):

    emailBodyText = ("""
        Hello,

        There was an error generating the "{}" report.
        Please see, if any, the attached error logs.
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
        emailClient.subject = emailClient.add_timestamp(subject)
        emailClient.receiver_to = emailConfig["receiver_to"]
        emailClient.receiver_cc = emailConfig["receiver_cc"]
        emailClient.email_body_text = emailBodyText
        emailClient.email_body_html = emailBodyhtml
        emailClient.attach_file(fileName)

        if defaultConfig.getboolean('SendEmail'):
            if utils.get_platform() == 'Windows':
                emailClient.win32com_send()
            else:
                emailClient.server = emailConfig['server']
                emailClient.port = emailConfig['port']
                emailClient.sender = emailConfig['sender']
                emailClient.email_from = emailConfig["from"]
                emailClient.smtp_send()

    except Exception as e:
        print("Failed to send email.")


if __name__ == '__main__':
    main()

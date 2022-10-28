import os
import logging
import logging.config
import configparser
import yaml
import scripts.utils as utils
from scripts.DBConnection import DBConnection
from scripts.EmailClient import EmailClient

logger = logging.getLogger()
config = configparser.ConfigParser()


class OrionReport(EmailClient):
    def __init__(self, configFile):
        config.read(configFile)

        self.configFile = configFile
        self.defaultConfig = config['DEFAULT']
        self.emailConfig = config[self.defaultConfig['emailInfo']]
        self.dbConfig = config[self.defaultConfig['databaseEnv']]
        self.debugConfig = config['DEBUG']

        self.__receiverToList = []
        self.__receiverCcList = []

        self.reportsFolderPath = None
        self.__initialize()

        self.orionDb = DBConnection(self.dbConfig['dbapi'], self.dbConfig['host'], self.dbConfig['port'],
                                    self.dbConfig['orion_db'], self.dbConfig['orion_user'], self.dbConfig['orion_pwd'])
        self.orionDb.connect()

        if self.dbConfig['tableau_db']:
            self.tableauDb = DBConnection(self.dbConfig['dbapi'], self.dbConfig['host'], self.dbConfig['port'],
                                          self.dbConfig['tableau_db'], self.dbConfig['tableau_user'], self.dbConfig['tableau_pwd'])
            self.tableauDb.connect()

        super().__init__()

    def __initialize(self):
        global scriptFolderName, loggerConfig, logsFolder
        scriptFolderPath = os.path.dirname(self.configFile)
        scriptFolderName = os.path.basename(scriptFolderPath)
        loggerConfig = os.path.join(os.path.dirname(
            scriptFolderPath), "logging.yml")

        try:
            # Setup logging
            logsFolder = None

            with open(loggerConfig, 'r') as stream:

                parsed_yaml = yaml.safe_load(stream)

                if os.path.exists(os.path.join(scriptFolderPath, "logging.yml")):
                    logging.config.dictConfig(parsed_yaml)
                else:
                    if config.has_option('DEBUG', 'logToFile') == True and self.debugConfig.getboolean('logToFile') == False:
                        parsed_yaml['root']['handlers'] = ['console']

                    if config.has_option('DEFAULT', 'logFile') == False:
                        logsFolder = os.path.join('logs', scriptFolderName)
                        utils.createFolder(logsFolder)
                        parsed_yaml['handlers']['timedRotatingFile']['filename'] = os.path.join(
                            logsFolder, f"{scriptFolderName}.log")
                    else:
                        parsed_yaml['handlers']['timedRotatingFile']['filename'] = self.defaultConfig['logFile']

                    logging.config.dictConfig(parsed_yaml)

                logFilePath = os.path.realpath(
                    parsed_yaml['handlers']['timedRotatingFile']['filename'])
                logger.info(f'Log path: {str(logFilePath)}')

                # Set log level
                if config.has_option('DEBUG', 'logLevel') == True:
                    logger.setLevel(self.getLevelNumValue(
                        self.debugConfig['logLevel']))

            # Setup reports folder
            if config.has_option('DEFAULT', 'reportsFolder') == False:
                self.reportsFolderPath = os.path.join(
                    'reports', scriptFolderName)
            else:
                self.reportsFolderPath = self.defaultConfig['reportsFolder']

            self.reportsFolderPath = os.path.realpath(self.reportsFolderPath)
            logger.info(f'Reports folder: {str(self.reportsFolderPath)}')
            utils.createFolder(self.reportsFolderPath)

        except Exception as err:
            logger.exception(err)
            raise Exception(err)

    def getLogLevel(self):

        if logger.level == 10:
            return 'DEBUG'
        elif logger.level == 20:
            return 'INFO'
        elif logger.level == 30:
            return 'WARNING'
        elif logger.level == 40:
            return 'ERROR'
        elif logger.level == 50:
            return 'CRITICAL'
        else:  # 0
            return 'NOTSET'

    def getLevelNumValue(self, level):

        if level.casefold() == 'debug':
            return 10
        elif level.casefold() == 'info':
            return 20
        elif level.casefold() == 'warning':
            return 30
        elif level.casefold() == 'error':
            return 40
        elif level.casefold() == 'critical':
            return 50
        else:  # 'NOTSET'
            return 0

    def createCsvFromDataframe(self, df, csvFilePath):
        logger.info("Generating report " +
                    os.path.basename(csvFilePath) + " ...")
        if self.debugConfig.getboolean('createReport') == True:
            utils.dataframeToCsv(df, csvFilePath)

    def addFileToZip(self, filesToZip, zipFile):
        if self.debugConfig.getboolean('createReport') == True:
            utils.zipFiles(filesToZip, zipFile,
                           self.defaultConfig['zipPassword'])

    def addReceiverTo(self, email):
        self.__receiverToList.append(email)

    def addReceiverCc(self, email):
        self.__receiverCcList.append(email)

    def __emailListToStr(self, emailList):
        emailsStr = ''

        for email in emailList:
            emailsStr = emailsStr + ';' + email

        return emailsStr

    def attachFile(self, attachment):
        if self.debugConfig.getboolean('createReport') == True:
            super().attachFile(attachment)

    def sendEmail(self):
        # Enable/Disable email
        if self.debugConfig.getboolean('sendEmail') == True:
            try:

                if self.defaultConfig['emailInfo'] == 'Email':
                    self.receiverTo = self.emailConfig["receiverTo"] + \
                        self.__emailListToStr(self.__receiverToList)
                    self.receiverCc = self.emailConfig["receiverCc"] + \
                        self.__emailListToStr(self.__receiverCcList)
                else:
                    self.receiverTo = self.emailConfig["receiverTo"]
                    self.receiverCc = self.emailConfig["receiverCc"]

                if self.subject == None:
                    self.subject = self.addTimestamp2("Orion Report")

                if self.emailBodyText == None:
                    self.emailBodyText = """
                        Hello,

                        Please see attached ORION report.


                        Thanks you and best regards,
                        Orion Team
                    """

                if self.emailBodyHtml == None:
                    self.emailBodyhtml = """\
                        <html>
                        <p>Hello,</p>
                        <p>Please see attached ORION report.</p>
                        <p>&nbsp;</p>
                        <p>Thank you and best regards,</p>
                        <p>Orion Team</p>
                        </html>
                        """

                if utils.getPlatform() == 'Windows':
                    self.win32comSend()
                else:
                    self.server = self.emailConfig['server']
                    self.port = self.emailConfig['port']
                    self.sender = self.emailConfig['sender']
                    self.emailFrom = self.emailConfig["from"]
                    self.smtpSend()

            except Exception as e:
                logger.error("Failed to send email.")
                raise Exception(e)

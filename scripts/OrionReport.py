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
        self.emailConfig = config[self.defaultConfig['EmailInfo']]
        self.dbConfig = config[self.defaultConfig['DatabaseEnv']]
        self.debugConfig = config['DEBUG']

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

            with open(loggerConfig, 'r') as stream:

                parsed_yaml = yaml.safe_load(stream)

                if os.path.exists(os.path.join(scriptFolderPath, "logging.yml")):
                    logging.config.dictConfig(parsed_yaml)
                else:
                    if config.has_option('DEBUG', 'logToFile') == True:
                        if self.debugConfig.getboolean('logToFile') == False:
                            parsed_yaml['root']['handlers'] = ['console']

                    if config.has_option('DEFAULT', 'logFile') == False:
                        logsFolder = os.path.join('logs', scriptFolderName)
                        utils.createFolder(logsFolder)
                        parsed_yaml['handlers']['timedRotatingFile']['filename'] = os.path.join(
                            logsFolder, f"{scriptFolderName}.log")
                    else:
                        parsed_yaml['handlers']['timedRotatingFile']['filename'] = self.defaultConfig['logFile']

                    logging.config.dictConfig(parsed_yaml)

                    if config.has_option('DEBUG', 'logLevel') == True:
                        logger.setLevel(self.getLevelNumValue(
                            self.debugConfig['logLevel']))

            if config.has_option('DEFAULT', 'reportsFolder') == False:
                self.reportsFolderPath = os.path.join(
                    'reports', scriptFolderName)
                utils.createFolder(self.reportsFolderPath)
            else:
                self.reportsFolderPath = self.defaultConfig['reportsFolder']
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

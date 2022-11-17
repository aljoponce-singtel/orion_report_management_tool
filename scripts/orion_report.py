# Import built-in packages
import configparser
from datetime import datetime
import logging
import logging.config
import os

# Import local packages
from scripts.helpers import DbConnection
from scripts.helpers import EmailClient
from scripts.helpers import utils

logger = logging.getLogger()
config = configparser.ConfigParser()


class OrionReport(EmailClient):

    receiver_to_list = []
    receiver_cc_list = []

    def __init__(self, config_file):
        config.read(config_file)

        self.config_file = config_file
        self.default_config = config['DEFAULT']
        self.email_config = config[self.default_config['emailInfo']]
        self.db_config = config[self.default_config['databaseEnv']]
        self.debug_config = config['DEBUG']

        self.reports_folder_path = None
        self.log_file_path = None
        self.__initialize()

        self.orionDb = DbConnection(self.db_config['dbapi'], self.db_config['host'], self.db_config['port'],
                                    self.db_config['orion_db'], self.db_config['orion_user'], self.db_config['orion_pwd'])
        self.orionDb.connect()

        if self.db_config['tableau_db']:
            self.tableau_db = DbConnection(self.db_config['dbapi'], self.db_config['host'], self.db_config['port'],
                                           self.db_config['tableau_db'], self.db_config['tableau_user'], self.db_config['tableau_pwd'])
            self.tableau_db.connect()

        super().__init__()

    # Calling destructor
    def __del__(self):
        logger.info("END of script - " +
                    utils.get_current_datetime(format="%a %m/%d/%Y, %H:%M:%S"))

    # private method
    def __initialize(self):

        try:
            self.__setup_logging()

            # START LOGGING OF SCRIPT
            logger.info("==========================================")
            logger.info("START of script - " +
                        utils.get_current_datetime(format="%a %m/%d/%Y, %H:%M:%S"))
            logger.info("Running script in " + utils.get_platform())

            self.__setup_reports_folder()
            utils.create_folder(self.reports_folder_path)

            logger.info(f'Log path: {str(self.log_file_path)}')
            logger.info(f'Reports folder: {str(self.reports_folder_path)}')

        except Exception as err:
            raise Exception(err)

    # private method
    def __setup_logging(self):
        script_folder_path = os.path.dirname(self.config_file)
        script_folder_name = os.path.basename(script_folder_path)
        script_logger_config = os.path.join(script_folder_path, "logging.yml")
        # main_logger_config = os.path.join(os.path.dirname(
        #     script_folder_path), "logging.yml")
        main_logger_config = os.path.join(
            os.path.dirname(__file__), "logging.yml")
        logs_folder = None

        parsed_yaml = utils.read_yaml_file(main_logger_config)

        # If there is a separate logging.yml file in the reports script folder, load this instead.
        if os.path.exists(script_logger_config):
            parsed_yaml = utils.read_yaml_file(script_logger_config)
            logging.config.dictConfig(parsed_yaml)
        # Load the default logging.yml file
        else:
            if config.has_option('DEBUG', 'logToFile') == True and self.debug_config.getboolean('logToFile') == False:
                parsed_yaml['root']['handlers'] = ['console']

            if config.has_option('DEFAULT', 'logFile') == False:
                logs_folder = os.path.join('logs', script_folder_name)
                utils.create_folder(logs_folder)
                parsed_yaml['handlers']['timedRotatingFile']['filename'] = os.path.join(
                    logs_folder, f"{script_folder_name}.log")
            else:
                parsed_yaml['handlers']['timedRotatingFile']['filename'] = self.default_config['logFile']

            logging.config.dictConfig(parsed_yaml)

        self.log_file_path = os.path.realpath(
            parsed_yaml['handlers']['timedRotatingFile']['filename'])

        # Set log level
        if config.has_option('DEBUG', 'logLevel') == True:
            logger.setLevel(self.get_level_num_value(
                self.debug_config['logLevel']))

    # private method
    def __setup_reports_folder(self):
        script_folder_path = os.path.dirname(self.config_file)
        script_folder_name = os.path.basename(script_folder_path)

        if config.has_option('DEFAULT', 'reportsFolder') == False:
            self.reports_folder_path = os.path.join(
                'reports', script_folder_name)
        else:
            self.reports_folder_path = self.default_config['reportsFolder']

        self.reports_folder_path = os.path.realpath(self.reports_folder_path)
        utils.create_folder(self.reports_folder_path)

    def get_log_level(self):

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

    def get_level_num_value(self, level: str):

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

    def create_csv_from_df(self, df, csv_file_path):
        if self.debug_config.getboolean('createReport') == True:
            utils.df_to_csv(df, csv_file_path)

    def add_to_zip_file(self, files_to_zip, zip_file):
        if self.debug_config.getboolean('createReport') == True:
            utils.compress_files(files_to_zip, zip_file,
                                 self.default_config['zipPassword'])

    def add_email_receiver_to(self, email):
        self.receiver_to_list.append(email)

    def add_email_receiver_cc(self, email):
        self.receiver_cc_list.append(email)

    # private method
    def __email_list_to_str(self, email_list):
        email_str = ''

        for email in email_list:
            email_str = email_str + ';' + email

        return email_str

    def attach_file_to_email(self, attachment):
        if self.debug_config.getboolean('createReport') == True:
            super().attach_file(attachment)

    def send_email(self):
        # Enable/Disable email
        if self.debug_config.getboolean('sendEmail') == True:
            try:
                self.server = self.email_config['server']
                self.port = self.email_config['port']
                self.sender = self.email_config['sender']
                self.email_from = self.email_config["from"]

                if self.default_config['emailInfo'] == 'Email':
                    self.receiver_to = self.email_config["receiverTo"] + \
                        self.__email_list_to_str(self.receiver_to_list)
                    self.receiver_cc = self.email_config["receiverCc"] + \
                        self.__email_list_to_str(self.receiver_cc_list)
                else:
                    self.receiver_to = self.email_config["receiverTo"]
                    self.receiver_cc = self.email_config["receiverCc"]

                if self.subject == None:
                    self.subject = self.add_timestamp("Orion Report")

                if self.email_body_text == None:
                    self.email_body_text = """
                        Hello,

                        Please see attached ORION report.


                        Thanks you and best regards,
                        Orion Team
                    """

                if self.email_body_html == None:
                    self.email_body_html = """\
                        <html>
                        <p>Hello,</p>
                        <p>Please see attached ORION report.</p>
                        <p>&nbsp;</p>
                        <p>Thank you and best regards,</p>
                        <p>Orion Team</p>
                        </html>
                        """

                self.send()

            except Exception as e:
                logger.error("Failed to send email.")
                raise Exception(e)

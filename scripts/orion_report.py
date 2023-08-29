# Import built-in packages
import logging
import logging.config
import os
from configparser import ConfigParser, SectionProxy
from os.path import abspath, basename, dirname

# Import third-party packages
import pandas as pd

# Import local packages
from scripts.helpers import DbConnection
from scripts.helpers import EmailClient
from scripts.helpers import utils

logger = logging.getLogger()


class OrionReport(EmailClient):

    config = ConfigParser()
    receiver_to_list = []
    receiver_cc_list = []

    def __init__(self, config_file):

        # Config
        self.config_file = config_file
        self.config = self.__load_config()
        self.default_config: SectionProxy
        self.email_config: SectionProxy
        self.db_config: SectionProxy
        self.debug_config: SectionProxy
        self.default_config = self.config['DEFAULT']
        self.email_config = self.config[self.default_config['email_info']]
        self.db_config = self.config[self.default_config['database_env']]
        self.debug_config = self.config['Debug']

        self.filename = None
        self.report_date = None
        self.start_date = None
        self.end_date = None
        self.reports_folder_path = None
        self.log_file_path = None
        # setup logging and reports folder
        self.__initialize()

        self.orion_db = DbConnection(self.db_config['dbapi'], self.db_config['host'], self.db_config['port'],
                                     self.db_config['orion_db'], self.db_config['orion_user'], self.db_config['orion_pwd'])
        self.orion_db.connect()

        self.staging_db = DbConnection(self.db_config['dbapi'], self.db_config['host'], self.db_config['port'],
                                       self.db_config['staging_db'], self.db_config['orion_user'], self.db_config['orion_pwd'])
        self.staging_db.connect()

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
    def __load_config(self):
        # Load the parent (orion_report.ini) config file
        parent_config_file = abspath(os.path.join(
            dirname(__file__), "./orion_report.ini"))
        parent_config = ConfigParser()
        parent_config.read(parent_config_file)

        # Load the current report (config.ini) config file
        report_config = ConfigParser()
        report_config.read(self.config_file)

        # Create new config
        # Load the parent config first
        self.config.read(parent_config_file)
        # Override the parent config with the report config
        self.config.read(self.config_file)

        return self.config

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
        script_folder_path = dirname(self.config_file)
        script_folder_name = basename(script_folder_path)
        script_logger_config = os.path.join(script_folder_path, "logging.yml")
        # main_logger_config = os.path.join(dirname(
        #     script_folder_path), "logging.yml")
        main_logger_config = os.path.join(
            dirname(__file__), "logging.yml")
        logs_folder = None

        parsed_yaml = utils.read_yaml_file(main_logger_config)

        # If there is a separate logging.yml file in the reports script folder, load this instead.
        if os.path.exists(script_logger_config):
            parsed_yaml = utils.read_yaml_file(script_logger_config)
            logging.config.dictConfig(parsed_yaml)
        # Load the default logging.yml file
        else:
            if self.config.has_option('Debug', 'log_to_file') == True and self.debug_config.getboolean('log_to_file') == False:
                parsed_yaml['root']['handlers'] = ['console']

            if self.config.has_option('DEFAULT', 'log_file') == False:
                logs_folder = os.path.join('logs', script_folder_name)
                utils.create_folder(logs_folder)
                parsed_yaml['handlers']['timedRotatingFile']['filename'] = os.path.join(
                    logs_folder, f"{script_folder_name}.log")
            else:
                parsed_yaml['handlers']['timedRotatingFile']['filename'] = self.default_config['logFile']

            logging.config.dictConfig(parsed_yaml)

        self.log_file_path = abspath(
            parsed_yaml['handlers']['timedRotatingFile']['filename'])

        # Set log level
        if self.config.has_option('Debug', 'log_level') == True:
            logger.setLevel(self.get_level_num_value(
                self.debug_config['log_level']))

    # private method
    def __setup_reports_folder(self):
        script_folder_path = dirname(self.config_file)
        script_folder_name = basename(script_folder_path)

        if self.config.has_option('DEFAULT', 'reports_folder') == False:
            self.reports_folder_path = os.path.join(
                'reports', script_folder_name)
        else:
            self.reports_folder_path = self.default_config['reports_folder']

        self.reports_folder_path = abspath(self.reports_folder_path)
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

    def create_csv_from_df(self, df: pd.DataFrame, file_path=None, filename=None):
        if self.debug_config.getboolean('create_report') == True:
            if filename is None:
                filename = ("{}_{}.csv").format(
                    self.filename, utils.get_current_datetime())
            if file_path is None:
                file_path = os.path.join(
                    self.reports_folder_path, filename)

            utils.df_to_csv(df, file_path)
            logger.debug("CSV file path: " +
                         os.path.dirname(file_path) + " ...")

        return file_path

    def create_excel_from_df(self, df: pd.DataFrame, file_path=None, filename=None, index=False):
        if self.debug_config.getboolean('create_report') == True:
            if filename is None:
                filename = ("{}_{}.xlsx").format(
                    self.filename, utils.get_current_datetime())
            if file_path is None:
                file_path = os.path.join(
                    self.reports_folder_path, filename)

            # Set index=False if you don't want to export the index column
            df.to_excel(file_path, index=index)

            logger.info("Creating file " +
                        os.path.basename(file_path) + " ...")
            logger.debug("Excel file path: " +
                         os.path.dirname(file_path) + " ...")

        return file_path

    def add_to_zip_file(self, files_to_zip, zip_file_path=None, zip_filename=None, password=None):
        if self.debug_config.getboolean('create_report') == True:
            if zip_filename is None:
                zip_filename = ("{}_{}.zip").format(
                    self.filename, utils.get_current_datetime())
            if zip_file_path is None:
                zip_file_path = os.path.join(
                    self.reports_folder_path, zip_filename)
            if password == None:
                password = self.default_config['zip_password']

            utils.compress_files(files_to_zip, zip_file_path, password)
            logger.debug("ZIP file path: " +
                         os.path.dirname(zip_file_path) + " ...")

        return zip_file_path

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
        if self.debug_config.getboolean('create_report') == True:
            super().attach_file(attachment)

    def preview_email_body_html(self, email_body_html, filename, file_path=None):
        html_file_path = None

        if self.debug_config.getboolean('preview_email') == True:
            if file_path is None:
                html_file_path = os.path.join(
                    self.reports_folder_path, filename)
            else:
                html_file_path = os.path.join(file_path, filename)

            self.export_to_html_file(email_body_html, html_file_path)
            utils.open_file_using_default_program(html_file_path)

        return html_file_path

    def send_email(self):
        # Preview email before sending
        self.preview_email_body_html(self.email_body_html, 'email_body.html')

        # Enable/Disable email
        if self.debug_config.getboolean('send_email') == True:
            try:
                self.server = self.email_config['server']
                self.port = self.email_config['port']
                self.sender = self.email_config['sender']
                self.email_from = self.email_config["from"]

                if self.default_config['email_info'] == 'Email':
                    self.receiver_to = self.email_config["receiver_to"] + \
                        self.__email_list_to_str(self.receiver_to_list)
                    self.receiver_cc = self.email_config["receiver_cc"] + \
                        self.__email_list_to_str(self.receiver_cc_list)
                else:
                    self.receiver_to = self.email_config["receiver_to"]
                    self.receiver_cc = self.email_config["receiver_cc"]

                if self.subject == None:
                    self.subject = self.add_timestamp("Orion Report")

                if self.email_body_text == None:
                    self.email_body_text = """
                        Hello,

                        Please see attached ORION report.


                        Best regards,
                        The Orion Team
                    """

                if self.email_body_html == None:
                    self.email_body_html = """\
                        <html>
                        <p>Hello,</p>
                        <p>Please see attached ORION report.</p>
                        <p>&nbsp;</p>
                        <p>Best regards,</p>
                        <p>The Orion Team</p>
                        </html>
                        """

                self.send()

            except Exception as e:
                logger.error("Failed to send email.")
                raise Exception(e)

# Import built-in packages
import logging
import logging.config
import os
from configparser import ConfigParser, SectionProxy
from datetime import datetime
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

    def __init__(self, config_file, report_name='Orion Report'):

        # Config
        self.config_file = config_file
        self.config = self.__load_config()
        self.default_config: SectionProxy
        self.email_config: SectionProxy
        self.email_preview_config: SectionProxy
        self.db_config: SectionProxy
        self.debug_config: SectionProxy
        self.default_config = self.config['DEFAULT']
        self.email_config = self.config[self.default_config['email_info']]
        self.email_preview_config = self.config['Email']
        self.db_config = self.config[self.default_config['database_env']]
        self.debug_config = self.config['Debug']

        self.filename = 'orion_report'
        self.report_name = report_name
        self.report_date = None
        self.start_date = None
        self.end_date = None
        self.reports_folder_path = None
        self.log_file_path = None
        # setup logging and reports folder
        self.__initialize()

        # Connect to Orion DB
        self.orion_db = DbConnection(self.db_config['dbapi'], self.db_config['host'], self.db_config['port'],
                                     self.db_config['orion_db'], self.db_config['orion_user'], self.db_config['orion_pwd'])
        self.orion_db.connect()
        # Connect to Staging DB
        self.staging_db = DbConnection(self.db_config['dbapi'], self.db_config['host'], self.db_config['port'],
                                       self.db_config['staging_db'], self.db_config['orion_user'], self.db_config['orion_pwd'])
        self.staging_db.connect()
        # Connect to Tableau DB
        self.tableau_db = DbConnection(self.db_config['dbapi'], self.db_config['host'], self.db_config['port'],
                                       self.db_config['tableau_db'], self.db_config['tableau_user'], self.db_config['tableau_pwd'])
        self.tableau_db.connect()

        super().__init__()

    # Calling destructor
    def __del__(self):
        logger.info("END of script - " +
                    utils.get_current_datetime(format="%a %m/%d/%Y, %H:%M:%S"))

    # private method
    def __load_config(self) -> ConfigParser:
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
            logger.info(f"/* {self.report_name} */")
            logger.info("Running script in " + utils.get_platform())

            # This file (orion_report.py) will be executed by `run.sh`
            # If this script was manually executed, the parent process will be `bash`
            # If this script was automatically executed or scheduled to run by a cronjob, the parent process will be `crond`
            # was_called_by_cronjob() will return true if this script was called by `crond`
            if not utils.was_called_by_cronjob(os.getppid()):
                logger.warn(f'THIS SCRIPT WAS MANUALLY EXECUTED.')

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

        self.set_reports_folder_path(abspath(self.reports_folder_path))
        utils.create_folder(self.reports_folder_path)

    def set_reports_folder_path(self, path):
        self.reports_folder_path = abspath(path)

    def set_report_name(self, name):
        self.report_name = name

    def set_filename(self, filename):
        self.filename = filename

    def set_report_date(self, date):
        # Convert date input to a date object
        date_obj = utils.to_date_obj(date)
        self.report_date = date_obj

    def set_start_date(self, date):
        # Convert date input to a date object
        date_obj = utils.to_date_obj(date)
        self.start_date = date_obj

    def set_end_date(self, date):
        # Convert date input to a date object
        date_obj = utils.to_date_obj(date)
        self.end_date = date_obj

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

    def insert_df_to_tableau_db(self, df: pd.DataFrame):
        # Allow Tableaue DB update
        if self.debug_config.getboolean('update_tableau_db'):
            try:
                logger.info(
                    'Inserting records to ' + self.db_config['tableau_db'] + '.' + self.default_config['tableau_table'] + ' ...')
                # insert records to DB
                self.tableau_db.insert_df_to_table(
                    df, self.default_config['tableau_table'])

            except Exception as err:
                logger.info("Failed processing DB " + self.db_config['tableau_db'] + ' at ' +
                            self.db_config['tableau_user'] + '@' + self.db_config['host'] + ':' + self.db_config['port'] + '.')
                logger.exception(err)

                raise Exception(err)

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

    def add_to_zip_file(self, files_to_zip, zip_file_path=None, zip_filename=None, password=None, remove_file=True):
        if self.debug_config.getboolean('create_report') == True:
            if zip_filename is None:
                zip_filename = ("{}_{}.zip").format(
                    self.filename, utils.get_current_datetime())
            if zip_file_path is None:
                zip_file_path = os.path.join(
                    self.reports_folder_path, zip_filename)
            if password == None:
                password = self.default_config['zip_password']
            if self.config.has_option('Debug', 'remove_file_after_zip'):
                remove_file = self.debug_config.getboolean(
                    'remove_file_after_zip')

            utils.compress_files(
                files_to_zip, zip_file_path, password, remove_file)
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

    def set_email_subject(self, subject='', add_timestamp=False):

        if not subject:
            subject = self.report_name

        if add_timestamp:
            super().set_email_subject(super().add_timestamp(subject))
        else:
            super().set_email_subject(subject)

    def attach_file_to_email(self, attachment):
        if self.debug_config.getboolean('create_report') == True:
            super().attach_file(attachment)

    def preview_email(self, filename=None, file_path=None, open_file=True):

        preview_html = f"""\
                        <html>
                        <p>Subject: {self.subject}</p>
                        <p>To: {self.email_preview_config["receiver_to"] + self.__email_list_to_str(self.receiver_to_list)}</p>
                        <p>Cc: {self.email_preview_config["receiver_cc"] + self.__email_list_to_str(self.receiver_cc_list)}</p>
                        <p>Attachments: {self.get_file_attachments(include_path=False)}</p>
                        <p>&nbsp;</p>
                        </html>
                        """
        preview_html = preview_html + "\n" + self.email_body_html
        html_file_path = None

        if self.default_config['email_info'] != 'Email':
            self.set_email_subject('TEST ' + self.get_email_subject())
            self.set_email_body_html(preview_html)

        if self.debug_config.getboolean('preview_email') == True:
            if filename is None:
                filename = ('{}.html').format(self.filename)

            if file_path is None:
                html_file_path = os.path.join(
                    self.reports_folder_path, filename)
            else:
                html_file_path = os.path.join(file_path, filename)

            self.export_to_html_file(preview_html, html_file_path)

            if open_file:
                logger.info("Previewing email ...")
                utils.open_file_using_default_program(html_file_path)

        return html_file_path

    def send_email(self):

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
                self.subject = self.report_name

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

            # Preview email before sending
            self.preview_email()

            # Enable/Disable sending email
            if self.debug_config.getboolean('send_email') == True:
                self.send()

        except Exception as e:
            logger.error("Failed to send email.")
            raise Exception(e)

    def set_reporting_date(self):
        if self.debug_config.getboolean('generate_manual_report'):
            logger.warn('REPORT DATES MANUALLY SET BY CONFIG.INI.')
            self.set_report_date(self.debug_config['report_date'])

        else:
            self.set_report_date(datetime.now().date())

        logger.info(f"Generating {self.report_name} ...")
        logger.info("report date: " + str(self.report_date))

        if self.config.has_option('Debug', 'update_tableau_db'):
            logger.info('update_tableau_db = ' +
                        str(self.debug_config.getboolean('update_tableau_db')))

    def set_prev_week_monday_sunday_date(self):
        if self.debug_config.getboolean('generate_manual_report'):
            logger.warn('REPORT DATES MANUALLY SET BY CONFIG.INI.')
            self.set_start_date(self.debug_config['report_start_date'])
            self.set_end_date(self.debug_config['report_end_date'])

        else:
            # Monday date of the week
            start_date, end_date = utils.get_prev_week_monday_sunday_date(
                datetime.now().date())
            self.set_start_date(start_date)
            self.set_end_date(end_date)

        logger.info(f"Generating {self.report_name} ...")
        logger.info("report start date: " + str(self.start_date))
        logger.info("report end date: " + str(self.end_date))

        if self.config.has_option('Debug', 'update_tableau_db'):
            logger.info('update_tableau_db = ' +
                        str(self.debug_config.getboolean('update_tableau_db')))

    def set_prev_month_first_last_day_date(self):
        if self.debug_config.getboolean('generate_manual_report'):
            logger.warn('REPORT DATES MANUALLY SET BY CONFIG.INI.')
            self.set_start_date(self.debug_config['report_start_date'])
            self.set_end_date(self.debug_config['report_end_date'])

        else:
            # 1st day of the month
            start_date, end_date = utils.get_prev_month_first_last_day_date(
                datetime.now().date())
            self.set_start_date(start_date)
            self.set_end_date(end_date)

        logger.info(f"Generating {self.report_name} ...")
        logger.info("report start date: " + str(self.start_date))
        logger.info("report end date: " + str(self.end_date))

        if self.config.has_option('Debug', 'update_tableau_db'):
            logger.info('update_tableau_db = ' +
                        str(self.debug_config.getboolean('update_tableau_db')))

    def set_gsp_billing_month_start_end_date(self):
        if self.debug_config.getboolean('generate_manual_report'):
            logger.warn('REPORT DATES MANUALLY SET BY CONFIG.INI.')
            self.set_start_date(self.debug_config['report_start_date'])
            self.set_end_date(self.debug_config['report_end_date'])

        else:
            # 26th day of the month
            start_date, end_date = self.get_gsp_billing_month_start_end_date(
                datetime.now().date())
            self.set_start_date(start_date)
            self.set_end_date(end_date)

        logger.info(f"Generating {self.report_name} ...")
        logger.info("report start date: " + str(self.start_date))
        logger.info("report end date: " + str(self.end_date))

        if self.config.has_option('Debug', 'update_tableau_db'):
            logger.info('update_tableau_db = ' +
                        str(self.debug_config.getboolean('update_tableau_db')))

    def get_gsp_billing_month_start_end_date(self, date):
        # Example:
        # /* Current date = 2023-01-26 */
        # If day of current date > 25
        # 2022-12-26 = First day of billing month
        # 2023-01-25 = Last day of billing month
        # /* Current date = 2023-01-25 */
        # If day of current date < 26
        # 2022-11-26 = First day of billing month
        # 2022-12-25 = Last day of billing month

        # Convert date input to a date object
        date_obj = utils.to_date_obj(date)
        year = date_obj.year
        month = date_obj.month
        day = date_obj.day
        start_date = None
        end_date = None

        if day > 25:
            start_date = utils.subtract_months(date_obj, 1).replace(day=26)
            end_date = date_obj.replace(day=25)
        else:
            start_date = utils.subtract_months(date_obj, 2).replace(day=26)
            end_date = utils.subtract_months(date_obj, 1).replace(day=25)

        return start_date, end_date

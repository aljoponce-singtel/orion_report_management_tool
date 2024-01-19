# Import built-in packages
import inspect
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

    def __init__(self, report_name='Orion Report', config_file=None):

        super().__init__()

        # Get the caller's frame
        caller_frame = inspect.currentframe().f_back
        # Get the filename of the calling script
        caller_filename = caller_frame.f_code.co_filename
        # Get the scripts > report > project folder path
        self.project_folder_path = os.path.dirname(caller_filename)

        # Config
        self.config, self.parent_config = self.__set_config(config_file)
        self.debug_config: SectionProxy = self.config['Debug']
        self.default_config: SectionProxy = self.config['DEFAULT']
        self.db_config: SectionProxy = self.config['Database']
        self.email_config: SectionProxy = self.__configure_email()
        self.receiver_to_main_list = []
        self.receiver_cc_main_list = []
        self.filename = 'orion_report'
        self.report_name = report_name
        self.report_date = None
        self.start_date = None
        self.end_date = None
        self.reports_folder_path = None
        self.sql_folder_path = os.path.join(self.project_folder_path, 'sql')
        self.log_file_path = None
        # Setup logging and reports folder
        self.__initialize()
        # Connect to Orion DB
        self.orion_db = self.__connect_to_db(self.db_config['orion_db'])
        # Connect to Staging DB
        if self.db_config['staging_db']:
            self.staging_db = self.__connect_to_db(
                self.db_config['staging_db'])
        # Connect to Tableau DB
        if self.db_config['tableau_db']:
            self.tableau_db = self.__connect_to_db(
                self.db_config['tableau_db'])

    # Calling destructor
    def __del__(self):

        logger.info("END of script - " +
                    utils.get_current_datetime(format="%a %m/%d/%Y, %H:%M:%S"))

    # private method
    def __set_config(self, config_file):

        parent_config = ConfigParser()
        report_config = ConfigParser()

        # Load the parent config (orion_report.ini)
        parent_config_file = abspath(
            os.path.join(dirname(__file__), "orion_report.ini"))
        parent_config.read(parent_config_file)

        # If no config_file information is provide, the system will check for the config file in the same project folder by default.
        # For example, if the project folder/name is admin, the system will check for the config file under:
        # scripts > reports > admin
        if config_file == None:
            # Get the default config file from the same directory
            config_file = os.path.join(self.project_folder_path, 'config.ini')
            # Check if config_file does not exists
            if not os.path.exists(config_file):
                # Use orion_report.ini as the default config file
                config_file = parent_config_file

            print(f"Using default config file: {basename(config_file)}")
        # If it does not follow the default folder structure, please provide the config_file path instead.
        else:
            print(f"Using provided config file: {basename(config_file)}")
        # If the parent_config is not the main configuration
        if config_file != parent_config_file:
            # Load the parent (orion_report.ini) config file first
            report_config.read(parent_config_file)

        # The cofiguration from the parent_config (if any) will be overriden by the config in the current project
        report_config.read(config_file)

        return report_config, parent_config

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

            # Setup email
            self.__configure_email()

        except Exception as err:
            raise Exception(err)

    # private method
    def __setup_logging(self):
        script_folder_path = self.project_folder_path
        script_folder_name = basename(script_folder_path)
        script_logger_config = os.path.join(script_folder_path, "logging.yml")
        # main_logger_config = os.path.join(dirname(
        #     script_folder_path), "logging.yml")
        main_logger_config = os.path.join(
            dirname(__file__), "logging.yml")
        logs_folder = None

        # Read the contents from the yaml file
        parsed_yaml = utils.read_yaml_file(main_logger_config)
        # If there is a separate logging.yml file in the script folder, load this file instead.
        if os.path.exists(script_logger_config):
            parsed_yaml = utils.read_yaml_file(script_logger_config)
            logging.config.dictConfig(parsed_yaml)
        # Load the default logging.yml file
        else:
            # Choose whether to output the logs to a file or to a console
            if self.config.has_option('Debug', 'log_to_file') == True and self.debug_config.getboolean('log_to_file') == False:
                parsed_yaml['root']['handlers'] = ['console']
            # If there is no log_file option specified in the config file
            if self.config.has_option('DEFAULT', 'log_file') == False:
                # Save the log file in the default log folder
                logs_folder = os.path.join('logs', script_folder_name)
                # Create a new folder (if not exist) named after the script (project) inside the log folder to save the logs
                utils.create_folder(logs_folder)
                # Name the log_file based on the script (project) name
                parsed_yaml['handlers']['timedRotatingFile']['filename'] = os.path.join(
                    logs_folder, f"{script_folder_name}.log")
            else:
                # Save the log in the specified log_file path
                parsed_yaml['handlers']['timedRotatingFile']['filename'] = self.default_config['log_file']

            # Load the final configuration to the logging config
            logging.config.dictConfig(parsed_yaml)

        # Set the log level
        if self.config.has_option('Debug', 'log_level') == True:
            logger.setLevel(utils.get_level_num_value(
                self.debug_config['log_level']))
        # Set the log file path
        self.log_file_path = abspath(
            parsed_yaml['handlers']['timedRotatingFile']['filename'])

    # private method
    def __setup_reports_folder(self):
        script_folder_path = self.project_folder_path
        script_folder_name = basename(script_folder_path)

        if self.config.has_option('DEFAULT', 'reports_folder') == False:
            self.reports_folder_path = os.path.join(
                'reports', script_folder_name)
        else:
            self.reports_folder_path = self.default_config['reports_folder']

        self.set_reports_folder_path(abspath(self.reports_folder_path))
        utils.create_folder(self.reports_folder_path)

    # private method
    def __connect_to_db(self, db_name):
        # Connect to Orion DB
        db_connection = DbConnection(self.db_config['dbapi'], self.db_config['host'], self.db_config['port'],
                                     db_name, self.db_config['orion_user'], self.db_config['orion_pwd'])
        db_connection.connect()

        return db_connection

    # private method
    def __configure_email(self):
        # Use the email config from orion_report.ini config for testing
        if self.config['Email'].getboolean('use_test_email') == True:
            """
            In case test email recipients are used, keep a copy of the actual email recipients.
            This will be used when previewing the actual email before sending.
            """
            self.receiver_to_main_list.extend(
                self.config['Email'].get("receiver_to").split(";"))
            self.receiver_cc_main_list.extend(
                self.config['Email'].get("receiver_cc").split(";"))
            # Switch to parent_config for testing purposes
            self.config['Email'] = self.parent_config['Email']

        self.add_email_receiver_to(self.config['Email'].get(
            "receiver_to"), is_test_email=False)
        self.add_email_receiver_cc(self.config['Email'].get(
            "receiver_cc"), is_test_email=False)

        self.server = self.config['Email'].get('server')
        self.port = self.config['Email'].get('port')
        self.sender = self.config['Email'].get('sender')
        self.email_from = self.config['Email'].get("from")

        return self.config['Email']

    def set_reports_folder_path(self, path):
        self.reports_folder_path = abspath(path)

    def set_report_name(self, name):
        self.report_name = name

    def set_filename(self, filename, add_timestamp=False):
        if add_timestamp:
            filename = ("{}_{}").format(
                filename, utils.get_current_datetime())
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

    def get_query_from_file(self, filename=None, file_path=None):
        # Only filename is provided
        if file_path is None:
            if not filename:
                raise Exception(
                    "Please provide the SQL filename to extract the query.")
            # Default SQL path + filename will be used
            file_path = os.path.join(
                self.sql_folder_path, filename)
        else:
            # Check if the path exists
            if os.path.exists(file_path):
                # Check if it's a directory
                if os.path.isdir(file_path):
                    # Must provide the filename if file_path is a directory
                    if filename:
                        file_path = os.path.join(file_path, filename)
                        # new file_path does not exist
                        if not os.path.exists(file_path):
                            raise Exception(
                                f"The file/path '{file_path}' does not exist.")
                    else:
                        raise Exception(
                            "Please provide the SQL filename to extract the query.")
            else:
                raise Exception(
                    f"The file/path '{file_path}' does not exist.")

        logger.info(f"Getting query from {basename(file_path)} ...")

        query = utils.file_to_string(file_path)

        return query

    def query_to_dataframe(self, query, db: DbConnection = None, data=None, query_description=None, column_names=[], datetime_to_date=False) -> pd.DataFrame:
        if db == None:
            db = self.orion_db

        return db.query_to_dataframe(
            query, data, query_description, column_names, datetime_to_date)

    def query_to_csv(self, query, db: DbConnection = None, data=None, query_description=None, column_names=[], datetime_to_date=False, file_path=None, filename=None, index=False, add_timestamp=False):
        if db == None:
            db = self.orion_db

        df = db.query_to_dataframe(
            query, data, query_description, column_names, datetime_to_date)

        if not df.empty:
            return self.create_csv_from_df(df, file_path, filename, index, add_timestamp)
        else:
            return None

    def query_to_excel(self, query, db: DbConnection = None, data=None, query_description=None, column_names=[], datetime_to_date=False, file_path=None, filename=None, index=False, add_timestamp=False):
        if db == None:
            db = self.orion_db

        df = db.query_to_dataframe(
            query, data, query_description, column_names, datetime_to_date)

        if not df.empty:
            return self.create_excel_from_df(df, file_path, filename, index, add_timestamp)
        else:
            return None

    def create_csv_from_df(self, df: pd.DataFrame, file_path=None, filename=None, index=False, add_timestamp=False):
        if self.debug_config.getboolean('create_report') == True:
            if filename is None:
                if add_timestamp:
                    filename = ("{}_{}.csv").format(
                        self.filename, utils.get_current_datetime())
                else:
                    filename = ("{}.csv").format(self.filename)
            if file_path is None:
                file_path = os.path.join(
                    self.reports_folder_path, filename)

            utils.df_to_csv(df, file_path, index=index)
            logger.debug("CSV file path: " +
                         os.path.dirname(file_path) + " ...")

        return file_path

    def create_excel_from_df(self, df: pd.DataFrame, file_path=None, filename=None, index=False, add_timestamp=False):
        if self.debug_config.getboolean('create_report') == True:
            if filename is None:
                if add_timestamp:
                    filename = ("{}_{}.xlsx").format(
                        self.filename, utils.get_current_datetime())
                else:
                    filename = ("{}.xlsx").format(self.filename)
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

    def add_to_zip_file(self, files_to_zip, zip_file_path=None, zip_filename=None, password=None, remove_file=True, add_timestamp=False):
        if self.debug_config.getboolean('create_report') == True:
            if zip_filename is None:
                if add_timestamp:
                    zip_filename = ("{}_{}.zip").format(
                        self.filename, utils.get_current_datetime())
                else:
                    zip_filename = ("{}.zip").format(self.filename)
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

    def set_email_subject(self, subject='', add_timestamp=False):

        if not subject:
            subject = self.report_name

        if add_timestamp:
            super().set_email_subject(super().add_timestamp(subject))
        else:
            super().set_email_subject(subject)

    def add_email_receiver_to(self, email: str, is_test_email: bool = False):
        """
        Do not add test emails to the official list of recipients.
        Only add them to the preview list for email preview purposes.
        """
        if is_test_email:
            if self.email_config.getboolean('use_test_email') == False:
                super().add_email_receiver_to(email)
            else:
                self.receiver_to_main_list.extend(email.split(";"))
        else:
            super().add_email_receiver_to(email)

    def add_email_receiver_cc(self, email: str, is_test_email: bool = False):
        """
        Do not add test emails to the official list of recipients.
        Only add them to the preview list for email preview purposes.
        """
        if is_test_email:
            if self.email_config.getboolean('use_test_email') == False:
                super().add_email_receiver_cc(email)
            else:
                self.receiver_cc_main_list.extend(email.split(";"))
        else:
            super().add_email_receiver_cc(email)

    def remove_email_receiver_to(self, email: str):
        super().remove_email_receiver_to(email)
        if email in self.receiver_to_main_list:
            self.receiver_to_main_list.remove(email)

    def remove_email_receiver_cc(self, email: str):
        super().remove_email_receiver_cc(email)
        if email in self.receiver_cc_main_list:
            self.receiver_cc_main_list.remove(email)

    def attach_file_to_email(self, attachment, append_file_ext=None, rm_appended_file=True):
        # Attach the file if the create_report option is true
        if self.debug_config.getboolean('create_report') == True:
            # Temporary file name
            new_attachement = None
            # Check if a new extension type is to be concatenated on the filename
            if append_file_ext:
                # Append the new extension type to the filename
                new_attachement = utils.add_ext_type(
                    attachment, new_extension=append_file_ext)
                # Duplicate the file with the new extension type
                utils.copy_file(attachment, new_attachement)
                # The file to attach will be the new attachement file
                attachment = new_attachement

            # Prevent email attachment of large files
            file_stat = os.stat(attachment)
            # Get the file size in bytes
            filesize_bytes = file_stat.st_size
            # Get the file size in megabytes
            filesize_mbytes = filesize_bytes/1048576
            # Attach the file if less than the max allowed file size
            if (filesize_mbytes < self.default_config.getfloat('email_att_max_size')):
                logger.debug(
                    f"File size for {basename(attachment)} is {filesize_mbytes:.2f} mb in size.")
                super().attach_file(attachment)
            else:
                # Ignore attaching the file but still send the email
                logger.warn(
                    f"FILE TOO LARGE TO ATTACH IN EMAIL: Max is {self.default_config.getint('email_att_max_size')} mb, and {basename(attachment)} is {filesize_mbytes:.2f} mb in size.")

            """
            Check if the temporary file should be removed after sending the email.
            This is mainly used for attachming files created on runtime and works together with the append_file_ext option.
            For example, a query.sql files needs to be attached in the email.
            Because .sql files are prohibited to be attached, it will be duplicated and renamed by appending .txt extension resulting to query.sql.txt.
            The query.sql.txt file will be attached instead.
            Setting rm_appended_file=True (default) will remove/cleanup the recently created query.sql.txt file before the script ends.
            """
            if rm_appended_file:
                # Only remove the newly created file
                if attachment == new_attachement:
                    # Store the attachment to a list to remove at the end of this script
                    self.add_to_list_of_att_to_remove(new_attachement)

        return attachment

    def preview_email(self, filename=None, file_path=None, open_file=True):

        if self.email_config.getboolean('use_test_email'):
            self.set_email_subject('[TEST] ' + self.get_email_subject())

        preview_html = f"""\
                        <html>
                        <p>Subject: {self.subject}</p>
                        <p>To: {self.email_list_to_str(self.receiver_to_main_list)}</p>
                        <p>Cc: {self.email_list_to_str(self.receiver_cc_main_list)}</p>
                        <p>Attachments: {self.get_file_attachments(include_path=False)}</p>
                        <p>&nbsp;</p>
                        </html>
                        """
        preview_html = preview_html + "\n" + self.email_body_html
        html_file_path = None

        if self.email_config.getboolean('use_test_email') == False:
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
                if utils.get_platform() == 'Windows':
                    logger.info("Previewing email ...")
                # Preview HTML using the Edge browser as it is the only allowed browser from Singtel.
                utils.open_file_using_default_program(
                    html_file_path, app_name='Edge')

        return html_file_path

    def send_email(self):

        try:
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
                if self.email_config.getboolean('use_test_email') == False:
                    logger.warn("THIS IS A TEST EMAIL")
                super().send()

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
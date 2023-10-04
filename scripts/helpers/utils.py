# Import built-in packages
import calendar
import logging
import os
import pathlib
import shutil
import subprocess
import sys
import csv
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
from zipfile import ZipFile

# Import third-party packages
import pandas as pd
import psutil
import yaml
from openpyxl import load_workbook
from openpyxl.workbook.protection import WorkbookProtection


logger = logging.getLogger(__name__)


# Converts a list to a string without the brackets
# E.g. ['GSDT7', 'GSDT8'] => -'GSDT7', 'GSDT8'
def list_to_string(list):
    str_list = str(list)
    str_list_no_brackets = str(str_list).replace('[', '').replace(']', '')

    return str_list_no_brackets


def df_to_csv(df: pd.DataFrame, file_path, index=False):
    logger.info("Creating file " + os.path.basename(file_path) + " ...")
    df.to_csv(file_path, index=index)


# Updates the dataframe based from the contents and sequence of the priority list
# Example:
# df_act = pd.DataFrame({'activities': ['activity_1', 'activity_2', 'activity_3',  'activity_4']},
#                       {'order_id': ['id_123', 'id_456', 'id_789',  'id_901']})
# act_seq_list = ['activity_4', 'activity_2']
# Only the rows where the df_act['activities'] is in the act_seq_list will be selected and should also follow the sequence from the list
# df_output = pd.DataFrame({'activities': ['activity_4', 'activity_2']},
#                           {'order_id': ['id_901', 'id_456']})
# There is also a keep_top_record option to keep the top record/s for the purpose of getting only the highest priority
def sort_df_by_priority_sequence(df: pd.DataFrame, column_name: str, priority_seq_list, keep_top_record=True) -> pd.DataFrame:

    df_prio_seq = pd.DataFrame(columns=df.columns)

    # Iterate through the priority list
    for element in priority_seq_list:
        # Gets the unique list of values from the specified column name
        df_list = df[column_name].unique()
        # Checks if the current element from the priority list exist in the dataframe
        if element in df_list:
            # Adds the matched row to the new dataframe
            df_to_add = df[df[column_name] == element]
            df_prio_seq = pd.concat([df_prio_seq, df_to_add])

    # Option to return all records or keep the top record based on priority and sequence
    if keep_top_record:
        # Gets the string value of the top record from the specified column
        top_record = df_prio_seq.head(1)[column_name].iloc[0]
        # Gets the rows that matches the top record
        df_prio_seq = df_prio_seq[df_prio_seq[column_name] == top_record]

    return df_prio_seq


def write_to_csv(csv_file_path, dataset, headers, folder_path):

    try:
        csvfilePath = os.path.join(folder_path, csv_file_path)

        with open(csvfilePath, 'w', newline='') as csv_file:
            spamwriter = csv.writer(
                csv_file,
                delimiter=',',
                quoting=csv.QUOTE_NONNUMERIC)
            spamwriter.writerow(headers)
            spamwriter.writerows(dataset)

    except Exception as e:
        logger.error(e)
        raise Exception(e)


def compress_files(files_to_zip, zip_file_path, password=None, remove_file=True):

    files = []

    if type(files_to_zip) is not list:
        files.append(files_to_zip)
    else:
        files = files_to_zip

    platform = get_platform()

    if platform == "Linux":
        os_zip_file(files, zip_file_path, password, remove_file)
    elif platform == "Windows":
        zip_file(files, zip_file_path, remove_file)
    else:
        raise Exception(f"OS {platform} not supported.")


def zip_file(files_to_zip, zip_file, remove_file=True):

    with ZipFile(zip_file, 'a') as zip_obj:
        for file in files_to_zip:
            logger.info("Adding " + os.path.basename(file) +
                        ' to ' + os.path.basename(zip_file) + ' ...')
            zip_obj.write(filename=file, arcname=os.path.basename(file))

            if remove_file:
                logger.info(f"Deleting file {os.path.basename(file)} ...")
                os.remove(file)


# Only works with Linux
def os_zip_file(files_to_zip, zip_file, password=None, remove_file=True):
    files = ' '.join(files_to_zip)

    for file in files_to_zip:
        logger.info("Adding " + os.path.basename(file) +
                    ' to ' + os.path.basename(zip_file) + ' ...')

    # -j: junk (don't record) directory names
    # -e: encrypt
    # -P: password
    if password:
        os.system("zip -j -e %s %s -P %s" %
                  (zip_file, files, password))
    else:
        os.system("zip -j %s %s" % (zip_file, files))

    if remove_file:
        for file in files_to_zip:
            logger.info(f"Deleting file {os.path.basename(file)} ...")
            os.remove(file)


def get_platform():
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


def get_current_datetime(format=None):
    if format:
        return datetime.now().strftime(format)
    else:
        # sample format - '20220808_1800'
        return datetime.now().strftime("%Y%m%d_%H%M")


def get_prev_week_monday_sunday_date(date):
    # Example:
    # 2023-08-15 = Current date
    # 2023-08-07 = Monday's date from previous week
    # 2023-08-13 = Sunday's date from previous week

    # Convert date input to a date object
    date_obj = to_date_obj(date)
    # Calculate the number of days since the last Monday (0 = Monday, 1 = Tuesday, ..., 6 = Sunday)
    days_since_last_monday = (date_obj.weekday() - 0) % 7
    # Calculate the date of the last week's Monday
    last_week_monday = date_obj - timedelta(days=days_since_last_monday + 7)
    # Calculate the date of the last week's Sunday
    last_week_sunday = date_obj - timedelta(days=days_since_last_monday + 1)

    return last_week_monday, last_week_sunday


def get_prev_month_first_last_day_date(date):
    # Example:
    # 2023-01-25 = Current date
    # 2022-12-01 = First day from previous month
    # 2022-12-31 = Last day from previous month

    # Convert date input to a date object
    date_obj = to_date_obj(date)
    year = date_obj.year
    month = date_obj.month
    day = date_obj.day
    start_date = None
    end_date = None

    # start date
    start_date = subtract_months(date_obj, 1).replace(day=1)

    # end date
    prev_month_date = subtract_months(date_obj, 1)
    end_date = get_last_day_of_month(prev_month_date)

    return start_date, end_date


def subtract_months(date, no_of_months):

    # Convert date input to a date object
    date_obj = to_date_obj(date)
    # Create a relativedelta to subtract the specified number of months
    delta = relativedelta(months=no_of_months)
    # Subtract the delta from the date
    new_date_obj = date_obj - delta
    # Convert the new date object back to a string in the format 'YYYY-MM-DD'
    new_date_str = new_date_obj.strftime('%Y-%m-%d')

    return new_date_obj


def get_last_day_of_month(date):
    # Example:
    # Current date = 2023-01-26 */
    # 2023-01-31 = Date with last day of the month

    date_obj = to_date_obj(date)
    last_day_of_the_month = calendar.monthrange(
        date_obj.year, date_obj.month)[1]

    return date_obj.replace(day=last_day_of_the_month)


def to_date_obj(date_input):
    date_obj = None

    # Convert date input to a date object
    # Is date/datetime
    if (type(date_input) is datetime) or (type(date_input) is date):
        date_obj = date_input
    # Is string
    elif type(date_input) is str:
        try:
            try:
                # Convert to date objecgt
                date_obj = datetime.strptime(date_input, '%Y-%m-%d').date()
            except ValueError:
                # Convert to datetime object
                date_obj = datetime.strptime(date_input, '%Y-%m-%d %H:%M:%S')
        except:
            raise Exception(f"Invalid date: {str(date_input)}")
    else:
        raise Exception(f"Invalid date: {str(date_input)}")

    # Is datetime object
    if isinstance(date_obj, datetime):
        # Is time 00:00:00
        if date_obj.hour == 0 and date_obj.minute == 0 and date_obj.second == 0:
            # Convert to date object
            date_obj = date_obj.date()

    return date_obj


def create_folder(folder_path, overwrite=False):
    # Check if the specified path exists
    is_exist = os.path.exists(folder_path)

    if not is_exist:
        # Create a new directory because it does not exist
        logger.info("Creating directory " + folder_path + ' ...')
        os.makedirs(folder_path)


def scan_files(dir_path, regex=None):

    logger.info(f'Scanning files in {dir_path} ...')

    # to store file names
    filenames = []
    # construct path object
    directory = pathlib.Path(dir_path)

    # iterate directory
    for entry in directory.iterdir():
        # check if it a file
        if entry.is_file():
            filenames.append(entry)
            logger.debug(f'Scanned file = {entry}')

    return filenames


def read_yaml_file(file_path):
    parsed_yaml = None

    with open(file_path, 'r') as stream:
        parsed_yaml = yaml.safe_load(stream)

    return parsed_yaml


def copy_file(source_path, destination_path):
    shutil.copy2(source_path, destination_path)


def replace_extension(filename, new_extension):
    base_name = os.path.splitext(filename)[0]  # Get the base name of the file
    new_filename = f"{base_name}.{new_extension}"  # Create the new filename
    return new_filename


def set_excel_password_linux(file_path, password):
    logger.info('Setting excel password ...')

    try:
        # Open the original Excel file
        workbook = load_workbook(filename=file_path)
        # Set a password for the workbook
        workbook.security = WorkbookProtection(workbookPassword=password)
        # Save and close the workbook
        workbook.save(filename=file_path)
        workbook.close()

    except Exception as e:
        logger.error("Failed to set excel password.")
        raise Exception(e)


def set_excel_password_windows(file_path, password):
    logger.info('Setting excel password ...')

    try:
        import win32com.client as win32
        excel = win32.gencache.EnsureDispatch('Excel.Application')

        # Open the the workbook
        workbook = excel.Workbooks.Open(file_path)
        # Set the password for the workbook
        workbook.Password = password
        # Save the copy of the workbook with a password
        workbook.Save()
        workbook.Close()
        # Quit Excel application
        excel.Quit()

    except Exception as e:
        logger.error("Failed to set excel password.")
        raise Exception(e)


def set_excel_password(file_path, password, replace=True, append_string='_protected'):

    if replace == False:
        protected_file_path = file_path.split(
            '.xlsx')[0] + ('{}.xlsx').format(append_string)
        copy_file(file_path, protected_file_path)
        file_path = protected_file_path

    platform = get_platform()

    if platform == 'Linux':
        set_excel_password_linux(file_path, password)
    elif platform == 'Windows':
        set_excel_password_windows(file_path, password)
    else:
        raise Exception(f"OS {platform} not supported.")


# Only supports Windows for now
def open_file_using_default_program(file_path, app_name=None):

    if get_platform() == 'Windows':
        if app_name:
            if app_name == 'Edge':
                # Specify the path to the Microsoft Edge executable
                edge_exe_path = r'C:\Program Files (x86)\Microsoft\Edge\Application\msedge.exe'
                # Use subprocess to open the HTML file with Microsoft Edge
                subprocess.run([edge_exe_path, file_path])
            elif app_name == 'Chrome':
                # Specify the path to the Google Chrome executable
                chrome_exe_path = r'C:\Program Files\Google\Chrome\Application\chrome.exe'
                # Use subprocess to open the HTML file with Microsoft Edge
                subprocess.run([chrome_exe_path, file_path])
            else:
                # Open the file using the default program
                subprocess.run(["start", "", file_path], shell=True)
        else:
            # Open the file using the default program
            subprocess.run(["start", "", file_path], shell=True)


def check_disk_usage(path):
    # Get disk usage statistics for the specified path
    disk_usage = psutil.disk_usage(path)

    # Calculate the percentage of disk usage
    disk_usage_percentage = (disk_usage.used / disk_usage.total) * 100

    return disk_usage_percentage


def get_ppid_of_process(target_pid):
    # Get the parent process id (only for Linux)
    if get_platform() == 'Linux':
        try:
            # Create the path to the status file of the target process
            status_file_path = f"/proc/{target_pid}/status"

            # Open and read the status file
            with open(status_file_path, "r") as file:
                for line in file:
                    if line.startswith("PPid:"):
                        # Extract the PPID (Parent Process ID)
                        ppid = int(line.split()[1])
                        return ppid
        except FileNotFoundError:
            pass

    return None


def was_called_by_cronjob(pid=None):
    # Check if the parent process ID was executed by a cronjob (crond)
    # Only for Linux
    if get_platform() == 'Linux':
        try:
            # Get the parent process ID (PPID) of the current process
            if pid:
                ppid = get_ppid_of_process(pid)
            else:
                ppid = os.getppid()

            logger.debug(f"ppid: {ppid}")

            if ppid:
                # Check if the parent process is named 'cron'
                with open(f'/proc/{ppid}/comm', 'r') as f:
                    parent_process_name = f.read().strip()
                    logger.debug(f"parent_process_name: {parent_process_name}")
                    if parent_process_name == 'crond':
                        return True

        except FileNotFoundError:
            # If /proc/<PPID>/comm doesn't exist, it's likely not a cron job
            pass

    return False

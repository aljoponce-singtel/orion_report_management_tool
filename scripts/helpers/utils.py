import os
import pathlib
import sys
import logging
import csv
import yaml
import calendar
from datetime import datetime, timedelta, date
from dateutil.relativedelta import relativedelta
from zipfile import ZipFile
import pandas as pd
import shutil
from openpyxl import load_workbook
from openpyxl.workbook.protection import WorkbookProtection

logger = logging.getLogger(__name__)


# Converts a list to a string without the brackets
# E.g. ['GSDT7', 'GSDT8'] => -'GSDT7', 'GSDT8'
def list_to_string(list):
    str_list = str(list)
    str_list_no_brackets = str(str_list).replace('[', '').replace(']', '')

    return str_list_no_brackets


def df_to_csv(df, file_path):
    logger.info("Creating file " + os.path.basename(file_path) + " ...")
    df = pd.DataFrame(df)
    df.to_csv(file_path, index=False)


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


# Legacy/deprecated function
def get_current_datetime_2():
    # sample format - '050622_1845'
    return datetime.now().strftime("%d%m%y_%H%M")


def get_monday_date_from_prev_week(date):
    # Example:
    # 2023-08-15 = Current date
    # 2023-08-07 = Monday's date from previous week

    # Calculate the number of days since the last Monday (0 = Monday, 1 = Tuesday, ..., 6 = Sunday)
    days_since_last_monday = (date.weekday() - 0) % 7
    # Calculate the date of the last week's Monday
    last_week_monday = date - timedelta(days=days_since_last_monday + 7)

    return last_week_monday


def get_sunday_date_from_prev_week(date):
    # Example:
    # 2023-08-15 = Current date
    # 2023-08-13 = Sunday's date from previous week

    # Calculate the number of days since the last Monday (0 = Monday, 1 = Tuesday, ..., 6 = Sunday)
    days_since_last_monday = (date.weekday() - 0) % 7
    # Calculate the date of the last week's Sunday
    last_week_sunday = date - timedelta(days=days_since_last_monday + 1)

    return last_week_sunday


def get_prev_week_start_end_date(date):
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


def get_prev_month_start_end_date(date):
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


def get_billing_month_start_end_date(date):
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
    date_obj = to_date_obj(date)
    year = date_obj.year
    month = date_obj.month
    day = date_obj.day
    start_date = None
    end_date = None

    if day > 25:
        start_date = subtract_months(date_obj, 1).replace(day=26)
        end_date = date_obj.replace(day=25)
    else:
        start_date = subtract_months(date_obj, 2).replace(day=26)
        end_date = subtract_months(date_obj, 1).replace(day=25)

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
    # Convert date input to a date object
    if (type(date_input) is datetime) or (type(date_input) is date):
        return date_input
    elif type(date_input) is str:
        return datetime.strptime(date_input, '%Y-%m-%d')
    else:
        raise Exception("Invalid date")


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

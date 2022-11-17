import os
import pathlib
import sys
import logging
import csv
import yaml
import calendar
from datetime import datetime, timedelta
from zipfile import ZipFile
import pandas as pd

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
    logger.info(f"Creating {os.path.basename(zip_file)} ...")

    with ZipFile(zip_file, 'a') as zip_obj:
        for file in files_to_zip:
            logger.info("Adding " + os.path.basename(file) +
                        ' to ' + os.path.basename(zip_file) + ' ...')
            zip_obj.write(filename=file, arcname=os.path.basename(file))

            if remove_file:
                os.remove(file)


# Only works with Linux
def os_zip_file(files_to_zip, zip_file, password=None, remove_file=True):
    files = ' '.join(files_to_zip)
    logger.info(f"Creating {os.path.basename(zip_file)} ...")

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


def get_first_day_from_prev_month(date):
    # Example:
    # 2022-11-15 = Current date
    # 2022-10-01 = First day from previous month
    prev_month_date = date.replace(day=1) - timedelta(days=1)
    return prev_month_date.replace(day=1)


def get_last_day_from_prev_month(date):
    # Example:
    # 2022-11-15 = Current date
    # 2022-10-31 = Last day from previous month
    prev_month_date = (date.replace(day=1) - timedelta(days=1)
                       ).replace(day=date.day)
    last_day = calendar.monthrange(
        prev_month_date.year, prev_month_date.month)[1]
    return prev_month_date.replace(day=last_day)


def get_start_date_from_prev_month(date):
    # Example:
    # 2022-11-26 = Current date
    # 2022-10-26 = First day from previous month
    return (date.replace(day=1) - timedelta(days=1)).replace(day=date.day)


def get_end_date_from_prev_month(date):
    # Example:
    # 2022-11-26 = Current date
    # 2022-11-25 = First day from previous month
    return date - timedelta(days=1)


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

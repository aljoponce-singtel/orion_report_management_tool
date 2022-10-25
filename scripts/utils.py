import sys
import logging
import os
import csv
import calendar
from datetime import datetime, timedelta
from zipfile import ZipFile
import pandas as pd

logger = logging.getLogger(__name__)


# Converts a list to a string without the brackets
# E.g. ['GSDT7', 'GSDT8'] => -'GSDT7', 'GSDT8'
def listToString(list):
    strList = str(list)
    strListNoBrackets = str(strList).replace('[', '').replace(']', '')

    return strListNoBrackets


def dataframeToCsv(df, filePath):
    logger.info("Creating file " + os.path.basename(filePath) + " ...")
    df = pd.DataFrame(df)
    df.to_csv(filePath, index=False)


def write_to_csv(csvfile, dataset, headers, folderPath):

    try:
        csvfilePath = os.path.join(folderPath, csvfile)

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


def zipFiles(filesToZip, zipFile, password=None):

    files = []

    if type(filesToZip) is not list:
        files.append(filesToZip)
    else:
        files = filesToZip

    if getPlatform() == "Linux":
        os_zip_file(files, zipFile, password)
    elif getPlatform() == "Windows":
        zip_file(files, zipFile)
    else:
        errorMsg = "OS " + os + " not supported."
        logger.error(errorMsg)
        raise Exception(errorMsg)


def zip_file(filesToZip, zipfile, password=None):
    # logger.info("Creating zip file " + os.path.basename(zipfile) + ' ...')

    with ZipFile(zipfile, 'a') as zipObj:
        for file in filesToZip:
            logger.info("Adding " + os.path.basename(file) +
                        ' to ' + os.path.basename(zipfile) + ' ...')
            zipObj.write(filename=file, arcname=os.path.basename(file))
            os.remove(file)


# Linux
def os_zip_file(filesToZip, zipfile, password=None):
    files = ' '.join(filesToZip)

    # -j: junk (don't record) directory names
    # -e: encrypt
    # -P: password
    if password:
        os.system("zip -j -e %s %s -P %s" %
                  (zipfile, files, password))
    else:
        os.system("zip -j %s %s" % (zipfile, files))

    for file in filesToZip:
        os.remove(file)


def getPlatform():
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


def getCurrentDateTime():
    # sample format - '20220808_1800'
    return datetime.now().strftime("%Y%m%d_%H%M")


# Legacy/deprecated function
def getCurrentDateTime2():
    # sample format - '050622_1845'
    return datetime.now().strftime("%d%m%y_%H%M")


def getPrevMonthFirstDayDate(date):
    previousMonthDate = date.replace(day=1) - timedelta(days=1)
    return previousMonthDate.replace(day=1)


def getPrevMonthLastDayDate(date):
    previousMonthDate = (date.replace(day=1) - timedelta(days=1)
                         ).replace(day=date.day)
    lastDay = calendar.monthrange(
        previousMonthDate.year, previousMonthDate.month)[1]
    return previousMonthDate.replace(day=lastDay)


def getPrevMonthStartDate(date):
    return (date.replace(day=1) - timedelta(days=1)).replace(day=date.day)


def getPrevMonthEndDate(date):
    return date - timedelta(days=1)


def createFolder(folderPath, overwrite=False):
    # Check if the specified path exists
    isExist = os.path.exists(folderPath)

    if not isExist:
        # Create a new directory because it does not exist
        logger.info("Creating directory " + folderPath + ' ...')
        os.makedirs(folderPath)

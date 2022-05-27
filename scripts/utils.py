import sys
import logging
import os
import csv
from datetime import datetime
from zipfile import ZipFile

logger = logging.getLogger(__name__)


# Converts a list to a string without the brackets
# E.g. ['GSDT7', 'GSDT8'] => -'GSDT7', 'GSDT8'
def listToString(list):
    strList = str(list)
    strListNoBrackets = str(strList).replace('[','').replace(']','')

    return strListNoBrackets


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


def zipFile(filesToZip, zipfile, folderPath, password):

    logger.info("Creating " + zipfile + " for " +
                ', '.join(filesToZip) + ' ...')
    os.chdir(folderPath)

    if getPlatform() == "Linux":
        os_zip_file(filesToZip, zipfile, password)
    elif getPlatform() == "Windows":
        zip_file(filesToZip, zipfile)
    else:
        errorMsg = "OS " + os + " not supported."
        logger.error(errorMsg)
        raise Exception(errorMsg)

    os.chdir('../')


def zip_file(filesToZip, zipfile):
    with ZipFile(zipfile, 'w') as zipObj:
        for file in filesToZip:
            filePath = file
            zipObj.write(filePath)
            os.remove(filePath)


def os_zip_file(filesToZip, zipfile, password):
    filesToZip = ' '.join(filesToZip)
    os.system("zip -e %s %s -P %s" %
              (zipfile, filesToZip, password))
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
    return datetime.now().strftime("%d%m%y_%H%M")

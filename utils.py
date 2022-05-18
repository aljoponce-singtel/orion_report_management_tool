import sys
import logging
import os
import csv
from datetime import datetime
from zipfile import ZipFile

logger = logging.getLogger(__name__)


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


def zip_file(filesToZip, zipfile, folderPath, password):

    logger.info("Creating " + zipfile + " for " +
                ', '.join(filesToZip) + ' ...')
    os.chdir(folderPath)

    if getPlatform() == "Linux":
        os_zip_csvFile(filesToZip, zipfile, password)
    elif getPlatform() == "Windows":
        zip_csvFile(filesToZip, zipfile)
    else:
        raise Exception("OS " + os + " not supported.")

    os.chdir('../')


def zip_csvFile(filesToZip, zipfile):
    with ZipFile(zipfile, 'w') as zipObj:
        for csv in filesToZip:
            csvfilePath = csv
            zipObj.write(csvfilePath)
            os.remove(csvfilePath)


def os_zip_csvFile(filesToZip, zipfile, password):
    filesToZip = ' '.join(filesToZip)
    os.system("zip -e %s %s -P %s" %
              (zipfile, filesToZip, password))
    for csv in filesToZip:
        os.remove(csv)


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

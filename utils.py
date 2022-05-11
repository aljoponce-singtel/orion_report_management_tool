import sys
import logging
import os
import configparser
from datetime import datetime
from zipfile import ZipFile

logger = logging.getLogger(__name__)
config = configparser.ConfigParser()
config.read('config.ini')
defaultConfig = config['DEFAULT']
logsFolderPath = os.path.join(os.getcwd(), "logs")


def zip_file(csvFiles, zipfile, folderPath):

    logger.info("Creating " + zipfile + " for " +
                ', '.join(csvFiles) + ' ...')
    os.chdir(folderPath)

    if getPlatform() == "Linux":
        os_zip_csvFile(csvFiles, zipfile)
    elif getPlatform() == "Windows":
        zip_csvFile(csvFiles, zipfile)
    else:
        raise Exception("OS " + os + " not supported.")

    os.chdir('../')


def zip_csvFile(csvFiles, zipfile):
    with ZipFile(zipfile, 'w') as zipObj:
        for csv in csvFiles:
            csvfilePath = csv
            zipObj.write(csvfilePath)
            os.remove(csvfilePath)


def os_zip_csvFile(csvFiles, zipfile):
    csvfiles = ' '.join(csvFiles)
    os.system("zip -e %s %s -P %s" %
              (zipfile, csvfiles, defaultConfig['ZipPassword']))
    for csv in csvFiles:
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


def log():
    logger.info("TEST SUCCESS")

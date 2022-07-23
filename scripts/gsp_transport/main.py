import sys
import os
import logging
import configparser

# Getting the name of the directory where this file is present.
current = os.path.dirname(os.path.realpath(__file__))
# Getting the parent directory name where the current directory is present.
parent = os.path.dirname(current)
# Adding the parent directory to the sys.path.
sys.path.append(parent)
# Import modules as recognized from sys.path.
import log
import utils

config = configparser.ConfigParser()
# configFile = 'scripts/gsp_sdwan/config.ini'
configFile = os.path.join(os.path.dirname(
    os.path.realpath(__file__)), 'config.ini')
config.read(configFile)
log.initialize(configFile)
defaultConfig = config['DEFAULT']
logger = logging.getLogger(__name__)


def main():
    print(utils.getPlatform())


if __name__ == '__main__':
    main()

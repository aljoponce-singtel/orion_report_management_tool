# import sys
import os
import logging
import configparser
from datetime import datetime
from scripts import log
from scripts import utils
from scripts.gsp_transport import reports

# # Getting the name of the directory where this file is present.
# current = os.path.dirname(os.path.realpath(__file__))
# # Getting the parent directory name where the current directory is present.
# parent = os.path.dirname(current)
# # Adding the parent directory to the sys.path.
# sys.path.append(parent)
# # Import modules as recognized from sys.path.

# import log
# import utils

config = configparser.ConfigParser()
configFile = 'scripts/gsp_transport/config.ini'
# configFile = os.path.join(os.path.dirname(
#     os.path.realpath(__file__)), 'config.ini')
config.read(configFile)
log.initialize(configFile)
defaultConfig = config['DEFAULT']
logger = logging.getLogger(__name__)


def main():
    logger.info("==========================================")
    logger.info("START of script - " +
                datetime.now().strftime("%a %m/%d/%Y, %H:%M:%S"))
    logger.info("Running script in " + utils.getPlatform())

    today_date = datetime.now().date()


if __name__ == '__main__':
    main()

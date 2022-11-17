# from scripts.reports.gsp_war_room import reports
from helper import generateWarRoomReport
import os
import sys
import logging

# Add the parent (scripts) directory to sys.path
# currentDir = os.path.dirname(os.path.realpath(__file__))
# parentDir = os.path.dirname(currentDir)
# sys.path.append(parentDir)

logger = logging.getLogger(__name__)


def main():

    try:
        generateWarRoomReport()

    except Exception as err:
        logger.exception(err)
        raise Exception(err)


if __name__ == '__main__':
    main()

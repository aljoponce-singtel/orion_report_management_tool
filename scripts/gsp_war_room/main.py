import logging
from scripts.gsp_war_room import reports

logger = logging.getLogger(__name__)


def main():

    try:
        reports.generateWarRoomReport()

    except Exception as err:
        logger.exception(err)
        raise Exception(err)


if __name__ == '__main__':
    main()

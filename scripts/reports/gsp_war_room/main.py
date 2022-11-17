from helper import generateWarRoomReport
import logging

logger = logging.getLogger(__name__)


def main():

    try:
        generateWarRoomReport()

    except Exception as err:
        logger.exception(err)
        raise Exception(err)


if __name__ == '__main__':
    main()

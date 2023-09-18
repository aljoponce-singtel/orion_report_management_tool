# import sys
import logging

# Import local packages
from helpers import check_disk_usage, check_new_queueowners

logger = logging.getLogger(__name__)


def main():

    try:
        check_disk_usage()
        check_new_queueowners()

    except Exception as err:
        logger.exception(err)
        raise Exception(err)


if __name__ == '__main__':
    main()

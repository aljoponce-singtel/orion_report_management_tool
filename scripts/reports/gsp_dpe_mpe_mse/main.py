# import sys
import logging

# Import local packages
from helpers import generate_main_report

logger = logging.getLogger(__name__)


def main():

    try:
        generate_main_report()

    except Exception as err:
        logger.exception(err)
        raise Exception(err)


if __name__ == '__main__':
    main()

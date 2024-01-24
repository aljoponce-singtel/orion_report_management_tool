# import sys
import logging

# Import local packages
from helper import generate_transport_report

logger = logging.getLogger(__name__)


def main():

    try:
        generate_transport_report()

    except Exception as err:
        logger.exception(err)
        raise Exception(err)


if __name__ == '__main__':
    main()

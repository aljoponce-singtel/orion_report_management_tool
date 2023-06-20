# Import built-in packages
import logging

# Import local packages
from helpers import generate_report_44, generate_report_44_no_contacts

logger = logging.getLogger(__name__)


def main():

    try:
        # generate_report_44()
        generate_report_44_no_contacts()

    except Exception as err:
        logger.exception(err)
        raise Exception(err)


if __name__ == '__main__':
    main()

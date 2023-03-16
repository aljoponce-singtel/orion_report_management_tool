# Import built-in packages
import logging

# Import local packages
from helpers import generate_warroom_report

logger = logging.getLogger(__name__)


def main():

    try:
        generate_warroom_report()

    except Exception as err:
        logger.exception(err)
        raise Exception(err)


if __name__ == '__main__':
    main()
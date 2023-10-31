# Import built-in packages
import logging

# Import local packages
from helper import generate_pol_report, generate_pol_non_pm_report

logger = logging.getLogger(__name__)


def main():

    try:
        generate_pol_report()
        # generate_pol_non_pm_report()

    except Exception as err:
        logger.exception(err)
        raise Exception(err)


if __name__ == '__main__':
    main()

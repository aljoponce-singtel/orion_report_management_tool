# import sys
import logging

# Import local packages
from scripts.reports.gsp_ilc_transport.helper import generate_ilc_transport_billing_report

logger = logging.getLogger(__name__)


def main():

    try:
        generate_ilc_transport_billing_report()

    except Exception as err:
        logger.exception(err)
        raise Exception(err)


if __name__ == '__main__':
    main()

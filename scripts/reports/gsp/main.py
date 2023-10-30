import logging

# Import local packages
from helper import generate_sdwan_report, generate_cplus_ip_report, \
    generate_megapop_report, generate_singnet_report, generate_internet_report, \
    generate_stix_report

logger = logging.getLogger(__name__)


def main():

    try:
        generate_sdwan_report()
        # generate_cplus_ip_report()
        # generate_megapop_report()
        # generate_singnet_report()
        # generate_internet_report()
        # generate_stix_report()

    except Exception as err:
        logger.exception(err)
        raise Exception(err)


if __name__ == '__main__':
    main()

# import sys
import logging

# Import local packages
from helpers import check_disk_usage, check_new_queueowners, query_to_file, check_new_product_codes_to_map, get_superusers

logger = logging.getLogger(__name__)


def main():

    try:
        # query_to_file('query_aop.sql', 'gsp_aop', 'GSP AOP Report')
        check_disk_usage()
        check_new_queueowners()
        check_new_product_codes_to_map()
        get_superusers()

    except Exception as err:
        logger.exception(err)
        raise Exception(err)


if __name__ == '__main__':
    main()

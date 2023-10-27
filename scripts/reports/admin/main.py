# import sys
import logging

# Import local packages
from helpers import check_disk_usage, check_new_queueowners, query_to_file, \
    check_new_product_codes_to_map, get_superusers, load_csv_to_table, test_email

logger = logging.getLogger(__name__)


def main():

    try:
        # query_to_file('query_aop.sql', 'gsp_aop', 'GSP AOP Report')
        # query_to_file('query_aop_poe.sql', 'gsp_aop', 'GSP AOP Report')
        check_disk_usage()
        check_new_queueowners()
        check_new_product_codes_to_map()
        get_superusers()
        # load_csv_to_table('esom_order_status_20231004.csv',
        #                   'tmp_esom_order_status')
        # test_email("EdmsOutlook@edmssingtel.onmicrosoft.com;jamesp@prem-grp.com")

    except Exception as err:
        logger.exception(err)
        raise Exception(err)


if __name__ == '__main__':
    main()

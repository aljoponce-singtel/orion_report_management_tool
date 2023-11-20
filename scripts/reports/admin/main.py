# Import built-in packages
import logging

# Import local packages
from helpers import check_disk_usage, check_new_queueowners, query_to_csv, \
    check_new_product_codes_to_map, get_superusers, load_csv_to_table, test_email, \
    query_to_excel

logger = logging.getLogger(__name__)


def main():

    try:
        # check_disk_usage()
        # check_new_queueowners()
        # check_new_product_codes_to_map()
        # get_superusers()

        '''
        CMDB CI
        '''
        # load_csv_to_table(
        #     r'C:\Users\p1319639\OneDrive - Singtel\Documents\Orion\CMDB_CI\20231110\cmdb_ci_final.csv', 'tmp_cmdb_ci_final', ['ci_id', 'order_code'])
        # query_to_csv(
        #     r'C:\Users\p1319639\OneDrive - Singtel\Documents\Orion\CMDB_CI\20231110\query_ci_cd.sql', "cmdb_ci_final")

        '''
        ESOM ORDER STATUS
        '''
        columns: list = [
            'order_code',
            'job_effective_date',
            'completed_date',
            'close_date',
            'group_id',
            'activity_code',
            'activity_name',
            'activity_status',
            'due_date',
            'exe_date',
            'dly_date',
            'last_updated_id',
            'act_status_change_date'
        ]
        datetime_colums: list = [
            'job_effective_date',
            'completed_date',
            'close_date',
            'due_date',
            'exe_date',
            'dly_date',
            'act_status_change_date'
        ]
        # datetime_colums: list = None
        # load_csv_to_table(r'C:\Users\p1319639\OneDrive - Singtel\Documents\Orion\ETL\20231114_esom_order_status\esom_order_status_v4.csv',
        #                   'tmp_esom_order_status_20231114', columns=columns, date_columns=datetime_colums)
        # query_to_file(
        #     r'C:\Users\p1319639\OneDrive - Singtel\Documents\Orion\ETL\20231107_esom_order_status\query_4.sql', "esom_order_status", "ESOM Order Status")
        # query_to_csv(
        #     r'C:\Users\p1319639\OneDrive - Singtel\Documents\Orion\ETL\20231114_esom_order_status\ord_esom_compare.sql', "missing_orders", "MISSING ORDERS")

        '''
        FOR TESTING
        '''
        # query_to_csv('query_aop.sql', 'gsp_aop', 'GSP AOP Report')
        # query_to_csv('query_aop_poe.sql', 'gsp_aop', 'GSP AOP Report')
        # test_email("EdmsOutlook@edmssingtel.onmicrosoft.com;jamesp@prem-grp.com")
        # test_email("aljo.ponce@singtel.com", "POL_PM_Managed_161123_1312.zip")
        # query_to_csv(
        #     r'C:\Users\p1319639\Development\orion_report_management_tool\scripts\reports\gsp_trx_ce_survey\sql\query.sql', "trx_ce_survey_no_evolve", "TRX CE SURVEY")
        # query_to_excel(
        #     r'C:\Users\p1319639\OneDrive - Singtel\Documents\Orion\Auto-Escalation\20231108_alanlam_replacement\query.sql', "level_3_update", "LEVEL 3 UPDATE")

    except Exception as err:
        logger.exception(err)
        raise Exception(err)


if __name__ == '__main__':
    main()

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
            'work_order_status',
            'order_status',
            'ord_creation_date',
            'job_effective_date',
            'completed_date',
            'closed_date',
            'group_id',
            'activity_no',
            'activity_name',
            'activity_status',
            'due_date',
            'assigned_date',
            'exe_date',
            'dly_date',
            'com_date',
            'act_completed_date',
            'act_status_change_date',
            'last_updated_id',
            'last_updated_date'
        ]
        datetime_colums: list = [
            'ord_creation_date',
            'job_effective_date',
            'completed_date',
            'closed_date',
            'due_date',
            'assigned_date',
            'exe_date',
            'dly_date',
            'com_date',
            'act_completed_date',
            'act_status_change_date',
            'last_updated_date'
        ]
        date_colums: list = [
            'ord_creation_date',
            'job_effective_date',
            'completed_date',
            'closed_date',
            'due_date',
            'assigned_date',
            'exe_date',
            'dly_date',
            'com_date',
            'act_completed_date',
            'act_status_change_date',
            'last_updated_date'
        ]
        # datetime_colums: list = None
        # load_csv_to_table(r'C:\Users\p1319639\OneDrive - Singtel\Documents\Orion\ETL\20231130_esom_order_status\esom_order_status.csv',
        #                   'esom_order_status_20231130', columns=columns, date_columns=date_colums, chunk_size=5000)
        # query_to_file(
        #     r'C:\Users\p1319639\OneDrive - Singtel\Documents\Orion\ETL\20231107_esom_order_status\query_4.sql', "esom_order_status", "ESOM Order Status")
        # query_to_csv(
        #     r'C:\Users\p1319639\OneDrive - Singtel\Documents\Orion\ETL\20231114_esom_order_status\ord_esom_compare.sql', "missing_orders", "MISSING ORDERS")
        # query_to_csv(
        #     r'C:\Users\p1319639\OneDrive - Singtel\Documents\Orion\ETL\20231130_esom_order_status\order_select_date_reversed_all.sql', "reversed_dates", "REVERSED DATES")

        '''
        PRODUCT CODE MAPPING
        '''
        # load_csv_to_table(csv_file=r'C:\Users\p1319639\OneDrive - Singtel\Documents\Orion\Products\20231130_voice_uc\product_mapping_20231130.csv', database_name="o2ptest",
        #                   table_name='product_mapping_20231130')

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

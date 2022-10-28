import os
import sys
from datetime import datetime
import logging
from scripts.OrionReport import OrionReport
import pandas as pd
import numpy as np
import constants as const
from sqlalchemy import select, case, and_, or_, null, func
from sqlalchemy.types import Integer
from scripts import utils

logger = logging.getLogger(__name__)

configFile = os.path.join(os.path.dirname(__file__), 'config.ini')


def generateWarRoomReport():

    orionReport = OrionReport(configFile)

    emailSubject = 'GSP War Room Report'
    fileName = 'gsp_warroom_report'
    startDate = None
    endDate = None

    if orionReport.debugConfig.getboolean('genReportManually'):
        logger.info('\\* MANUAL RUN *\\')
        startDate = orionReport.debugConfig['reportStartDate']
        endDate = orionReport.debugConfig['reportEndDate']

    else:
        startDate = str(utils.getPrevMonthFirstDayDate(
            datetime.now().date()))
        endDate = str(utils.getPrevMonthLastDayDate(datetime.now().date()))

    logger.info("start date: " + str(startDate))
    logger.info("end date: " + str(endDate))

    # query = "SELECT COUNT(id) FROM RestInterface_person WHERE role = 'GIP_KR'"

    # person_table = orionDb.getTableMetadata('RestInterface_person')
    # query = select([func.count()]).select_from(
    #     person_table).where(person_table.c.role == 'GIP_KR')

    # result = orionDb.queryToList(query)
    # df = pd.DataFrame(data=result)

    # print(df)

    # return

    gsp_q_own_table = orionReport.orionDb.getTableMetadata('GSP_Q_ownership')
    # query = select(gsp_q_own_table)
    query = select(gsp_q_own_table.c.group_id,
                   gsp_q_own_table.c.department).select_from(gsp_q_own_table)
    orionReport.orionDb.logFullQuery(query)
    result = orionReport.orionDb.queryToList(query)
    df_gsp_q_own = pd.DataFrame(data=result)
    # print(df_gsp_q_own[['group_id', 'department']])
    # print(df_gsp_q_own)

    query = ("""
                SELECT
                    DISTINCT ORD.order_code,
                    ORD.service_number,
                    CUS.name AS customer,
                    ORD.order_type,
                    ORD.order_status,
                    ORD.taken_date,
                    ORD.current_crd,
                    ORD.initial_crd,
                    SINOTE.date_created AS crd_amendment_date,
                    SINOTE.details AS crd_amendment_details,
                    REGEXP_SUBSTR(SINOTE.details, '(?<=Old CRD:)(.*)(?= New CRD:)') AS old_crd,
                    REGEXP_SUBSTR(SINOTE.details, '(?<=New CRD:)(.*)(?= Category Code:)') AS new_crd,
                    NOTEDLY.reason_gsp AS crd_amendment_reason,
                    NOTEDLY.reason_gsp AS crd_amendment_reason_gsp,
                    ORD.assignee,
                    PRJ.project_code,
                    CKT.circuit_code,
                    PRD.network_product_code AS product_code,
                    PRD.network_product_desc AS product_description,
                    ORD.business_sector,
                    SITE.site_code AS exchange_code_a,
                    SITE.site_code_second AS exchange_code_b,
                    BRN.brn,
                    ORD.sde_received_date,
                    ORD.arbor_disp AS arbor_service,
                    ORD.service_type,
                    ORD.order_priority,
                    PAR.parameter_value AS ed_pd_diversity,
                    GRP.department,
                    PER.role AS group_id,
                    CAST(ACT.activity_code AS SIGNED INTEGER) AS step_no,
                    ACT.name AS activity_name,
                    ACT.due_date,
                    ACT.status,
                    ACT.ready_date,
                    ACT.completed_date,
                    RMK.created_at AS act_dly_reason_date,
                    ACTDLY.reason AS act_delay_reason
                FROM
                    RestInterface_order ORD
                    JOIN RestInterface_activity ACT ON ACT.order_id = ORD.id
                    LEFT JOIN RestInterface_person PER ON PER.id = ACT.person_id
                    LEFT JOIN auto_escalation_remarks RMK ON RMK.activity_id = ACT.id
                    LEFT JOIN auto_escalation_queueownerdelayreasons ACTDLY ON RMK.delay_reason_id = ACTDLY.id
                    LEFT JOIN auto_escalation_escalationgroup GRP ON GRP.person_id = PER.id
                    LEFT JOIN RestInterface_ordersinote SINOTE ON SINOTE.order_id = ORD.id
                    AND SINOTE.categoty = 'CRD'
                    AND SINOTE.sub_categoty = 'CRD Change History'
                    AND SINOTE.reason_code IS NOT NULL
                    AND DATE_FORMAT(SINOTE.date_created, '%y-%m') = DATE_FORMAT(NOW(), '%y-%m')
                    LEFT JOIN RestInterface_delayreason NOTEDLY ON NOTEDLY.code = SINOTE.reason_code
                    LEFT JOIN RestInterface_project PRJ ON ORD.project_id = PRJ.id
                    LEFT JOIN RestInterface_circuit CKT ON ORD.circuit_id = CKT.id
                    LEFT JOIN RestInterface_customer CUS ON CUS.id = ORD.customer_id
                    LEFT JOIN RestInterface_site SITE ON SITE.id = ORD.site_id
                    LEFT JOIN RestInterface_customerbrnmapping BRN ON BRN.id = ORD.customer_brn_id
                    LEFT JOIN RestInterface_npp NPP ON NPP.order_id = ORD.id
                    AND NPP.level = 'Mainline'
                    AND NPP.status != 'Cancel'
                    LEFT JOIN RestInterface_product PRD ON PRD.id = NPP.product_id
                    LEFT JOIN RestInterface_parameter PAR ON PAR.npp_id = NPP.id
                    AND PAR.parameter_name = 'Type'
                    AND PAR.parameter_value IN ('1', '2', '010', '020')
                WHERE
                    ACT.tag_name = 'Pegasus'
                    AND ORD.order_status IN (
                        'Submitted',
                        'PONR',
                        'Pending Cancellation',
                        'Completed'
                    )
                    -- AND ORD.current_crd <= DATE_ADD(NOW(), INTERVAL 3 MONTH);
                    AND ORD.current_crd BETWEEN NOW() AND DATE_ADD(NOW(), INTERVAL 1 DAY);
                    -- AND ORD.order_code = 'YPD3691001';
                    -- AND ORD.order_code IN ('YQC7084002');
            """)

    result = orionReport.orionDb.queryToList(query)
    df_raw = pd.DataFrame(data=result)

    # # logger.info(df_raw['crd_amendment_details'])
    # # logger.info(df_raw['old_crd'])
    # # logger.info(df_raw['new_crd'])

    # for index, row in df_raw.iterrows():
    #     logger.info(row['crd_amendment_details'])

    #     df_date = pd.DataFrame(df_raw.loc[index])

    #     # logger.info(df_date)

    #     # logger.info(df_date.loc['old_crd'])
    #     # logger.info(df_date.loc['new_crd'])

    #     # pd.to_datetime(df_date.loc['old_crd'])

    # return

    # set columns to datetime type
    df_raw[const.DATE_COLUMNS] = df_raw[const.DATE_COLUMNS].apply(
        pd.to_datetime)

    # convert datetime to date (remove time)
    df_raw['act_dly_reason_date'] = pd.to_datetime(
        df_raw['act_dly_reason_date']).dt.date

    # Insert new column 'department_gsp' after 'department' column
    # and set to empty default value
    # df_raw.insert(loc=df_raw.columns.get_loc('department') + 1,
    #               column='department_gsp', value=np.nan)
    # print(df_raw.columns)

    # Rename 'department' column to 'department_gsp'
    df_gsp_q_own = df_gsp_q_own.rename(
        columns={'department': 'department_gsp'})
    # print(df_gsp_q_own.columns)

    df_final = df_raw.merge(df_gsp_q_own, how='left', on=['group_id'])
    # df_final = pd.merge(df_raw, df_gsp_q_own, how='left', left_index=True, right_index=True)

    # print(df_final.columns)
    # print(df_final)

    # Write to CSV
    df_main = df_final[const.MAIN_COLUMNS]
    df_main = df_main.sort_values(
        by=['current_crd', 'order_code', 'step_no'], ascending=[False, True, True])
    csvFile = ("{}_{}.csv").format(fileName, utils.getCurrentDateTime())
    csvMainFilePath = os.path.join(orionReport.reportsFolderPath, csvFile)
    orionReport.createCsvFromDataframe(df_main, csvMainFilePath)

    df_crd_amendment = df_final[const.CRD_AMENDMENT_COLUMNS].drop_duplicates().dropna(
        subset=['crd_amendment_date'])
    df_crd_amendment = df_crd_amendment.sort_values(
        by=['order_code', 'crd_amendment_date'], ascending=[True, False])
    csvFile = ("{}_crd_amendments_{}.csv").format(
        fileName, utils.getCurrentDateTime())
    csvAmdFilePath = os.path.join(orionReport.reportsFolderPath, csvFile)
    orionReport.createCsvFromDataframe(df_crd_amendment, csvAmdFilePath)

    # Compress files
    zipFile = ("{}_{}.zip").format(fileName, utils.getCurrentDateTime())
    zipFilePath = os.path.join(orionReport.reportsFolderPath, zipFile)
    orionReport.addFileToZip(csvMainFilePath, zipFilePath)
    orionReport.addFileToZip(csvAmdFilePath, zipFilePath)

    # Send Email
    orionReport.setEmailSubject(orionReport.addTimestamp(emailSubject))
    orionReport.attachFile(zipFilePath)
    # orionReport.attachFile(csvMainFilePath)
    # orionReport.attachFile(csvAmdFilePath)
    # orionReport.addReceiverTo('test1@singtel.com')
    # orionReport.addReceiverCc('test2@singtel.com')
    orionReport.sendEmail()

    return


def generateWarRoomNonGovReport(fileName, startDate, endDate, emailSubject):

    return

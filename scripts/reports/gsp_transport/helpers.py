# Import built-in packages
import os
from datetime import datetime
import logging

# Import third-party packages
import numpy as np
import pandas as pd
from sqlalchemy import select, case, and_, or_, null, func, Integer

# Import local packages
import constants as const
from scripts.helpers import utils
from scripts.orion_report import OrionReport
from models import TransportBase

logger = logging.getLogger(__name__)
configFile = os.path.join(os.path.dirname(__file__), 'config.ini')


def updateTableauDB(report, dataframe):
    # Allow Tableaue DB update
    if report.debug_config.getboolean('update_tableau_db'):
        try:
            logger.info(
                'Inserting records to ' + report.db_config['tableau_db'] + '.' + report.default_config['tableau_table'] + ' ...')

            df = pd.DataFrame(dataframe)

            # add new column
            df["update_time"] = pd.Timestamp.now()

            # set columns to datetime type
            df[const.TABLEAU_DATE_COLUMNS] = df[const.TABLEAU_DATE_COLUMNS].apply(
                pd.to_datetime)

            # set empty values to null
            df.replace('', None)
            # insert records to DB
            report.tableau_db.insert_df_to_table(
                df, report.default_config['tableau_table'])

            # logger.info("TableauDB Updated for " + report_id.lower())

        except Exception as err:
            logger.info("Failed processing DB " + report.db_config['tableau_db'] + ' at ' +
                        report.db_config['tableau_user'] + '@' + report.db_config['host'] + ':' + report.db_config['port'] + '.')
            logger.exception(err)

            raise Exception(err)


def generate_transport_report():

    report = OrionReport(configFile)

    email_subject = 'Transport Report'
    filename = 'transport_report'
    start_date = None
    end_date = None

    if report.debug_config.getboolean('generate_manual_report'):
        logger.info('\\* MANUAL RUN *\\')

        start_date = report.debug_config['report_start_date']
        end_date = report.debug_config['report_end_date']

        report.debug_config['update_tableau_db'] = 'false'

    else:
        if datetime.now().date().day == 26:  # 26th of the month
            start_date = utils.get_start_date_from_prev_month(
                datetime.now().date())
            end_date = utils.get_end_date_from_prev_month(
                datetime.now().date())
            report.debug_config['update_tableau_db'] = 'false'
        else:  # 1st of the month
            start_date = utils.get_first_day_from_prev_month(
                datetime.now().date())
            end_date = utils.get_last_day_from_prev_month(
                datetime.now().date())
            report.debug_config['update_tableau_db'] = 'true'

    logger.info("report start date: " + str(start_date))
    logger.info("report end date: " + str(end_date))

    logger.info('update_tableau_db = ' +
                str(report.debug_config.getboolean('update_tableau_db')))

    logger.info("Generating report ...")

    df_order_id = getTransportOrders(report, start_date, end_date)
    df = pd.DataFrame(getTransportRecords(
        report, df_order_id['order_id'].to_list(), start_date, end_date))
    df_finalReport = pd.DataFrame(columns=const.FINAL_COLUMNS)
    df_orders = df[['Service', 'OrderCode', 'CustomerName', 'CRD',
                   'ServiceNumber', 'OrderStatus', 'OrderType', 'ProductCode']]

    for index, row in df_orders.drop_duplicates().iterrows():
        df_activities = df[df['OrderCode'] == row['OrderCode']]

        service = row['Service']
        orderCode = row['OrderCode']
        customer = row['CustomerName']
        crd = row['CRD']
        serviceNumber = row['ServiceNumber']
        orderStatus = row['OrderStatus']
        orderType = row['OrderType']
        productCode = row['ProductCode']

        if df_orders.at[index, 'Service'] == 'Diginet':
            coordGroupId, coordTeam, coordActName, coordActStatus, coordActDueDate, coordActCOMDate = getActRecord(
                df_activities, ['GSDT Co-ordination Wrk-BQ'])

            if df_orders.at[index, 'OrderType'] == 'Provide' or df_orders.at[index, 'OrderType'] == 'Change':
                preConfigGroupId, preConfigTeam, preConfigActName, preConfigStatus, preConfigDueDate, preConfigCOMDate = getActRecord(
                    df_activities, ['Circuit Creation'])

            if df_orders.at[index, 'OrderType'] == 'Cease':
                preConfigGroupId, preConfigTeam, preConfigActName, preConfigStatus, preConfigDueDate, preConfigCOMDate = getActRecord(
                    df_activities, ['Node & Cct Del (DN-ISDN)', 'Node & Cct Deletion (DN)'])

        if df_orders.at[index, 'Service'] == 'MetroE':
            coordGroupId, coordTeam, coordActName, coordActStatus, coordActDueDate, coordActCOMDate = getActRecord(
                df_activities, ['GSDT Co-ordination Wrk-BQ'])

            if df_orders.at[index, 'OrderType'] == 'Provide':
                preConfigGroupId, preConfigTeam, preConfigActName, preConfigStatus, preConfigDueDate, preConfigCOMDate = getActRecord(
                    df_activities, ['Circuit Creation'])

            if df_orders.at[index, 'OrderType'] == 'Change':
                preConfigGroupId, preConfigTeam, preConfigActName, preConfigStatus, preConfigDueDate, preConfigCOMDate = getActRecord(
                    df_activities, ['Circuit Creation', 'Change Speed Configure'])

            if df_orders.at[index, 'OrderType'] == 'Cease':
                preConfigGroupId, preConfigTeam, preConfigActName, preConfigStatus, preConfigDueDate, preConfigCOMDate = getActRecord(
                    df_activities, ['Node & Circuit Deletion'])

        if df_orders.at[index, 'Service'] == 'MegaPop (CE)':
            coordGroupId, coordTeam, coordActName, coordActStatus, coordActDueDate, coordActCOMDate = getActRecord(
                df_activities, [])

            if df_orders.at[index, 'OrderType'] == 'Provide' or df_orders.at[index, 'OrderType'] == 'Change':
                preConfigGroupId, preConfigTeam, preConfigActName, preConfigStatus, preConfigDueDate, preConfigCOMDate = getActRecord(
                    df_activities, ['Circuit Creation'])

            if df_orders.at[index, 'OrderType'] == 'Cease':
                preConfigGroupId, preConfigTeam, preConfigActName, preConfigStatus, preConfigDueDate, preConfigCOMDate = getActRecord(
                    df_activities, ['Node & Circuit Deletion'])

        if df_orders.at[index, 'Service'] == 'Gigawave':
            coordGroupId, coordTeam, coordActName, coordActStatus, coordActDueDate, coordActCOMDate = getActRecord(
                df_activities, ['GSDT Co-ordination Work'])

            if df_orders.at[index, 'OrderType'] == 'Provide':
                preConfigGroupId, preConfigTeam, preConfigActName, preConfigStatus, preConfigDueDate, preConfigCOMDate = getActRecord(
                    df_activities, ['Circuit Creation'])

            if df_orders.at[index, 'OrderType'] == 'Cease':
                preConfigGroupId, preConfigTeam, preConfigActName, preConfigStatus, preConfigDueDate, preConfigCOMDate = getActRecord(
                    df_activities, ['Circuit Removal from NMS'])

        reportData = [
            service,
            orderCode,
            customer,
            crd,
            serviceNumber,
            orderStatus,
            orderType,
            productCode,
            preConfigGroupId,
            preConfigTeam,
            preConfigActName,
            preConfigStatus,
            preConfigDueDate,
            preConfigCOMDate,
            coordGroupId,
            coordTeam,
            coordActName,
            coordActStatus,
            coordActDueDate,
            coordActCOMDate
        ]

        # add new data (df_toAdd) to df_report
        df_toAdd = pd.DataFrame(
            data=[reportData], columns=const.FINAL_COLUMNS)
        df_finalReport = pd.concat([df_finalReport, df_toAdd])

    # Insert records to tableau db
    updateTableauDB(report, df_finalReport)

    # Write to CSV for Warroom Report
    csv_file = ("{}_{}.csv").format(filename, utils.get_current_datetime())
    csv_main_file_path = os.path.join(report.reports_folder_path, csv_file)
    report.create_csv_from_df(
        df_finalReport[const.FINAL_COLUMNS], csv_main_file_path)

    # Add CSV to zip file
    zip_file = ("{}_{}.zip").format(filename, utils.get_current_datetime())
    zip_file_path = os.path.join(report.reports_folder_path, zip_file)
    report.add_to_zip_file(csv_main_file_path, zip_file_path)

    # Send Email
    report.set_email_subject(report.add_timestamp(email_subject))
    report.attach_file_to_email(zip_file_path)
    report.send_email()


def getActRecord(df, activities):
    df_activities = pd.DataFrame(df)
    df_activities = df_activities[df_activities['ActName'].isin(activities)]

    # If there are multiple records, keep only 1 based on priority
    if len(df_activities) > 1:
        # If there are multiple records of the same activity name,
        # keep the activity with the highest step_no
        df_sorted = df_activities.sort_values(by=['ActStepNo', 'ActName'])
        df_dropped_duplicates = df_sorted.drop_duplicates(
            subset=['ActName'], keep='last')

        # If there are multiple records of different activity name,
        # select the 1st activity based from the priority sequence in [activities]

        # Configure 'ActName' column to follow priority sequence from [activities]
        df_actPriority = pd.DataFrame(df_dropped_duplicates)
        df_actPriority['ActName'] = pd.Categorical(
            values=df_actPriority['ActName'], categories=activities)
        # Sort rows by 'ActName' priority
        df_actPrioritySorted = df_actPriority.sort_values(
            by=['ActName'])
        # Keep only 1 activity based from priority (1st row)
        df_activity = df_actPrioritySorted.head(1)
        df_activities = df_activity

    if np.size(df_activities['GroupID'].values):
        actGroupId = df_activities['GroupID'].values[0]

        if str(actGroupId).startswith('ODC_'):
            actTeam = 'ODC'
        elif str(actGroupId).startswith('RDC_'):
            actTeam = 'RDC'
        else:
            actTeam = 'SGP'
    else:
        actGroupId = None
        actTeam = None

    actName = df_activities['ActName'].values[0] if np.size(
        df_activities['ActName'].values) else None
    actStatus = df_activities['ActStatus'].values[0] if np.size(
        df_activities['ActStatus'].values) else None
    actDueDate = df_activities['ActDueDate'].values[0] if np.size(
        df_activities['ActDueDate'].values) else None
    actComDate = df_activities['ActComDate'].values[0] if np.size(
        df_activities['ActComDate'].values) else None

    return actGroupId, actTeam, actName, actStatus, actDueDate, actComDate


def getTransportOrders(report, startDate, endDate):

    # store variables to upper_case variables for better readability in the query
    START_DATE = startDate
    END_DATE = endDate

    order_table = report.orion_db.get_table_metadata(
        'RestInterface_order', 'ord')
    activity_table = report.orion_db.get_table_metadata(
        'RestInterface_activity', 'act')
    person_table = report.orion_db.get_table_metadata(
        'RestInterface_person', 'per')
    npp_table = report.orion_db.get_table_metadata('RestInterface_npp', 'npp')
    product_table = report.orion_db.get_table_metadata(
        'RestInterface_product', 'prd')

    # see gsp_transport/sql/getTransportOrders.sql for the raw MySQL query
    query = (
        select([order_table.c.id])
        .distinct()
        .select_from(order_table)
        .join(activity_table,
              activity_table.c.order_id == order_table.c.id)
        .outerjoin(person_table,
                   person_table.c.id == activity_table.c.person_id)
        .outerjoin(npp_table,
                   and_(
                       npp_table.c.order_id == order_table.c.id,
                       npp_table.c.level == 'Mainline',
                       npp_table.c.status != 'Cancel'
                   ))
        .outerjoin(product_table,
                   product_table.c.id == npp_table.c.product_id)
        .where(
            and_(
                or_(
                    and_(
                        product_table.c.network_product_code.like('DGN%'),
                        order_table.c.order_type.in_(
                            ['Provide', 'Change', 'Cease']),
                        or_(
                            person_table.c.role.like('ODC_%'),
                            person_table.c.role.like('RDC_%'),
                            person_table.c.role.like('GSPSG_%')
                        ),
                        activity_table.c.name == 'GSDT Co-ordination Wrk-BQ',
                        activity_table.c.status == 'COM'
                    ).self_group(),
                    and_(
                        product_table.c.network_product_code.like('DME%'),
                        order_table.c.order_type.in_(
                            ['Provide', 'Change', 'Cease']),
                        or_(
                            person_table.c.role.like('ODC_%'),
                            person_table.c.role.like('RDC_%'),
                            person_table.c.role.like('GSPSG_%')
                        ),
                        activity_table.c.name == 'GSDT Co-ordination Wrk-BQ',
                        activity_table.c.status == 'COM'
                    ).self_group(),
                    and_(
                        product_table.c.network_product_code == 'ELK0052',
                        or_(
                            and_(
                                order_table.c.order_type.in_(
                                    ['Provide', 'Change']),
                                or_(
                                    person_table.c.role.like('ODC_%'),
                                    person_table.c.role.like('RDC_%'),
                                    person_table.c.role.like('GSPSG_%'),
                                ),
                                activity_table.c.name == 'Circuit Creation',
                                activity_table.c.status == 'COM'
                            ).self_group(),
                            and_(
                                order_table.c.order_type == 'Cease',
                                or_(
                                    person_table.c.role.like('ODC_%'),
                                    person_table.c.role.like('RDC_%'),
                                    person_table.c.role.like('GSPSG_%'),
                                ),
                                activity_table.c.name == 'Node & Circuit Deletion',
                                activity_table.c.status == 'COM'
                            ).self_group()
                        )
                    ).self_group(),
                    and_(
                        or_(
                            and_(
                                order_table.c.order_type == 'Provide',
                                person_table.c.role == 'GSP_LTC_GW',
                                activity_table.c.name == 'GSDT Co-ordination Work',
                                activity_table.c.status == 'COM'
                            ).self_group(),
                            and_(
                                order_table.c.order_type == 'Cease',
                                person_table.c.role == 'GSDT31',
                                activity_table.c.name == 'GSDT Co-ordination Work',
                                activity_table.c.status == 'COM'
                            ).self_group(),
                        )
                    ).self_group()
                ),
                activity_table.c.completed_date.between(START_DATE, END_DATE)
            )
        )
    )

    # report.orion_db.log_full_query(query)

    result = report.orion_db.query_to_list(query)
    return pd.DataFrame(data=result, columns=['order_id'])


def getTransportRecords(report, order_id_list, startDate, endDate):

    # store variables to upper_case variables for better readability in the query
    ORDER_ID_LIST = order_id_list
    START_DATE = startDate
    END_DATE = endDate

    order_table = report.orion_db.get_table_metadata(
        'RestInterface_order', 'ord')
    activity_table = report.orion_db.get_table_metadata(
        'RestInterface_activity', 'act')
    person_table = report.orion_db.get_table_metadata(
        'RestInterface_person', 'per')
    customer_table = report.orion_db.get_table_metadata(
        'RestInterface_customer', 'cus')
    npp_table = report.orion_db.get_table_metadata('RestInterface_npp', 'npp')
    product_table = report.orion_db.get_table_metadata(
        'RestInterface_product', 'prd')

    # see gsp_transport/sql/getTransportRecords.sql for the raw MySQL query
    query = (
        select([
            case(
                (product_table.c.network_product_code.like('DGN%'), 'Diginet'),
                (product_table.c.network_product_code.like('DME%'), 'MetroE'),
                (product_table.c.network_product_code == 'ELK0052', 'MegaPop (CE)'),
                (product_table.c.network_product_code.like('GGW%'), 'Gigawave'),
                else_=null()
            ).label('service'),
            order_table.c.order_code,
            customer_table.c.name,
            order_table.c.current_crd,
            order_table.c.service_number,
            order_table.c.order_status,
            order_table.c.order_type,
            product_table.c.network_product_code,
            person_table.c.role,
            activity_table.c.activity_code.cast(Integer).label('step_no'),
            activity_table.c.name,
            activity_table.c.status,
            activity_table.c.due_date,
            activity_table.c.completed_date
        ])
        .distinct()
        .select_from(order_table)
        .join(activity_table,
              activity_table.c.order_id == order_table.c.id)
        .outerjoin(person_table,
                   person_table.c.id == activity_table.c.person_id)
        .outerjoin(customer_table,
                   customer_table.c.id == order_table.c.customer_id)
        .outerjoin(npp_table,
                   and_(
                       npp_table.c.order_id == order_table.c.id,
                       npp_table.c.level == 'Mainline',
                       npp_table.c.status != 'Cancel'
                   ))
        .outerjoin(product_table,
                   product_table.c.id == npp_table.c.product_id)
        .where(
            and_(
                order_table.c.id.in_(ORDER_ID_LIST),
                or_(
                    and_(
                        product_table.c.network_product_code.like('DGN%'),
                        or_(
                            and_(
                                order_table.c.order_type.in_(
                                    ['Provide', 'Change']),
                                or_(
                                    person_table.c.role.like('ODC_%'),
                                    person_table.c.role.like('RDC_%'),
                                    person_table.c.role.like('GSPSG_%')
                                ),
                                and_(
                                    or_(
                                        and_(
                                            activity_table.c.name == 'GSDT Co-ordination Wrk-BQ',
                                            activity_table.c.status == 'COM',
                                            activity_table.c.completed_date.between(
                                                START_DATE, END_DATE)
                                        ).self_group(),
                                        activity_table.c.name == 'Circuit Creation'
                                    )
                                )
                            ),
                            and_(
                                order_table.c.order_type == 'Cease',
                                or_(
                                    person_table.c.role.like('ODC_%'),
                                    person_table.c.role.like('RDC_%'),
                                    person_table.c.role.like('GSPSG_%')
                                ),
                                and_(
                                    or_(
                                        and_(
                                            activity_table.c.name == 'GSDT Co-ordination Wrk-BQ',
                                            activity_table.c.status == 'COM',
                                            activity_table.c.completed_date.between(
                                                START_DATE, END_DATE)
                                        ).self_group(),
                                        activity_table.c.name.in_(
                                            ['Node & Cct Del (DN-ISDN)', 'Node & Cct Deletion (DN)'])
                                    )
                                )
                            ).self_group()
                        )
                    ).self_group(),
                    and_(
                        product_table.c.network_product_code.like('DME%'),
                        or_(
                            and_(
                                order_table.c.order_type == 'Provide',
                                or_(
                                    person_table.c.role.like('ODC_%'),
                                    person_table.c.role.like('RDC_%'),
                                    person_table.c.role.like('GSPSG_%')
                                ),
                                and_(
                                    or_(
                                        and_(
                                            activity_table.c.name == 'GSDT Co-ordination Wrk-BQ',
                                            activity_table.c.status == 'COM',
                                            activity_table.c.completed_date.between(
                                                START_DATE, END_DATE)
                                        ).self_group(),
                                        activity_table.c.name == 'Circuit Creation'
                                    )
                                )
                            ).self_group(),
                            and_(
                                order_table.c.order_type == 'Change',
                                or_(
                                    and_(
                                        or_(
                                            person_table.c.role.like('ODC_%'),
                                            person_table.c.role.like('RDC_%'),
                                            person_table.c.role.like('GSPSG_%')
                                        ),
                                        activity_table.c.name == 'GSDT Co-ordination Wrk-BQ',
                                        activity_table.c.status == 'COM',
                                        activity_table.c.completed_date.between(
                                            START_DATE, END_DATE),
                                    ).self_group(),
                                    and_(
                                        or_(
                                            person_table.c.role.like('ODC_%'),
                                            person_table.c.role.like('RDC_%'),
                                            person_table.c.role.like('GSP_%')
                                        ),
                                        activity_table.c.name.in_(
                                            ['Circuit Creation', 'Change Speed Configure'])
                                    ).self_group()
                                )
                            ).self_group(),
                            and_(
                                order_table.c.order_type == 'Cease',
                                or_(
                                    person_table.c.role.like('ODC_%'),
                                    person_table.c.role.like('RDC_%'),
                                    person_table.c.role.like('GSPSG_%')
                                ),
                                and_(
                                    or_(
                                        and_(
                                            activity_table.c.name == 'GSDT Co-ordination Wrk-BQ',
                                            activity_table.c.status == 'COM',
                                            activity_table.c.completed_date.between(
                                                START_DATE, END_DATE)
                                        ).self_group(),
                                        activity_table.c.name == 'Node & Circuit Deletion'
                                    )
                                )
                            ).self_group()
                        )
                    ).self_group(),
                    and_(
                        product_table.c.network_product_code == 'ELK0052',
                        or_(
                            and_(
                                order_table.c.order_type.in_(
                                    ['Provide', 'Change']),
                                or_(
                                    person_table.c.role.like('ODC_%'),
                                    person_table.c.role.like('RDC_%'),
                                    person_table.c.role.like('GSPSG_%')
                                ),
                                activity_table.c.name == 'Circuit Creation',
                                activity_table.c.status == 'COM',
                                activity_table.c.completed_date.between(
                                    START_DATE, END_DATE)
                            ).self_group(),
                            and_(
                                order_table.c.order_type == 'Cease',
                                or_(
                                    person_table.c.role.like('ODC_%'),
                                    person_table.c.role.like('RDC_%'),
                                    person_table.c.role.like('GSPSG_%')
                                ),
                                activity_table.c.name == 'Node & Circuit Deletion',
                                activity_table.c.status == 'COM',
                                activity_table.c.completed_date.between(
                                    START_DATE, END_DATE)
                            ).self_group()
                        )
                    ).self_group(),
                    and_(
                        product_table.c.network_product_code.like('GGW%'),
                        or_(
                            and_(
                                order_table.c.order_type == 'Provide',
                                or_(
                                    and_(
                                        person_table.c.role == 'GSP_LTC_GW',
                                        activity_table.c.name == 'GSDT Co-ordination Work',
                                        activity_table.c.status == 'COM',
                                        activity_table.c.completed_date.between(
                                            START_DATE, END_DATE)
                                    ).self_group(),
                                    and_(
                                        or_(
                                            person_table.c.role.like('ODC_%'),
                                            person_table.c.role.like('RDC_%'),
                                            person_table.c.role.like('GSPSG_%')
                                        ),
                                        activity_table.c.name == 'Circuit Creation',
                                    )
                                )
                            ).self_group(),
                            and_(
                                order_table.c.order_type == 'Cease',
                                or_(
                                    and_(
                                        person_table.c.role == 'GSDT31',
                                        activity_table.c.name == 'GSDT Co-ordination Work',
                                        activity_table.c.status == 'COM',
                                        activity_table.c.completed_date.between(
                                            START_DATE, END_DATE)
                                    ).self_group(),
                                    and_(
                                        person_table.c.role == 'GSP_LTC_GW',
                                        activity_table.c.name == 'Circuit Removal from NMS',
                                    ).self_group()
                                )
                            ).self_group()
                        )
                    ).self_group()
                )
            )
        )
        .order_by(
            'service',
            order_table.c.order_type.desc(),
            activity_table.c.name,
            'step_no',
            person_table.c.role,
            order_table.c.order_code
        )
    )

    # report.orion_db.log_full_query(query)

    result = report.orion_db.query_to_list(query)
    return pd.DataFrame(data=result, columns=const.DRAFT_COLUMNS)

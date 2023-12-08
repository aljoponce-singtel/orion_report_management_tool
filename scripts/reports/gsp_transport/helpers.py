# Import built-in packages
import logging

# Import third-party packages
import numpy as np
import pandas as pd
from sqlalchemy import select, case, and_, or_, null, func, Integer

# Import local packages
import constants as const
from scripts.orion_report import OrionReport
from models import Transport

logger = logging.getLogger(__name__)


def generate_transport_report():

    report = OrionReport('Transport Report')
    report.set_filename('transport_report')
    report.set_prev_month_first_last_day_date()
    # Create Report
    df_finalReport = createFinalReport(report)
    # Insert records to tableau db
    update_tableau_table(report, df_finalReport)
    # Write to CSV
    csv_file = report.create_csv_from_df(
        df_finalReport[const.FINAL_COLUMNS], add_timestamp=True)
    # Add CSV to zip file
    zip_file = report.add_to_zip_file(csv_file, add_timestamp=True)
    # Send Email
    report.attach_file_to_email(zip_file)
    report.add_email_receiver_to('teokokwee@singtel.com')
    report.add_email_receiver_to('xv.hema.pawar@singtel.com')
    report.add_email_receiver_to('xv.chetna.deshmukh@singtel.com')
    report.add_email_receiver_to('maladzim@singtel.com')
    report.send_email()


def generate_transport_billing_report():

    report = OrionReport('Transport (Billing) Report')
    report.set_filename('transport_report')
    report.set_gsp_billing_month_start_end_date()
    # Create Report
    df_finalReport = createFinalReport(report)
    # Removing SGP from teams
    # Dropping rows where the PreConfig_Team or Coordination_Team column matches the string value 'SGP'
    df_finalReport = df_finalReport.drop(
        (df_finalReport.loc[df_finalReport['PreConfig_Team'] == 'SGP'].index) | (df_finalReport.loc[df_finalReport['Coordination_Team'] == 'SGP'].index))
    # Write to CSV
    csv_file = report.create_csv_from_df(
        df_finalReport[const.FINAL_COLUMNS], add_timestamp=True)
    # Add CSV to zip file
    zip_file = report.add_to_zip_file(csv_file, add_timestamp=True)
    # Send Email
    report.attach_file_to_email(zip_file)
    report.add_email_receiver_to('xv.hema.pawar@singtel.com')
    report.add_email_receiver_to('xv.abhijeet.navale@singtel.com')
    report.add_email_receiver_to('xv.chetna.deshmukh@singtel.com')
    report.add_email_receiver_to('maladzim@singtel.com')
    report.send_email()


def update_tableau_table(report: OrionReport, df: pd.DataFrame):
    # add new column
    df["update_time"] = pd.Timestamp.now()
    # set columns to datetime type
    df[const.TABLEAU_DATE_COLUMNS] = df[const.TABLEAU_DATE_COLUMNS].apply(
        pd.to_datetime)
    # set empty values to null
    df.replace('', None)
    # insert records to DB
    report.insert_df_to_tableau_table(
        df, table_name=Transport.__tablename__, table_model=Transport)


def createFinalReport(report: OrionReport):

    df_order_id = getTransportOrders(report)
    df = pd.DataFrame(getTransportRecords(
        report, df_order_id['order_id'].to_list()))
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
                df_activities, ['GSDT Co-ordination Wrk-BQ', 'GSDT Co-ordination Work'])

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
            if df_orders.at[index, 'OrderType'] == 'Provide':
                coordGroupId, coordTeam, coordActName, coordActStatus, coordActDueDate, coordActCOMDate = getActRecord(
                    df_activities, ['Circuit Configuration-STM', 'Circuit Creation'])

            if df_orders.at[index, 'OrderType'] == 'Change':
                coordGroupId, coordTeam, coordActName, coordActStatus, coordActDueDate, coordActCOMDate = getActRecord(
                    df_activities, ['Reconfiguration', 'Circuit Creation'])

            if df_orders.at[index, 'OrderType'] == 'Cease':
                coordGroupId, coordTeam, coordActName, coordActStatus, coordActDueDate, coordActCOMDate = getActRecord(
                    df_activities, ['Node & Circuit Deletion'])

            # COPY pre-config values to coordination values
            if df_orders.at[index, 'OrderType'] == 'Provide':
                preConfigGroupId, preConfigTeam, preConfigActName, preConfigStatus, preConfigDueDate, preConfigCOMDate = getActRecord(
                    df_activities, ['Circuit Configuration-STM', 'Circuit Creation'])

            if df_orders.at[index, 'OrderType'] == 'Change':
                preConfigGroupId, preConfigTeam, preConfigActName, preConfigStatus, preConfigDueDate, preConfigCOMDate = getActRecord(
                    df_activities, ['Reconfiguration', 'Circuit Creation'])

            if df_orders.at[index, 'OrderType'] == 'Cease':
                preConfigGroupId, preConfigTeam, preConfigActName, preConfigStatus, preConfigDueDate, preConfigCOMDate = getActRecord(
                    df_activities, ['Node & Circuit Deletion'])

        if df_orders.at[index, 'Service'] == 'Gigawave':
            if df_orders.at[index, 'OrderType'] == 'Provide' or df_orders.at[index, 'OrderType'] == 'Cease':
                coordGroupId, coordTeam, coordActName, coordActStatus, coordActDueDate, coordActCOMDate = getActRecord(
                    df_activities, ['GSDT Co-ordination Work', 'GSDT Co-ordination Wrk-BQ', 'GSDT Co-ordination WK-BQ'])

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

    df_finalReport = df_finalReport.reset_index(drop=True)

    return df_finalReport


def getActRecord(df: pd.DataFrame, activities):
    df_activities = df[df['ActName'].isin(activities)]

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


def getTransportOrders(report: OrionReport) -> pd.DataFrame:

    # store variables to upper_case (string) variables for better readability in the query
    START_DATE = str(report.start_date)
    END_DATE = str(report.end_date)

    order_table = report.orion_db.get_table_metadata(
        'RestInterface_order', 'ord')
    activity_table = report.orion_db.get_table_metadata(
        'RestInterface_activity', 'act')
    person_table = report.orion_db.get_table_metadata(
        'RestInterface_person', 'per')
    npp_table = report.orion_db.get_table_metadata('RestInterface_npp', 'npp')
    product_table = report.orion_db.get_table_metadata(
        'RestInterface_product', 'prd')
    customer_table = report.orion_db.get_table_metadata(
        'RestInterface_customer', 'cus')

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
        .outerjoin(customer_table,
                   customer_table.c.id == order_table.c.customer_id)
        .where(
            and_(
                or_(
                    and_(
                        or_(
                            product_table.c.network_product_code.like('DGN%'),
                            product_table.c.network_product_code.like('DEK%'),
                            product_table.c.network_product_code.like('DLC%')
                        ),
                        or_(
                            person_table.c.role.like('ODC_%'),
                            person_table.c.role.like('RDC_%'),
                            person_table.c.role.like('GSPSG_%')
                        ),
                        or_(
                            and_(
                                order_table.c.order_type.in_(
                                    ['Provide', 'Change', 'Cease']),
                                activity_table.c.name == 'GSDT Co-ordination Wrk-BQ'
                            ).self_group(),
                            and_(
                                or_(
                                    and_(
                                        order_table.c.order_type.in_(
                                            ['Provide', 'Change']),
                                        activity_table.c.name == 'Circuit creation'
                                    ).self_group(),
                                    and_(
                                        order_table.c.order_type == 'Cease',
                                        activity_table.c.name.in_(
                                            ['Node & Cct Del (DN-ISDN)', 'Node & Cct Deletion (DN)'])
                                    ).self_group()
                                ),
                                customer_table.c.name.like('SINGNET%')
                            ).self_group()
                        )
                    ).self_group(),
                    and_(
                        product_table.c.network_product_code.like('DME%'),
                        or_(
                            person_table.c.role.like('ODC_%'),
                            person_table.c.role.like('RDC_%'),
                            person_table.c.role.like('GSPSG_%')
                        ),
                        or_(
                            and_(
                                order_table.c.order_type.in_(
                                    ['Provide', 'Change', 'Cease']),
                                activity_table.c.name.in_(
                                    ['GSDT Co-ordination Wrk-BQ', 'GSDT Co-ordination Work'])
                            ).self_group(),
                            and_(
                                or_(
                                    and_(
                                        order_table.c.order_type == 'Provide',
                                        activity_table.c.name == 'Circuit creation'
                                    ).self_group(),
                                    and_(
                                        order_table.c.order_type == 'Change',
                                        activity_table.c.name.in_(
                                            ['Circuit creation', 'Change Speed Configure'])
                                    ).self_group(),
                                    and_(
                                        order_table.c.order_type == 'Cease',
                                        activity_table.c.name == 'Node & Circuit Deletion'
                                    ).self_group()
                                ),
                                customer_table.c.name.like('SINGNET%')
                            ).self_group()
                        )
                    ).self_group(),
                    and_(
                        product_table.c.network_product_code.in_(
                            ['ELK0031',
                             'ELK0052',
                             'ELK0053',
                             'ELK0055',
                             'ELK0089',
                             'ELK0090',
                             'ELK0091',
                             'ELK0092',
                             'ELK0093',
                             'ELK0094',
                             'ELK0003']),
                        or_(
                            and_(
                                or_(
                                    person_table.c.role.like('ODC_%'),
                                    person_table.c.role.like('RDC_%'),
                                    person_table.c.role.like('GSPSG_%')
                                ),
                                or_(
                                    and_(
                                        order_table.c.order_type == 'Provide',
                                        activity_table.c.name == 'Circuit Creation'
                                    ).self_group(),
                                    and_(
                                        order_table.c.order_type == 'Change',
                                        activity_table.c.name.in_(
                                            ['Circuit Creation', 'Reconfiguration'])
                                    ).self_group(),
                                    and_(
                                        order_table.c.order_type == 'Cease',
                                        activity_table.c.name.in_(
                                            ['Node & Circuit Deletion', 'Node & Cct Deletion (DN)'])
                                    ).self_group()
                                )
                            ).self_group(),
                            and_(
                                person_table.c.role == 'GSPSG_ME',
                                order_table.c.order_type == 'Provide',
                                activity_table.c.name == 'Circuit Configuration-STM'
                            ).self_group()
                        )
                    ).self_group(),
                    and_(
                        product_table.c.network_product_code.like('GGW%'),
                        activity_table.c.name.in_(
                            ['GSDT Co-ordination Wrk-BQ', 'GSDT Co-ordination WK-BQ', 'GSDT Co-ordination Work']),
                        or_(
                            and_(
                                order_table.c.order_type == 'Provide',
                                or_(
                                    person_table.c.role == 'GSP_LTC_GW',
                                    person_table.c.role.like('ODC_%'),
                                    person_table.c.role.like('RDC_%')
                                )
                            ).self_group(),
                            and_(
                                order_table.c.order_type == 'Cease',
                                or_(
                                    person_table.c.role == 'GSDT31',
                                    person_table.c.role.like('ODC_%'),
                                    person_table.c.role.like('RDC_%')
                                )
                            ).self_group(),
                        )
                    ).self_group()
                ),
                activity_table.c.completed_date.between(
                    START_DATE, END_DATE)
            )
        )
    )

    return report.query_to_dataframe(
        query, query_description='orders', column_names=['order_id'])


def getTransportRecords(report: OrionReport, order_id_list) -> pd.DataFrame:

    # store variables to upper_case variables for better readability in the query
    ORDER_ID_LIST = order_id_list

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
                (or_(
                    product_table.c.network_product_code.like('DGN%'),
                    product_table.c.network_product_code.like('DEK%'),
                    product_table.c.network_product_code.like('DLC%')
                ), 'Diginet'),
                (product_table.c.network_product_code.like('DME%'), 'MetroE'),
                (product_table.c.network_product_code.in_(
                    ['ELK0031',
                     'ELK0052',
                     'ELK0053',
                     'ELK0055',
                     'ELK0089',
                     'ELK0090',
                     'ELK0091',
                     'ELK0092',
                     'ELK0093',
                     'ELK0094',
                     'ELK0003']), 'MegaPop (CE)'),
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
                        or_(
                            product_table.c.network_product_code.like('DGN%'),
                            product_table.c.network_product_code.like('DEK%'),
                            product_table.c.network_product_code.like('DLC%')
                        ),
                        or_(
                            person_table.c.role.like('ODC_%'),
                            person_table.c.role.like('RDC_%'),
                            person_table.c.role.like('GSPSG_%')
                        ),
                        or_(
                            and_(
                                order_table.c.order_type.in_(
                                    ['Provide', 'Change']),
                                activity_table.c.name.in_(
                                    ['GSDT Co-ordination Wrk-BQ',
                                     'Circuit Creation'])
                            ).self_group(),
                            and_(
                                order_table.c.order_type == 'Cease',
                                activity_table.c.name.in_(
                                    ['GSDT Co-ordination Wrk-BQ',
                                     'Node & Cct Del (DN-ISDN)',
                                     'Node & Cct Deletion (DN)'])
                            ).self_group()
                        )
                    ).self_group(),
                    and_(
                        product_table.c.network_product_code.like('DME%'),
                        or_(
                            person_table.c.role.like('ODC_%'),
                            person_table.c.role.like('RDC_%'),
                            person_table.c.role.like('GSPSG_%')
                        ),
                        or_(
                            and_(
                                order_table.c.order_type == 'Provide',
                                activity_table.c.name.in_(
                                    ['GSDT Co-ordination Wrk-BQ',
                                     'GSDT Co-ordination Work',
                                     'Circuit Creation']),
                            ).self_group(),
                            and_(
                                order_table.c.order_type == 'Change',
                                activity_table.c.name.in_(
                                    ['GSDT Co-ordination Wrk-BQ',
                                     'GSDT Co-ordination Work',
                                     'Circuit Creation',
                                     'Change Speed Configure']),
                            ).self_group(),
                            and_(
                                order_table.c.order_type == 'Cease',
                                activity_table.c.name.in_(
                                    ['GSDT Co-ordination Wrk-BQ',
                                     'GSDT Co-ordination Work',
                                     'Node & Circuit Deletion']),
                            ).self_group()
                        )
                    ).self_group(),
                    and_(
                        product_table.c.network_product_code.in_(
                            ['ELK0031',
                             'ELK0052',
                             'ELK0053',
                             'ELK0055',
                             'ELK0089',
                             'ELK0090',
                             'ELK0091',
                             'ELK0092',
                             'ELK0093',
                             'ELK0094',
                             'ELK0003']),
                        or_(
                            and_(
                                order_table.c.order_type == 'Provide',
                                or_(
                                    and_(
                                        or_(
                                            person_table.c.role.like('ODC_%'),
                                            person_table.c.role.like('RDC_%'),
                                            person_table.c.role.like('GSPSG_%')
                                        ),
                                        activity_table.c.name == 'Circuit Creation'
                                    ).self_group(),
                                    and_(
                                        person_table.c.role == 'GSPSG_ME',
                                        activity_table.c.name == 'Circuit Configuration-STM'
                                    ).self_group()
                                ).self_group()
                            ).self_group(),
                            and_(
                                order_table.c.order_type == 'Change',
                                or_(
                                    person_table.c.role.like('ODC_%'),
                                    person_table.c.role.like('RDC_%'),
                                    person_table.c.role.like('GSPSG_%')
                                ),
                                activity_table.c.name.in_(
                                    ['Circuit Creation', 'Reconfiguration'])
                            ).self_group(),
                            and_(
                                order_table.c.order_type == 'Cease',
                                or_(
                                    person_table.c.role.like('ODC_%'),
                                    person_table.c.role.like('RDC_%'),
                                    person_table.c.role.like('GSPSG_%')
                                ),
                                activity_table.c.name.in_(
                                    ['Node & Circuit Deletion', 'Node & Cct Deletion (DN)'])
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
                                        or_(
                                            person_table.c.role == 'GSP_LTC_GW',
                                            person_table.c.role.like('ODC_%'),
                                            person_table.c.role.like('RDC_%')
                                        ),
                                        activity_table.c.name.in_(
                                            ['GSDT Co-ordination Wrk-BQ',
                                             'GSDT Co-ordination WK-BQ',
                                             'GSDT Co-ordination Work'])
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
                                        or_(
                                            person_table.c.role == 'GSDT31',
                                            person_table.c.role.like('ODC_%'),
                                            person_table.c.role.like('RDC_%')
                                        ),
                                        activity_table.c.name.in_(
                                            ['GSDT Co-ordination Wrk-BQ',
                                             'GSDT Co-ordination WK-BQ',
                                             'GSDT Co-ordination Work'])
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

    return report.query_to_dataframe(query, column_names=const.DRAFT_COLUMNS)

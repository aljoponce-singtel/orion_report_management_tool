# Import built-in packages
import logging
import numpy as np
import pandas as pd
import sqlalchemy as db

# Import local packages
import constants as const
from scripts.orion_report import OrionReport

logger = logging.getLogger(__name__)


def generate_sdo_singnet_report():

    report = OrionReport('SDO Singnet Report')
    report.set_filename('sdo_singnet_report')
    report.set_reporting_date()

    product_code_list = [
        'SGN0051',
        'SGN0119',
        'SGN0157',
        'SGN0160',
        'SGN0170',
        'SGN0340',
        'SGN2004'
    ]

    product_code_instance = [
        'SGN0170',
        'SGN2004'
    ]

    df_tableau_orders = get_tableau_orders(report, 'GSDT7', product_code_list)
    df_raw_report = get_raw_orders(
        report, df_tableau_orders['Workorder_no'].to_list())
    df_raw_report = add_param_svcno_col_to_df(
        report, df_raw_report, product_code_instance, const.PARAMETERS_TO_SEARCH)
    df_raw_report = add_order_info_col_to_df(report, df_raw_report)

    # Add Site survey (SS) activities to df_raw_report
    ss_activities_map = [
        {"productCodes": ['SGN0170', 'SGN2004', 'SGN0051', 'SGN0157'],
         "activities": ['Site Survey - A End']},
        {"productCodes": ['SGN0160'],
         "activities": ['Check HSD Resources']},
        {"productCodes": ['SGN0119', 'SGN0340'],
         "activities": ['Site Survey - A End', 'Site Survey', 'Check HSD Resources']}
    ]

    df_ss = create_activity_df(report, df_raw_report,
                               ss_activities_map, const.SS_COLUMNS)
    df_raw_report = pd.merge(df_raw_report, df_ss, how='left')

    # Add Routing info (RI) activities to df_rawReport
    ri_activities_map = [
        {"productCodes": ['SGN0170', 'SGN2004', 'SGN0051', 'SGN0157'],
         "activities": ['Cct Allocate-ETE Routing', 'Circuit Allocation']},
        {"productCodes": ['SGN0160'],
         "activities": ['TNP/HSD Activities']},
        {"productCodes": ['SGN0119', 'SGN0340'],
         "activities": ['Cct Allocate-ETE Routing', 'Circuit Allocation', 'TNP/HSD Activities']}
    ]

    df_ri = create_activity_df(report, df_raw_report,
                               ri_activities_map, const.RI_COLUMNS)
    df_raw_report = pd.merge(df_raw_report, df_ri, how='left')

    # Add Testing and Installation (TI) activities to df_rawReport
    ti_activities_map = [
        {"productCodes": ['SGN0170', 'SGN2004', 'SGN0051', 'SGN0157'],
         "activities": ['CPE Instln & Testingg', 'End-To-End Test - A End']},
        {"productCodes": ['SGN0160'],
         "activities": ['E-To-E Test (PNOC)']},
        {"productCodes": ['SGN0119', 'SGN0340'],
         "activities": ['CPE Instln & Testingg', 'DWFM Installation Work', 'E-To-E Test (PNOC)', 'End-To-End Test - A End']}
    ]

    df_ti = create_activity_df(report, df_raw_report,
                               ti_activities_map, const.TI_COLUMNS)
    df_raw_report = pd.merge(df_raw_report, df_ti, how='left')
    df_final_report = df_raw_report[const.FINAL_COLUMNS]
    # Set emply cells to NULL only for the ProjectManager column
    df_final_report['ProjectManager'].replace(np.nan, 'NULL', inplace=True)

    # Write to CSV
    csv_file = report.create_csv_from_df(
        df_final_report, add_timestamp=report.get_current_datetime(format="%d%m%y_%H%M"))
    zip_file = report.add_to_zip_file(csv_file, add_timestamp=True)
    # Insert records to tableau db
    update_tableau_db(report, df_final_report, 'SINGNET')
    # Attach report and send to email
    report.attach_file_to_email(zip_file)
    report.add_email_receiver_to('kajendran@singtel.com')
    report.send_email()


def generate_sdo_megapop_report():

    report = OrionReport('SDO MegaPop Report')
    report.set_filename('sdo_megapop_report')
    report.set_reporting_date()

    product_code_list = [
        'ELK0052',
        'ELK0053',
        'ELK0055',
        'ELK0089',
        'GEL0001',
        'GEL0018',
        'GEL0023',
        'GEL0036'
    ]

    product_code_instance = [
        'GEL0001',
        'GEL0018',
        'GEL0023',
        'GEL0036'
    ]

    df_tableau_orders = get_tableau_orders(report, 'GSDT8', product_code_list)
    df_raw_report = get_raw_orders(
        report, df_tableau_orders['Workorder_no'].to_list())
    df_raw_report = add_param_svcno_col_to_df(
        report, df_raw_report, product_code_instance, const.PARAMETERS_TO_SEARCH)
    df_raw_report = add_order_info_col_to_df(report, df_raw_report)

    # Add Site survey (SS) activities to df_raw_report
    ss_activities_map = [
        {"productCodes": ['GEL0001', 'GEL0036', 'ELK0052', 'ELK0053', 'ELK0055', 'ELK0089'],
         "activities": ['Site Survey - A End']},
        {"productCodes": ['GEL0018'],
         "activities": ['Site Survey - A End', 'Site Survey', 'Check & Plan Fiber  - SME', 'Check & Plan Fiber  - ON']},
        {"productCodes": ['GEL0023'],
         "activities": ['Site Survey', 'Check & Plan Fiber  - SME', 'Check & Plan Fiber  - ON']}
    ]

    df_ss = create_activity_df(report, df_raw_report,
                               ss_activities_map, const.SS_COLUMNS)
    df_raw_report = pd.merge(df_raw_report, df_ss, how='left')

    # Add Routing info (RI) activities to df_rawReport
    ri_activities_map = [
        {"productCodes": ['GEL0001', 'GEL0036', 'ELK0052', 'ELK0053', 'ELK0055', 'ELK0089'],
         "activities": ['Cct Allocate-ETE Routing', 'Circuit Allocation']},
        {"productCodes": ['GEL0018', 'GEL0023'],
         "activities": ['Cct Allocate-ETE Routing', 'Cct Allocate ETE Rtg - ON']}
    ]

    df_ri = create_activity_df(report, df_raw_report,
                               ri_activities_map, const.RI_COLUMNS)
    df_raw_report = pd.merge(df_raw_report, df_ri, how='left')

    # Add Testing and Installation (TI) activities to df_rawReport
    ti_activities_map = [
        {"productCodes": ['GEL0001', 'GEL0036', 'ELK0052', 'ELK0053', 'ELK0055', 'ELK0089'],
         "activities": ['CPE Instln & Testing', 'End-To-End Test - A End']},
        {"productCodes": ['GEL0018', 'GEL0023'],
         "activities": ['DWFM Installation Work', 'CPE Instln & Testing']}
    ]

    df_ti = create_activity_df(report, df_raw_report,
                               ti_activities_map, const.TI_COLUMNS)
    df_raw_report = pd.merge(df_raw_report, df_ti, how='left')
    df_final_report = df_raw_report[const.FINAL_COLUMNS]
    # Set emply cells to NULL only for the ProjectManager column
    df_final_report['ProjectManager'].replace(np.nan, 'NULL', inplace=True)

    # Write to CSV
    csv_file = report.create_csv_from_df(
        df_final_report, add_timestamp=report.get_current_datetime(format="%d%m%y_%H%M"))
    zip_file = report.add_to_zip_file(csv_file, add_timestamp=True)
    # Insert records to tableau db
    update_tableau_db(report, df_final_report, 'MEGAPOP')
    # Attach report and send to email
    report.attach_file_to_email(zip_file)
    report.add_email_receiver_to('kajendran@singtel.com')
    report.send_email()


def get_tableau_orders(report: OrionReport, group_id: str, product_code_list: list[str]) -> pd.DataFrame:

    query = report.get_query_from_file("query_tableau_orders.sql")
    formatted_query = query.format(report_id=group_id, product_code_list=report.list_to_string(
        product_code_list), report_date=report.report_date)

    return report.query_to_dataframe(query=formatted_query, db=report.tableau_db, query_description="tableau orders")


def get_raw_orders(report: OrionReport, order_list: list[str]) -> pd.DataFrame:

    query = report.get_query_from_file("query_raw.sql")
    formatted_query = query.format(
        order_list=report.list_to_string(order_list))

    return report.query_to_dataframe(query=formatted_query, query_description="orion orders")


def add_param_svcno_col_to_df(report: OrionReport, df: pd.DataFrame, product_code_instance: list[str], parameters_names: list[str]) -> pd.DataFrame:
    # Add new ParameterName and ParameterValue columns to df
    df_instance = df[df['ProductCode'].isin(product_code_instance)]
    para_names_list_str = report.list_to_string(parameters_names)
    serviceno_list_str = report.list_to_string(
        df_instance['ServiceNumberUpd'].to_list())
    query_param = report.get_query_from_file("query_parameters.sql")
    formatted_query = query_param.format(
        para_names_list=para_names_list_str, serviceno_list=serviceno_list_str)
    df_parameters = report.query_to_dataframe(
        query=formatted_query, query_description="parameter info")
    df = pd.merge(df, df_parameters, how='left')
    # Add new ServiceNoNew column
    df['ServiceNoNew'] = None
    # Copy df['ServiceNumberUpd'] values to df['ServiceNoNew] where df['ServiceNumberUpd'] == df_nonInstance['ServiceNumberUpd']
    df_non_instance = df[~df['ProductCode'].isin(product_code_instance)]
    df.loc[df['ServiceNumberUpd'].isin(df_non_instance['ServiceNumberUpd'].to_list(
    )), 'ServiceNoNew'] = df_non_instance['ServiceNumberUpd']

    # Copy df_parameters['ParameterValue'] values to df['ServiceNoNew] where df['ServiceNumberUpd'] == df_parameters['ParameterValue']
    df.set_index('ServiceNumberUpd', inplace=True)
    df_parameters.set_index('ServiceNumberUpd', inplace=True)
    df_parameters.rename(
        columns={'ParameterValue': 'ServiceNoNew'}, inplace=True)
    df.update(df_parameters)
    df.reset_index(inplace=True)

    return df


def add_order_info_col_to_df(report: OrionReport, df: pd.DataFrame) -> pd.DataFrame:
    serviceno_list = df['ServiceNoNew'].dropna().to_list()
    query = report.get_query_from_file("query_order_info.sql")
    formatted_query = query.format(
        serviceno_list=report.list_to_string(serviceno_list))
    df_order_info = report.query_to_dataframe(
        query=formatted_query, query_description="order info")

    # There are cases where a service number can have multiple orders.
    # Choose the order with the latest CRD.
    # Drop duplicate using ServiceNoNew by keeping the latest CRDNew.
    df_sorted = df_order_info.sort_values(
        by=['ServiceNoNew', 'CRDNew'])
    df_rmDuplicates = df_sorted.drop_duplicates(
        subset=['ServiceNoNew'], keep='last')

    df = pd.merge(df, df_rmDuplicates, how='left')

    return df


def create_activity_df(report: OrionReport, df_raw_report: pd.DataFrame, activities_map: list, act_columns: list):
    df_raw_report = pd.DataFrame(df_raw_report)
    order_list = df_raw_report['OrderCodeNew'].dropna().to_list()

    # Merge activities into 1 unique list
    unique_activity_list = []
    for activity_map in activities_map:
        if len(unique_activity_list) == 0:
            unique_activity_list = activity_map['activities']
        else:
            unique_activity_list = list(
                set(unique_activity_list + activity_map['activities']))

    query = report.get_query_from_file("query_activities.sql")
    formatted_query = query.format(
        unique_activities=report.list_to_string(unique_activity_list), order_list=report.list_to_string(order_list))
    df_act_info = report.query_to_dataframe(
        query=formatted_query, query_description="activity info")
    df_act_final = remove_duplicates(
        df_raw_report, df_act_info, activities_map)
    df_act_final.columns = act_columns

    return df_act_final


def remove_duplicates(df_raw_report: pd.DataFrame, df_act_info: pd.DataFrame, activities_map: pd.DataFrame) -> pd.DataFrame:
    df_act_final = pd.DataFrame(columns=df_act_info.columns)

    for activity_map in activities_map:
        # Get the df_rawReport records only if the product codes are included in the activityMap's product codes
        df_products = df_raw_report[df_raw_report['ProductCode'].isin(
            activity_map['productCodes'])]
        # Get the df_actInfo records only if the OrderCodeNew is in the df_products records
        df_act = df_act_info[df_act_info['OrderCodeNew'].isin(
            df_products['OrderCodeNew'].to_list())]
        # Get the df_act records only if the activities are included in the activityMap's activities
        df_valid_act = df_act[df_act['ActivityName'].isin(
            activity_map['activities'])]

        if not df_valid_act.empty:
            act_priority = activity_map['activities']

            # If there are duplicates, select the activity with the highest step_no
            # Drop duplicate OrderCodeNew with same ActivityName by keeping the highest step_no column
            df_sorted = df_valid_act.sort_values(
                by=['OrderCodeNew', 'step_no'])
            df_rm_duplicates = df_sorted.drop_duplicates(
                subset=['OrderCodeNew', 'ActivityName'], keep='last')

            # If there are duplicates, select 1 activity based from the sequence in the actPriority list
            # Drop duplicate OrderCodeNew with diff ActivityName by keeping 1 ActivityName from the actPriority list
            df_act_priority = pd.DataFrame(df_rm_duplicates)
            df_act_priority['ActivityName'] = pd.Categorical(
                values=df_act_priority['ActivityName'], categories=act_priority)
            df_act_priority_sorted = df_act_priority.sort_values(
                by=['OrderCodeNew', 'ActivityName'])
            df_act_priority_rm_duplicates = df_act_priority_sorted.drop_duplicates(
                subset=['OrderCodeNew'], keep='first')

            df_act_final = pd.concat(
                [df_act_final, df_act_priority_rm_duplicates])

    return df_act_final


def update_tableau_db(report: OrionReport, df: pd.DataFrame, report_id: str):

    # Get a list of public holidays from Tableaue DB
    t_GSP_holidays = report.tableau_db.get_table_metadata('t_GSP_holidays')
    # SELECT * FROM t_GSP_holidays
    query = db.select([t_GSP_holidays])
    df_holidays = report.tableau_db.query_to_dataframe(query)
    HOLIDAYS = df_holidays['Date'].values.tolist()
    # Counts the number of valid days between begindates and enddates, not including the day of enddates.
    # Weekends (Sat/Sun) and Public Holidays are excluded
    # Add 3 new columns (COM_Date_SS, COM_Date_RI and COM_Date_TI)
    for index, row in df.iterrows():
        if not pd.isnull(row["COM_Date_SS"]):
            startDate = row["COM_Date_SS"]
            endDate = row["CRDNew"]

            if endDate >= startDate:
                df_bdays = pd.bdate_range(
                    start=startDate, end=endDate, freq='C', holidays=HOLIDAYS)
                df.at[index, "SS_to_CRD"] = len(df_bdays)
            else:
                # Need to switch start & end date and explicitly set the count to negative
                # as pd.bdate_range() does not calculate negative business days
                df_bdays = pd.bdate_range(
                    start=endDate, end=startDate, freq='C', holidays=HOLIDAYS)
                df.at[index, "SS_to_CRD"] = len(df_bdays)*(-1)

        if not pd.isnull(row["COM_Date_RI"]):
            startDate = row["COM_Date_RI"]
            endDate = row["CRDNew"]

            if endDate >= startDate:
                df_bdays = pd.bdate_range(
                    start=startDate, end=endDate, freq='C', holidays=HOLIDAYS)
                df.at[index, "RI_to_CRD"] = len(df_bdays)
            else:
                # Need to switch start & end date and explicitly set the count to negative
                # as pd.bdate_range() does not calculate negative business days
                df_bdays = pd.bdate_range(
                    start=endDate, end=startDate, freq='C', holidays=HOLIDAYS)
                df.at[index, "RI_to_CRD"] = len(df_bdays)*(-1)

        if not pd.isnull(row["COM_Date_TI"]):
            startDate = row["CRDNew"]
            endDate = row["COM_Date_TI"]

            if endDate >= startDate:
                df_bdays = pd.bdate_range(
                    start=startDate, end=endDate, freq='C', holidays=HOLIDAYS)
                df.at[index, "TI_to_CRD"] = len(df_bdays)
            else:
                # Need to switch start & end date and explicitly set the count to negative
                # as pd.bdate_range() does not calculate negative business days
                df_bdays = pd.bdate_range(
                    start=endDate, end=startDate, freq='C', holidays=HOLIDAYS)
                df.at[index, "TI_to_CRD"] = len(df_bdays)*(-1)

    # add new columns
    df["report_id"] = report_id.lower()
    df["update_time"] = pd.Timestamp.now()

    # set columns to datetime type
    df[const.TABLEAU_DATE_COLUMNS] = df[const.TABLEAU_DATE_COLUMNS].apply(
        pd.to_datetime)

    # set empty values to null
    df.replace('', None)
    # insert records to DB
    report.insert_df_to_tableau_db(df)

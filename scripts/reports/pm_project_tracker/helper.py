# Import built-in packages
import logging
import os

# Import third-party packages
import pandas as pd

# Import local packages
from models import create_project_tracker_test_group_class
from scripts.orion_report import OrionReport

logger = logging.getLogger(__name__)


def generate_report():

    report = OrionReport('Project Tracker Test Report')
    group_name = report.default_config.get("group_name")
    report.set_filename(
        f'project_tracker_test_report_{group_name}')
    # if group_name:
    #     ProjectTrackerTestGroup = create_project_tracker_test_group_class(
    #         group_name)
    # else:
    #     ProjectTrackerTestGroup = create_project_tracker_test_group_class()

    ProjectTrackerTestGroup = create_project_tracker_test_group_class()

    # query mapping
    query = report.get_query_from_file("query_mapping.sql")
    formatted_query = query.format(group_name=group_name)
    df_mapping = report.query_to_dataframe(
        formatted_query, query_description="mapping records")
    # insert records to DB
    report.insert_df_to_test_table(
        df_mapping, table_name=ProjectTrackerTestGroup.__tablename__, table_model=ProjectTrackerTestGroup, if_exist="replace")

    report_final = OrionReport('Project Tracker Test Report')
    group_name = report_final.default_config.get("group_name")
    report_final.set_filename(
        f'project_tracker_test_report_{group_name}')
    # query mapping
    query = report_final.get_query_from_file("query_mapping.sql")
    formatted_query = query.format(group_name=group_name)
    df_mapping = report_final.query_to_dataframe(
        formatted_query, query_description="mapping records")
    # query report
    query = report_final.get_query_from_file("query_report.sql")
    formatted_query = query.format(
        group_table_name=ProjectTrackerTestGroup.__tablename__)
    df_report = report_final.query_to_dataframe(
        formatted_query, query_description="report records")
    # query order
    query = report_final.get_query_from_file("query_order.sql")
    formatted_query = query.format(
        group_table_name=ProjectTrackerTestGroup.__tablename__)
    df_order = report_final.query_to_dataframe(
        formatted_query, query_description="order records")
    # query npp
    query = report_final.get_query_from_file("query_npp.sql")
    formatted_query = query.format(
        group_table_name=ProjectTrackerTestGroup.__tablename__)
    df_npp = report_final.query_to_dataframe(
        formatted_query, query_description="npp records")
    # query activity
    query = report_final.get_query_from_file("query_activity.sql")
    formatted_query = query.format(
        group_table_name=ProjectTrackerTestGroup.__tablename__)
    df_activity = report_final.query_to_dataframe(
        formatted_query, query_description="activity records")
    # Create Excel writer object
    excel_file = f"{report_final.filename}.xlsx"
    excel_file_path = os.path.join(
        report_final.reports_folder_path, excel_file)
    excel_writer = pd.ExcelWriter(excel_file_path, engine='xlsxwriter')
    # Write dataframes to separate sheets
    df_mapping.to_excel(excel_writer, sheet_name='mapping', index=False)
    df_report.to_excel(excel_writer, sheet_name='report', index=False)
    df_order.to_excel(excel_writer, sheet_name='order', index=False)
    df_npp.to_excel(excel_writer, sheet_name='npp', index=False)
    df_activity.to_excel(excel_writer, sheet_name='activity', index=False)
    # Close the Excel file
    excel_writer.close()

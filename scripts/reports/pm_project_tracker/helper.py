# Import built-in packages
import logging

# Import local packages
from models import create_project_tracker_test_group_class
from scripts.orion_report import OrionReport

logger = logging.getLogger(__name__)


def generate_report():

    report = OrionReport('Project Tracker Test Report')
    group_name = report.default_config.get("group_name")
    report.set_filename(
        f'project_tracker_test_report_{group_name}')
    if group_name:
        ProjectTrackerTestGroup = create_project_tracker_test_group_class(
            group_name)
    else:
        ProjectTrackerTestGroup = create_project_tracker_test_group_class()
    # query to dataframe
    query = report.get_query_from_file("query_mapping.sql")
    formatted_query = query.format(group_name=group_name)
    df = report.query_to_dataframe(
        formatted_query, query_description="mapping records")
    # insert records to DB
    report.insert_df_to_test_table(
        df, table_name=ProjectTrackerTestGroup.__tablename__, table_model=ProjectTrackerTestGroup, if_exist="replace")
    excel_file = report.create_excel_from_df(df)

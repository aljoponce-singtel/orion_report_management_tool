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

    # query = report.get_query_from_file("query.sql")
    # formatted_query = query.format(
    #     start_date=report.start_date, end_date=report.end_date)
    # csv_file = report.query_to_csv(formatted_query, add_timestamp=True)
    # zip_file = report.add_to_zip_file(csv_file, add_timestamp=True)
    # report.attach_file_to_email(zip_file)
    # report.send_email()

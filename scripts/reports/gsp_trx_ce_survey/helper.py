# Import built-in packages
import logging

# Import local packages
from scripts.orion_report import OrionReport

logger = logging.getLogger(__name__)


def generate_report():

    report = OrionReport('Transactional CE Survey Listings for OFD and OF Biz')
    report.set_filename('OFD_OFBiz')
    report.set_prev_week_monday_sunday_date()
    query = report.get_query_from_file("query.sql")
    formatted_query = query.format(
        start_date=report.start_date, end_date=report.end_date)
    csv_file = report.query_to_csv(formatted_query, add_timestamp=True)
    zip_file = report.add_to_zip_file(csv_file, add_timestamp=True)
    report.attach_file_to_email(zip_file)
    report.send_email()

# Import built-in packages
import logging

# Import local packages
from scripts.orion_report import OrionReport

logger = logging.getLogger(__name__)


def generate_report():

    report = OrionReport('Transactional CE Survey Listings for OFD and OF Biz')
    report.set_filename('OFD_OFBiz')
    query = report.get_query_from_file("query.sql")
    csv_file = report.query_to_csv(query, add_timestamp=True)
    zip_file = report.add_to_zip_file(csv_file, add_timestamp=True)
    report.attach_file_to_email(zip_file)
    report.send_email()

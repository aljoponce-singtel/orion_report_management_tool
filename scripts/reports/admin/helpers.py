# Import built-in packages
import os
import logging

# Import third-party packages
import pandas as pd

# Import local packages
from os.path import basename
from scripts.helpers import utils
from scripts.orion_report import OrionReport

logger = logging.getLogger(__name__)


def query_to_file(query_file, filename, report_name='Quick Query'):

    report = OrionReport(report_name)
    report.set_filename(filename)
    query = report.get_query_from_file(query_file)
    csv_file = report.query_to_csv(query, add_timestamp=True)
    report.attach_file_to_email(csv_file)
    report.send_email()


def check_disk_usage():

    report = OrionReport("Disk Usage")
    path_to_check = "/"  # Replace with the path to the drive you want to check
    disk_usage_percentage = utils.check_disk_usage(path_to_check)
    disk_usage_treshold = report.default_config.getint('disk_usage_treshold')

    if disk_usage_percentage >= disk_usage_treshold:
        logger.warning(
            f"The drive at {path_to_check} is {disk_usage_percentage:.2f}% full.")
        logger.warning(
            f"PLEASE FREE UP SOME DRIVE SPACE NOT EXCEEDING {disk_usage_treshold}%.")

        email_body_html = f"""\
            <html>
            <p>Hello,</p>
            <p>The root drive "{path_to_check}" on SGOSSPRDO2PAPP01 is {disk_usage_percentage:.2f}% full.</p>
            <p>Please free up some drive space,</p>
            <p>&nbsp;</p>
            <p>Best regards,</p>
            <p>The Orion Team</p>
            </html>
            """

        report.set_email_body_html(email_body_html)
        report.add_email_receiver_to("jiangxu.jiang@singtel.com")
        report.attach_file_to_email(report.log_file_path)
        report.send_email()
    else:
        logger.info(
            f"The drive at '{path_to_check}' is {disk_usage_percentage:.2f} % full.")
        logger.info(
            f"It is still within the {report.default_config.get('disk_usage_treshold')} % threshold.")


def check_new_queueowners():

    report = OrionReport('New Queue Owner Updates')
    query = report.get_query_from_file("query_new_queueowner.sql")
    df = report.query_to_dataframe(query)

    if not df.empty:
        # Define a custom function to replace NaT with an empty string
        def replace_nat_with_empty_string(date):
            if pd.isna(date):
                return ''
            else:
                return date

        df_valid: pd.DataFrame
        # Valid email address and escalation enabled:
        df_valid = df[(df['is_valid'] == 'yes') & (df['is_enabled'] == 'yes')]
        # Reset and change starting index from 0 to 1 for proper table presentation
        df_valid = df_valid.reset_index(drop=True)
        df_valid.index += 1
        # Remove None values
        df_valid.fillna('', inplace=True)
        # Apply the custom function to replace NaT values
        df_valid['created_at'] = df_valid['created_at'].apply(
            replace_nat_with_empty_string)
        # remove is_valid column
        df_valid = df_valid.drop(['is_valid', 'is_enabled'], axis=1)
        # list unique users
        df_unique_valid_users = pd.DataFrame(df_valid['user'].unique(), columns=[
            'distinct_users']).sort_values(['distinct_users'])
        # Reset and change starting index from 0 to 1 for proper table presentation
        df_unique_valid_users = df_unique_valid_users.reset_index(drop=True)
        df_unique_valid_users.index += 1

        # Invalid email address:
        df_invalid = df[df['is_valid'] == 'no']
        # Reset and change starting index from 0 to 1 for proper table presentation
        df_invalid = df_invalid.reset_index(drop=True)
        df_invalid.index += 1
        # Remove None values
        df_invalid.fillna('', inplace=True)
        # Apply the custom function to replace NaT values
        df_invalid['created_at'] = df_invalid['created_at'].apply(
            replace_nat_with_empty_string)
        # remove is_valid column
        df_invalid = df_invalid.drop('is_valid', axis=1)
        # list unique users
        df_unique_invalid_users = pd.DataFrame(df_invalid['user'].unique(), columns=[
            'distinct_users']).sort_values(['distinct_users'])
        # Reset and change starting index from 0 to 1 for proper table presentation
        df_unique_invalid_users = df_unique_invalid_users.reset_index(
            drop=True)
        df_unique_invalid_users.index += 1

        email_body_html = f"""\
            <html>
            <p>Hello,</p>
            <p>Please see below the list of new/updated queue owners.</p>
            <p>VALID USERS:</p>
            <p>{df_valid.to_html()}</p>
            <p>{df_unique_valid_users.to_html()}</p>
            <p>INVALID USERS:</p>
            <p>{df_invalid.to_html()}</p>
            <p>{df_unique_invalid_users.to_html()}</p>
            <p>&nbsp;</p>
            <p>Best regards,</p>
            <p>The Orion Team</p>
            </html>
            """

        # Send email
        report.set_email_body_html(email_body_html)
        report.attach_file_to_email(report.log_file_path)

        if df_valid.empty:
            logger.info("NO NEW QUEUE OWNERS ADDED/UPDATED TODAY")
        else:
            logger.warning("There are new queue owners added/updated today.")
            logger.warning(
                "PLEASE ASSIGN/CREATE A USER ACCOUNT FOR THE NEW QUEUE OWNER/S.")
            report.send_email()

    else:
        logger.info("NO NEW QUEUE OWNERS ADDED/UPDATED TODAY")


def check_new_product_codes_to_map():

    report = OrionReport('New Product Codes To Map')
    query_file = os.path.join(report.sql_folder_path, "query_producttype.sql")
    query_update_file = os.path.join(
        report.sql_folder_path, "update_producttype.sql")
    query = report.get_query_from_file("query_producttype.sql")
    df = report.query_to_dataframe(query)

    if not df.empty:
        # Remove None values
        df.fillna('', inplace=True)
        # Reset and change starting index from 0 to 1 for proper table presentation
        df = df.reset_index(drop=True)
        df.index += 1

        query_file_txt = report.attach_file_to_email(
            query_file, append_file_ext='txt')
        query_update_file_txt = report.attach_file_to_email(
            query_update_file, append_file_ext='txt')

        email_body_html = f"""\
            <html>
            <p>Hello,</p>
            <p>Please see below the list of new product codes to be map in the product table.</p>
            <p>{df.to_html()}</p>
            <p>See attached file {basename(query_file_txt)} for the query.</p>
            <p>See attached file {basename(query_update_file_txt)} to map the product code.</p>
            <p>&nbsp;</p>
            <p>Best regards,</p>
            <p>The Orion Team</p>
            </html>
            """

        logger.warning("There are new product codes to map today.")
        logger.warning(
            "PLEASE MAP THE NEW PRODUCT CODE/S TO THE PRODUCT TABLE.")

        report.set_email_body_html(email_body_html)
        report.send_email()

    else:
        logger.info("NO NEW PRODUCT CODE/S TO MAP TODAY.")

# Import built-in packages
import os
import logging

# Import third-party packages
import pandas as pd

# Import local packages
from scripts.helpers import utils
from scripts.orion_report import OrionReport

logger = logging.getLogger(__name__)
configFile = os.path.join(os.path.dirname(__file__), 'config.ini')


def query_to_file(query_file, filename, report_name='Quick Query'):

    report = OrionReport(configFile, report_name)
    report.set_filename(filename)
    query_file = os.path.join(os.path.dirname(__file__), 'sql', query_file)
    query = None

    # Open the file in read mode
    with open(query_file, 'r') as file:
        # Read the entire contents of the file into a string
        query = file.read()

    csv_file = report.query_to_csv(query, add_timestamp=True)
    report.attach_file(csv_file)
    report.send_email()


def check_disk_usage():

    report = OrionReport(configFile, "Disk Usage")
    path_to_check = "/"  # Replace with the path to the drive you want to check
    disk_usage_percentage = utils.check_disk_usage(path_to_check)

    if disk_usage_percentage >= report.default_config.getint('disk_usage_treshold'):
        logger.warn(
            f"The drive at {path_to_check} is {disk_usage_percentage:.2f}% full.")

        # email_body_html = f"""\
        #     <html>
        #     <p>Hello,</p>
        #     <p>The drive at {path_to_check} is {disk_usage_percentage:.2f}% full.</p>
        #     <p>&nbsp;</p>
        #     <p>Best regards,</p>
        #     <p>The Orion Team</p>
        #     </html>
        #     """

        email_body_html = f"""\
            <html>
            <p>Hello,</p>
            <p>The root drive "{path_to_check}" on SGOSSPRDO2PAPP01 is {disk_usage_percentage:.2f}% full.</p>
            <p>&nbsp;</p>
            <p>Best regards,</p>
            <p>The Orion Team</p>
            </html>
            """

        report.set_email_body_html(email_body_html)
        report.add_email_receiver_to("jiangxu.jiang@singtel.com")
        report.attach_file(report.log_file_path)
        report.send_email()
    else:
        logger.info(
            f"The drive at {path_to_check} is {disk_usage_percentage:.2f}% full.")


def check_new_queueowners():

    report = OrionReport(configFile, 'New Queue Owner Updates')

    query = f"""
                SELECT
                    DISTINCT PER.role AS 'group_id',
                    PER.email AS 'user',
                    PER.user_id,
                    (
                        CASE
                            WHEN (
                                PER.email LIKE '%singtel.com'
                                AND NOT (
                                    PER.email LIKE '%,%'
                                    OR PER.email LIKE '%;%'
                                    OR PER.email LIKE '%\%'
                                    OR PER.email LIKE '%/%'
                                ) -- AND PER.email NOT REGEXP '[,;\/\\\\]'
                            ) THEN 'yes'
                            ELSE 'no'
                        END
                    ) AS is_valid,
                    (
                        CASE
                            WHEN GRP.is_enabled = 1 THEN 'yes'
                            ELSE 'no'
                        END
                    ) AS is_enabled,
                    USR_ACTUAL.username AS 'actual_user',
                    USR_ACTUAL.id AS 'actual_user_id',
                    USR_EXPECTED.username AS 'expected_user',
                    USR_EXPECTED.id AS 'expected_user_id',
                    PER.created_at,
                    PER.updated_at
                FROM
                    RestInterface_person PER
                    JOIN auto_escalation_escalationmatrix MTX ON MTX.person_id = PER.id
                    JOIN auto_escalation_escalationgroup GRP ON GRP.person_id = PER.id
                    LEFT JOIN RestInterface_user USR_ACTUAL ON USR_ACTUAL.id = PER.user_id
                    LEFT JOIN RestInterface_user USR_EXPECTED ON USR_EXPECTED.username = TRIM(PER.email)
                WHERE
                    PER.email != ''
                    AND (
                        TRIM(PER.email) != USR_ACTUAL.username
                        OR PER.user_id IS NULL
                    )
                ORDER BY
                    PER.email,
                    PER.role;
            """

    df = report.orion_db.query_to_dataframe(query)

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
        report.attach_file(report.log_file_path)

        if df_valid.empty:
            logger.info("NO NEW QUEUE OWNERS ADDED/UPDATED TODAY")
        else:
            report.send_email()

    else:
        logger.info("NO NEW QUEUE OWNERS ADDED/UPDATED TODAY")

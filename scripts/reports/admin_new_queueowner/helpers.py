# Import built-in packages
import os
import logging

# Import third-party packages
import pandas as pd

# Import local packages
import constants as const
from scripts.orion_report import OrionReport

logger = logging.getLogger(__name__)
configFile = os.path.join(os.path.dirname(__file__), 'config.ini')


def generate_report():

    report = OrionReport(configFile)
    report.set_email_subject(
        'New Queue Owner Updates', add_timestamp=True)

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
                    USR_ACTUAL.username AS 'actual_user',
                    USR_ACTUAL.id AS 'actual_user_id',
                    USR_EXPECTED.username AS 'expected_user',
                    USR_EXPECTED.id AS 'expected_user_id',
                    PER.created_at,
                    PER.updated_at
                FROM
                    RestInterface_person PER
                    JOIN auto_escalation_escalationmatrix MTX ON MTX.person_id = PER.id
                    LEFT JOIN RestInterface_user USR_ACTUAL ON USR_ACTUAL.id = PER.user_id
                    LEFT JOIN RestInterface_user USR_EXPECTED ON USR_EXPECTED.username = TRIM(PER.email)
                WHERE
                    PER.email != ''
                    AND (
                        TRIM(PER.email) != USR_ACTUAL.username
                        OR PER.user_id IS NULL
                    )
                    -- AND PER.email LIKE '%singtel%'
                ORDER BY
                    PER.email,
                    PER.role;
            """

    result = report.orion_db.query_to_list(query)

    if result:
        df = pd.DataFrame(data=result, columns=const.MAIN_COLUMNS)

        # set columns to datetime type
        df[const.DATE_COLUMNS] = df[const.DATE_COLUMNS].apply(
            pd.to_datetime)

        # Define a custom function to replace NaT with an empty string
        def replace_nat_with_empty_string(date):
            if pd.isna(date):
                return ''
            else:
                return date

        # Valid email address:
        df_valid = df[df['is_valid'] == 'yes']
        # Reset and change starting index from 0 to 1 for proper table presentation
        df_valid = df_valid.reset_index(drop=True)
        df_valid.index += 1
        # Remove None values
        df_valid.fillna('', inplace=True)
        # Apply the custom function to replace NaT values
        df_valid['created_at'] = df_valid['created_at'].apply(
            replace_nat_with_empty_string)
        # remove is_valid column
        df_valid = df_valid.drop('is_valid', axis=1)
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

        if df_valid.empty:
            logger.warn("NO NEW QUEUE OWNERS ADDED/UPDATED TODAY")
        else:
            report.send_email()

    else:
        logger.warn("NO NEW QUEUE OWNERS ADDED/UPDATED TODAY")

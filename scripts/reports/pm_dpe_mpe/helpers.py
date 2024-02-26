# Import built-in packages
import logging

# Import third-party packages
import pandas as pd

# Import local packages
from scripts.orion_report import OrionReport

logger = logging.getLogger(__name__)


def generate_report():

    report = OrionReport('Operation War room - DPE/MPE')
    report.set_filename('pm_dpe_mpe')
    report.set_prev_month_first_last_day_date()

    query = report.get_query_from_file("query.sql")
    formatted_query = query.format(
        start_date=report.start_date, end_date=report.end_date)
    df_raw = report.query_to_dataframe(formatted_query)
    df_raw['delay_reason'] = df_raw['delay_reason'].astype(str)
    df_raw['category'] = df_raw['category'].astype(str)

    # /****** START ******/
    # There are records where the category is included in the delay reason
    # E.g.
    # category = Others
    # delay reason = Others Deployed on 15 Jan 2023
    #
    # The code blow below will remove the category as a substring in the delay reason
    # output: delay reason = Deployed on 15 Jan 2023

    # define a custom function to remove a substring in column 'delay_reason'
    # using the string in column 'category'
    def remove_substring(row: pd.DataFrame):
        return row['delay_reason'].replace(row['category'], '')
    # apply the custom function to each row in the DataFrame
    df_raw['delay_reason'] = df_raw.apply(remove_substring, axis=1)
    # trim the string
    df_raw['delay_reason'] = df_raw['delay_reason'].str.strip()
    # /****** END ******/

    # remove leading '-' character from strings in the requestor column
    df_raw['requestor'] = df_raw['requestor'].str.lstrip('-')
    # Sort records in ascending order by order_code and note_code
    df_raw = df_raw.sort_values(
        by=['order_code', 'act_stepno'], ascending=[True, True])

    # Write to CSV
    csv_file = report.create_csv_from_df(df_raw, add_timestamp=True)
    # Send Email
    report.attach_file_to_email(csv_file)
    report.send_email()

    return

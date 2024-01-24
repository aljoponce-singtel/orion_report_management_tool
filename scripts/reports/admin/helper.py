# Import built-in packages
import os
import logging
import time

# Import third-party packages
import pandas as pd

# Import local packages
from models.order_service import create_order_service_list_class, create_order_service_match_class
from os.path import basename
from scripts.orion_report import OrionReport

logger = logging.getLogger(__name__)


def test_email(email_address, attachment_path=None, email_subject='[Orion] Test Email'):

    report = OrionReport(email_subject)
    report.add_email_receiver_to(email_address)

    if attachment_path:
        # The variable contains the filename with a path.
        if os.path.dirname(attachment_path):
            report.attach_file_to_email(attachment_path)
        # The variable contains only the filename.
        else:
            # Get the file inside the default reports folder path
            report.attach_file_to_email(os.path.join(
                report.reports_folder_path, attachment_path))

    report.send_email()


def query_to_csv(query_file, filename, db_name=None, report_name='Quick Query'):

    report = OrionReport(report_name)
    report.set_filename(filename)
    query = report.get_query_from_file(query_file)
    csv_file = None

    if db_name == "o2pprod" or db_name == "o2puat":
        csv_file = report.query_to_csv(
            query, db=report.orion_db, add_timestamp=True)
    elif db_name == "pegasusmulesoft":
        csv_file = report.query_to_csv(
            query, db=report.staging_db, add_timestamp=True)
    elif db_name == "o2ptableau":
        csv_file = report.query_to_csv(
            query, db=report.tableau_db, add_timestamp=True)
    else:
        # table_name == "o2ptest":
        csv_file = report.query_to_csv(
            query, db=report.test_db, add_timestamp=True)

    if csv_file:
        report.attach_file_to_email(csv_file)
        report.send_email()


def query_to_excel(query_file, filename, db_name=None, report_name='Quick Query'):

    report = OrionReport(report_name)
    report.set_filename(filename)
    query = report.get_query_from_file(query_file)
    csv_file = None

    if db_name == "o2pprod" or db_name == "o2puat":
        csv_file = report.query_to_excel(
            query, db=report.orion_db, add_timestamp=True)
    elif db_name == "pegasusmulesoft":
        csv_file = report.query_to_excel(
            query, db=report.staging_db, add_timestamp=True)
    elif db_name == "o2ptableau":
        csv_file = report.query_to_excel(
            query, db=report.tableau_db, add_timestamp=True)
    else:
        # table_name == "o2ptest":
        csv_file = report.query_to_excel(
            query, db=report.test_db, add_timestamp=True)

    if csv_file:
        report.attach_file_to_email(csv_file)
        report.send_email()


def insert_records(table_name, column_name, records_file, db_name=None, report_name='Insert Records'):

    report = OrionReport(report_name)

    with open(records_file, 'r') as file:
        records = [line.strip() for line in file]

    insert_query = "INSERT INTO {} ({}) VALUES (%s)".format(
        table_name, column_name)
    records = [(record,) for record in records]

    if db_name == "o2pprod" or db_name == "o2puat":
        inserted_row_count = report.orion_db.sql_insert(insert_query, records)
    elif db_name == "pegasusmulesoft":
        inserted_row_count = report.staging_db.sql_insert(
            insert_query, records)
    elif db_name == "o2ptableau":
        inserted_row_count = report.tableau_db.sql_insert(
            insert_query, records)
    else:
        # table_name == "o2ptest":
        inserted_row_count = report.test_db.sql_insert(insert_query, records)

    logger.info(f"No. of inserted rows: {inserted_row_count}")


def retrieve_orderid_of_serviceno(serviceno_list_file, batch_size=200, table_name=None, report_name="Retrieve OrderInfo of ServiceNo"):

    report = OrionReport(report_name)
    records = report.get_list_from_file(serviceno_list_file)

    try:
        if table_name:
            OrderServiceList = create_order_service_list_class(
                f"order_service_list_{table_name}")
            OrderServiceMatch = create_order_service_match_class(
                f"order_service_match_{table_name}")
        else:
            OrderServiceList = create_order_service_list_class(
                'order_service_list')
            OrderServiceMatch = create_order_service_match_class(
                'order_service_match')
        # User o2ptest database
        db = report.test_db
        # Create table if not exist and insert the service numbers from the file
        table_name_list = OrderServiceList.__tablename__
        table_model_list = OrderServiceList
        db.create_table_from_metadata(table_model_list)
        insert_query = "INSERT IGNORE INTO {} (ServiceNumber) VALUES (%s)".format(
            table_name_list)
        records = [(record,) for record in records]
        inserted_row_count = db.sql_insert(
            insert_query, records, query_description="service numbers")
        logger.info(f"No. of inserted rows: {inserted_row_count}")
        # Get the list of service numbers from the table
        df_records = db.query_to_dataframe(
            f"SELECT DISTINCT * FROM {table_name_list}", query_description="unique service numbers")
        list_records = df_records['ServiceNumber'].to_list()
        # Create table if not exist
        table_name_match = OrderServiceMatch.__tablename__
        table_model_match = OrderServiceMatch
        db.create_table_from_metadata(table_model_match)
        # Process in batches
        logger.info(f"Processing in batches of {batch_size} ...")
        # Monitor time of batch processing
        start_time = time.time()
        # Loop through each batch
        for i in range(0, len(list_records), batch_size):
            # Get the current batch to process
            batch_list = list_records[i:i + batch_size]
            # Perform your processing on the current batch
            query = f'''
                        SELECT DISTINCT
                            ORD.id         AS OrderId
                        , ORD.order_code AS OrderCode
                        , ORD.taken_date AS CreatedDate
                        , SVC.ServiceNumber
                        FROM
                            {report.test_db.database}.{table_name_list} SVC
                            LEFT JOIN
                                RestInterface_order ORD
                                ON
                                    ORD.service_number = SVC.ServiceNumber
                        WHERE
                            SVC.ServiceNumber         IN ({report.list_to_string(batch_list)})
                            AND SVC.ServiceNumber NOT IN
                            (
                                SELECT DISTINCT
                                    ServiceNumber
                                FROM
                                    {report.test_db.database}.{table_name_match}
                            )
                        ;
                '''
            df = report.query_to_dataframe(
                query, db=report.orion_db, query_description="order info of service numbers")
            logger.info(
                f"Inserting records to {db.database}.{table_name_match} ...")
            # insert records to DB
            db.insert_df_to_table(df, table_name_match)
            # Progress information
            end_time = time.time()
            elapsed_time = end_time - start_time
            total_processed = len(list_records) if i + \
                batch_size > len(list_records) else i+batch_size
            logger.info(
                f"{total_processed}/{len(list_records)} total records processed in {report.format_seconds(elapsed_time)}")

    except Exception as err:
        logger.exception(err)
        raise Exception(err)


def load_csv_to_table(csv_file, database_name=None, table_name=None, columns: list = None, datetime_columns: list = None, date_columns: list = None, report_name='CSV to DB', chunk_size: int = 1000):

    report = OrionReport(report_name)

    csv_file_path = os.path.join(
        os.path.dirname(__file__), 'resource', csv_file)

    if columns:
        df = pd.read_csv(csv_file_path,
                         #  encoding='utf-8',
                         #  header='infer',
                         skiprows=1,
                         header=None,
                         names=columns,
                         engine='python')
    else:
        df = pd.read_csv(csv_file_path,
                         engine='python')

    # logger.warning(f"\n{str(df)}")
    # logger.warning(df.dtypes)

    # convert to datetime
    if datetime_columns:
        df[datetime_columns] = df[datetime_columns].apply(
            pd.to_datetime)

    if date_columns:
        for date_column in date_columns:
            date_column: str
            df[date_column] = pd.to_datetime(
                df[date_column], format='%d/%m/%Y').dt.date

    # logger.warning(f"\n{df}")
    # logger.warning(df.dtypes)

    try:
        if database_name == "o2pprod":
            report.orion_db.insert_df_to_table(
                df, table_name, chunk_size=chunk_size)
        elif database_name == "pegasusmulesoft":
            report.staging_db.insert_df_to_table(
                df, table_name, chunk_size=chunk_size)
        elif database_name == "o2ptableau":
            report.tableau_db.insert_df_to_table(
                df, table_name, chunk_size=chunk_size)
        else:
            # table_name == "o2ptest":
            report.test_db.insert_df_to_table(
                df, table_name, chunk_size=chunk_size)

    except Exception as e:
        # Log including the stack trace
        logger.error(
            f"Failed to load data into database: {e}")

        # Log only the error description, as the raw error info maybe too large
        # Ignore logging the stack trace
        # logger.error(
        #     f"Failed to load data into database: {e.args[0]}")


def get_superusers():

    report = OrionReport("Orion list of superusers")

    # Today's date is the last day of the month
    if report.get_today_date() == report.get_last_day_of_month():
        query = report.get_query_from_file("query_superuser.sql")
        df = report.query_to_dataframe(query)

        if not df.empty:
            # Reset and change starting index from 0 to 1 for proper table presentation
            df = df.reset_index(drop=True)
            df.index += 1

            # Create email body
            email_body_html = f"""\
                    <html>
                    <p>Hello,</p>
                    <p>Please see below the list of Orion superusers as of today.</p>
                    <p>{df.to_html()}</p>
                    <p>&nbsp;</p>
                    <p>Best regards,</p>
                    <p>The Orion Team</p>
                    </html>
                    """
            # Send email
            report.set_email_body_html(email_body_html)
            report.add_email_receiver_to("jiangxu.jiang@singtel.com")
            report.send_email()


def check_disk_usage():

    report = OrionReport("Disk Usage")
    path_to_check = "/"  # Replace with the path to the drive you want to check
    disk_usage_percentage = report.check_disk_usage(path_to_check)
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
    query_file = os.path.join(report.sql_folder_path,
                              "query_new_queueowner.sql")
    query_update_file = os.path.join(
        report.sql_folder_path, "update_new_queueowner.sql")
    query = report.get_query_from_file("query_new_queueowner.sql")
    df = report.query_to_dataframe(query)

    if not df.empty:
        # Define a custom function to replace NaT with an empty string
        def replace_nat_with_empty_string(date):
            if pd.isna(date):
                return ''
            else:
                return date

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
        # Remove is_valid column
        df_valid = df_valid.drop(['is_valid', 'is_enabled'], axis=1)

        '''''
        Because there were NULL values for the user_id as an INT data type queried from MySQL,
        transforming the result to dataframe will cause Pandas to convert the datatype to float.
        Int type in Pandas doesn't accept NULL values as compared to float data type.
        Because float accepts decimal values, Pandas will add a decimal value to the whole number.
        For example, the INT value 423 will be converted to 423.00 float value.
        Depending on how you want to display the value, you can remove the decimal point during post-process.
        '''''
        # Define int data types for the columns for conversion
        dtype_int_mapping = {
            'user_id': int,
            'actual_user_id': int,
            'expected_user_id': int
        }
        int_columns = ['user_id', 'actual_user_id', 'expected_user_id']
        # Replace NaN values in int_columns with 0 to allow INT conversion
        df_valid[int_columns] = df_valid[int_columns].fillna(0)
        # Replace empty '' values in int_columns with 0 to allow INT conversion
        df_valid[int_columns] = df_valid[int_columns].replace('', 0)
        # Convert data types of DataFrame columns to INT to remove the decimal points.
        df_valid = df_valid.astype(dtype_int_mapping)
        # Define string data types for the columns for conversion
        dtype_str_mapping = {
            'user_id': str,
            'actual_user_id': str,
            'expected_user_id': str
        }
        # Convert data types of DataFrame columns to String.
        df_valid = df_valid.astype(dtype_str_mapping)
        # Replace the 0 values to ''.
        df_valid[int_columns] = df_valid[int_columns].replace('0', '')

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
        # Remove is_valid column
        df_invalid = df_invalid.drop('is_valid', axis=1)
        # list unique users
        df_unique_invalid_users = pd.DataFrame(df_invalid['user'].unique(), columns=[
            'distinct_users']).sort_values(['distinct_users'])
        # Reset and change starting index from 0 to 1 for proper table presentation
        df_unique_invalid_users = df_unique_invalid_users.reset_index(
            drop=True)
        df_unique_invalid_users.index += 1

        if df_valid.empty:
            logger.info("NO NEW QUEUE OWNERS ADDED/UPDATED TODAY")
        else:
            logger.warning("There are new queue owners added/updated today.")
            logger.warning(
                "PLEASE ASSIGN/CREATE A USER ACCOUNT FOR THE NEW QUEUE OWNER/S.")

            # Attach query and update SQL files
            query_file_txt = report.attach_file_to_email(
                query_file, append_file_ext='txt')
            query_update_file_txt = report.attach_file_to_email(
                query_update_file, append_file_ext='txt')
            # Create email body
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
                <p>See attached file {basename(query_file_txt)} for the query.</p>
                <p>See attached file {basename(query_update_file_txt)} to add/update the queue owners.</p>
                <p>&nbsp;</p>
                <p>Best regards,</p>
                <p>The Orion Team</p>
                </html>
                """
            # Send email
            report.set_email_body_html(email_body_html)
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

        logger.warning("There are new product codes to map today.")
        logger.warning(
            "PLEASE MAP THE NEW PRODUCT CODE/S TO THE PRODUCT TABLE.")

        # Attach query and update SQL files
        query_file_txt = report.attach_file_to_email(
            query_file, append_file_ext='txt')
        query_update_file_txt = report.attach_file_to_email(
            query_update_file, append_file_ext='txt')
        # Create email body
        email_body_html = f"""\
            <html>
            <p>Hello,</p>
            <p>Please see below the list of new product codes to be map in the product table.</p>
            <p>{df.to_html()}</p>
            <p>See attached file {basename(query_file_txt)} for the query.</p>
            <p>See attached file {basename(query_update_file_txt)} to map the product codes.</p>
            <p>&nbsp;</p>
            <p>Best regards,</p>
            <p>The Orion Team</p>
            </html>
            """
        # Send email
        report.set_email_body_html(email_body_html)
        report.send_email()

    else:
        logger.info("NO NEW PRODUCT CODE/S TO MAP TODAY.")

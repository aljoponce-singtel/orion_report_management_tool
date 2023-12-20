# Import built-in packages
import logging
import time

# Import third-party packages
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.engine.cursor import LegacyCursorResult
from sqlalchemy.ext.declarative import DeclarativeMeta
from sqlalchemy.sql import text

logger = logging.getLogger(__name__)


class DbConnection:
    def __init__(self, dbapi: str, host: str, port: str, database: str, user: str, password: str):
        self.dbapi = dbapi
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.conn = None
        self.engine = None
        self.metadata = None

    def connect(self):
        try:
            self.engine = create_engine(
                '{}://{}:{}@{}:{}/{}'.format(self.dbapi, self.user, self.password, self.host, self.port, self.database), echo=False)
            self.conn = self.engine.connect()

            logger.info("Connected to DB " + self.database + ' at ' +
                        self.user + '@' + self.host + ':' + self.port)

            self.metadata = MetaData(self.engine)

        except Exception as err:
            logger.error("Failed to connect to DB " + self.database + ' at ' +
                         self.user + '@' + self.host + ':' + self.port + '.')
            logger.exception(err)
            raise Exception(err)

    def get_table_metadata(self, table_name, alias=None):

        table = None

        if alias:
            table = Table(table_name, self.metadata,
                          autoload=True).alias(alias)
        else:
            table = Table(table_name, self.metadata, autoload=True)

        return table

    # Only works when log_level=debug
    def log_full_query(self, query):
        if type(query) is str:
            logger.debug(query)
        else:
            logger.debug(query.compile(self.engine,
                                       compile_kwargs={"literal_binds": True}))

    def create_table_from_metadata(self, table: DeclarativeMeta):
        return table.metadata.create_all(self.engine)

    def truncate_table(self, tableName):
        table = self.get_table_metadata(tableName)
        self.conn.execute(table.delete())

    def drop_table(self, table):
        table_to_drop = table

        if type(table) is str:
            table_to_drop = self.get_table_metadata(table)

        logger.info(f"Dropping table {table_to_drop.name} ...")

        return table_to_drop.metadata.drop_all(self.engine)

    def sql_update(self, query, data=None, query_description=None):

        # Only works when log_level=debug
        self.log_full_query(query)

        # Set default query description
        if not query_description:
            query_description = 'records'

        logger.info(f"[DB:{self.database}] Updating {query_description} ...")

        result = []
        query_type = type(query)
        start_time = time.time()
        result: LegacyCursorResult

        # query has data
        if data:
            result = self.conn.execute(query, data)
        # query is a an SQL text
        elif query_type is str:
            result = self.conn.execute(text(query))
        # query is a constructed SQL expression
        elif query_type.__name__ == 'Select':
            result = self.conn.execute(query)
        else:
            raise Exception("INVALID SQL STATEMENT")

        end_time = time.time()
        elapsed_time = end_time - start_time

        logger.debug(
            f"Update completion time: {self.__format_seconds(elapsed_time)}")

        # Get the number of rows updated
        updated_row_count = result.rowcount
        logger.debug(f"No. of updated rows: {updated_row_count}")

        return updated_row_count

    def sql_select(self, query, data=None, query_description=None):

        # Only works when log_level=debug
        self.log_full_query(query)

        # Set default query description
        if not query_description:
            query_description = 'records'

        logger.info(f"[DB:{self.database}] Querying {query_description} ...")

        result = []
        query_type = type(query)
        start_time = time.time()
        result: LegacyCursorResult

        # query has data
        if data:
            result = self.conn.execute(query, data)
        # query is a an SQL text
        elif query_type is str:
            result = self.conn.execute(text(query))
        # query is a constructed SQL expression
        elif query_type.__name__ == 'Select':
            result = self.conn.execute(query)
        else:
            raise Exception("INVALID SQL STATEMENT")

        end_time = time.time()
        elapsed_time = end_time - start_time

        logger.info(
            f"Query completion time: {self.__format_seconds(elapsed_time)}")

        return result

    def query_to_list(self, query, data=None, query_description=None):

        result = self.sql_select(query, data, query_description)
        # Get the list of records
        result_list = result.fetchall()
        no_of_rows_retrieved = len(result_list)
        logger.info(f"Number of rows retrieved: {no_of_rows_retrieved}")
        # Check if the query result is empty
        if no_of_rows_retrieved == 0:
            logger.warning('Query result is empty.')

        return result_list

    def query_to_dataframe(self, query, data=None, query_description=None, column_names=[], datetime_to_date=False) -> pd.DataFrame:

        df = pd.DataFrame()
        # Perform a query
        result = self.sql_select(query, data, query_description)

        # Get the columns that are of type DATE or DATETIME
        non_date_type_columns = []
        date_type_columns = []
        # Create dictionary for date/datetime mapping
        MYSQL_DATETYPE_DICT = {
            10: "DATE",
            12: "DATETIME"
        }
        # Create dictionary for non-date/datetime mapping
        MYSQL_NON_DATETYPE_DICT = {
            3: "INT",
            252: "LONGTEXT",
            253: "TEXT"
        }
        # Iterate through the result's column list to identify the column's data types
        for column_name, type_code, display_size, internal_size, precision, scale, nullability in result.cursor.description:
            # Add date/datetime columns to a list
            if type_code in MYSQL_DATETYPE_DICT.keys():
                date_type_columns.append(
                    [column_name, type_code, MYSQL_DATETYPE_DICT.get(type_code)])
            # Add non-date/datetime columns to a list
            if type_code not in MYSQL_DATETYPE_DICT.keys():
                if type_code in MYSQL_NON_DATETYPE_DICT.keys():
                    non_date_type_columns.append(
                        [column_name, type_code, MYSQL_NON_DATETYPE_DICT.get(type_code)])
                else:
                    # Map the unknown type_code
                    non_date_type_columns.append(
                        [column_name, type_code, type_code])
        # Print the list of date/datetime columns
        logger.debug(
            f"Date/Datetime columns: {[[record[0], record[2]] for record in date_type_columns]}")
        # Print the list of non-date/datetime columns
        logger.debug(
            f"Non-Date/Datetime columns: {[[record[0], record[2]] for record in non_date_type_columns]}")
        # Get the list of records
        result_list = result.fetchall()
        no_of_rows_retrieved = len(result_list)
        logger.info(f"Number of rows retrieved: {no_of_rows_retrieved}")
        # Check if the query result is empty
        if no_of_rows_retrieved == 0:
            logger.warning('Query result is empty.')
        # Create a dataframe from the result
        if len(column_names) == 0:
            # Extracting the column names from the query is required for Pandas package version 1.1.5 and below
            # The columns will be in numbers by default if not explicitly extracted.
            # Get the list of column names directly from the query if column_names is empty
            df = pd.DataFrame(data=result_list,
                              columns=result.keys())
        else:
            # Get the list of column names from column_names if provided
            df = pd.DataFrame(data=result_list, columns=column_names)
        # If enabled, convert all date/datetime columns to datetime
        if datetime_to_date:
            # # set columns to datetime type
            # df[date_type_columns] = df[date_type_columns].apply(
            #     pd.to_datetime)

            # Filter and extract the first element of each sub-list for DATETIME records
            datetime_columns = [record[0]
                                for record in date_type_columns if record[2] == 'DATETIME']
            logger.info(f"Converting datetime columns to date ...")
            # convert datetime to date (remove time)
            for column in datetime_columns:
                df[str(column)] = pd.to_datetime(
                    df[str(column)]).dt.date

        return df

    def insert_df_to_table(self, dataframe, table, if_exist=None, chunk_size=None):
        logger.info(f'Inserting records to {self.database}.{table} ...')

        no_of_affected_rows = 0
        df = pd.DataFrame(dataframe)

        '''
        Returns:
            None or int
                Number of rows affected by to_sql. None is returned if the callable passed into method does not return an integer number of rows.
                The number of returned rows affected is the sum of the rowcount attribute of sqlite3.Cursor or SQLAlchemy connectable 
                which may not reflect the exact number of written rows as stipulated in the sqlite3 or SQLAlchemy.

        Raises:
            ValueError
                When the table already exists and if_exists is ‘fail’ (the default).
        '''
        # If chunk_size is not 0 or NULL
        if not chunk_size:
            no_of_affected_rows = df.to_sql(table,
                                            #   con=self.engine,
                                            con=self.engine,
                                            #   indexbool, default True
                                            #         Write DataFrame index as a column. Uses index_label as the column name in the table.
                                            index=False,
                                            # if_exists : {‘fail’, ‘replace’, ‘append’}, default ‘fail’
                                            #     How to behave if the table already exists.
                                            #     fail: Raise a ValueError.
                                            #     replace: Drop the table before inserting new values.
                                            #     append: Insert new values to the existing table.
                                            if_exists='append' if not if_exist else if_exist,
                                            # method : {None, ‘multi’, callable}, optional
                                            #     Controls the SQL insertion clause used:
                                            #     None : Uses standard SQL INSERT clause (one per row).
                                            #     ‘multi’: Pass multiple values in a single INSERT clause.
                                            #     callable with signature (pd_table, conn, keys, data_iter).
                                            method='multi')
        # Insert in batches (chunk_size)
        else:
            logger.info(
                f"Inserting in chunk sizes of {chunk_size} ...")
            no_of_affected_rows = df.to_sql(table,
                                            con=self.engine,
                                            index=False,
                                            if_exists='append' if not if_exist else if_exist,
                                            chunksize=chunk_size,
                                            method='multi')

        logger.info(f"Number of affected rows: {no_of_affected_rows}")

        return no_of_affected_rows

    # private method
    def __format_seconds(self, seconds):
        # Calculate hours, minutes, and seconds
        hours, seconds = divmod(seconds, 3600)
        minutes, seconds = divmod(seconds, 60)
        # Round seconds to two decimal places
        seconds = round(seconds, 2)
        # Create a formatted string
        formatted_duration = f"{int(hours)}:{int(minutes):02}:{seconds:05.2f}"

        return formatted_duration

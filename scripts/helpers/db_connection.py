import logging
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.sql import text

logger = logging.getLogger(__name__)


class DbConnection:
    def __init__(self, dbapi, host, port, database, user, password):
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

    def log_full_query(self, query):
        logger.debug(query.compile(self.engine,
                                   compile_kwargs={"literal_binds": True}))

    def create_table_from_metadata(self, table):
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

    def query_to_list(self, query, data=None):
        query_type = type(query)

        # query has data
        if data:
            return self.conn.execute(query, data).fetchall()
        # query is a an SQL text
        elif query_type is str:
            return self.conn.execute(text(query)).fetchall()
        # query is a constructed SQL expression
        elif query_type.__name__ == 'Select':
            return self.conn.execute(query).fetchall()
        else:
            raise Exception("INVALID QUERY")

    def insert_df_to_table(self, dataframe, table, if_exist=None):
        logger.info(f'Inserting records to {table} table ...')

        df = pd.DataFrame(dataframe)
        df.to_sql(table,
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
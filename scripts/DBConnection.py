import logging
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.sql import text
import pymysql

logger = logging.getLogger(__name__)
pymysql.install_as_MySQLdb()


class DBConnection:
    def __init__(self, host, port, database, user, password):
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.__conn = None
        self.__engine = None

    def connect(self):
        try:
            self.__engine = create_engine(
                'mysql://{}:{}@{}:{}/{}'.format(self.user, self.password, self.host, self.port, self.database))
            self.__conn = self.__engine.connect()

            logger.info("Connected to DB " + self.database + ' at ' +
                        self.user + '@' + self.host + ':' + self.port)

        except Exception as err:
            logger.error("Failed to connect to DB " + self.database + ' at ' +
                         self.user + '@' + self.host + ':' + self.port + '.')
            logger.exception(err)
            raise Exception(err)

    # not used as the desctructor __del__ will take care of it
    def disconnect(self):
        self.__conn.close()
        logger.info("Disconnected to " + self.database + '.')

    def getTableMetadata(self, table):
        metadata = MetaData()
        tableMetadata = Table(table, metadata,
                              autoload=True, autoload_with=self.__engine)
        return tableMetadata

    def queryToList(self, query):
        dataset = self.__conn.execute(text(query)).fetchall()
        return dataset

    def queryToList2(self, query):
        dataset = self.__conn.execute(query).fetchall()
        return dataset

    def insertDataframeToTable(self, dataframe, table):
        df = pd.DataFrame(dataframe)
        df.to_sql(table,
                  con=self.__engine,
                  index=False,
                  if_exists='append',
                  method='multi')

    def __del__(self):
        if self.__conn:
            self.__conn.close()

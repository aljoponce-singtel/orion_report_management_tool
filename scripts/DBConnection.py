import logging
import pandas as pd
from sqlalchemy import create_engine
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
            __engine = create_engine(
                'mysql://{}:{}@{}:{}/{}'.format(self.user, self.password, self.host, self.port, self.database))
            self.__conn = __engine.connect()

            logger.info("Connected to DB " + self.database + ' at ' +
                        self.user + '@' + self.host + ':' + self.port)

        except Exception as err:
            logger.error("Failed to connect to DB " + self.database + ' at ' +
                         self.user + '@' + self.host + ':' + self.port + '.')
            logger.error(err)
            raise Exception(err)

    def disconnect(self):
        self.__conn.close()
        logger.info("Disconnected to " + self.database + '.')

    def queryToList(self, query):
        dataset = self.__conn.execute(text(query)).fetchall()
        return dataset

    def dataframeToDb(self, dataframe, table):
        # verify if pd.DataFrame will receive dataframe as reference
        df = pd.DataFrame(dataframe)
        df.to_sql(table,
                  con=self.__engine,
                  index=False,
                  if_exists='append',
                  method='multi')

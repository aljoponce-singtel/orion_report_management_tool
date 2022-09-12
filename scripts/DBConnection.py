import logging
import pandas as pd
from sqlalchemy import create_engine, MetaData
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
        self.__metadata = None

    def connect(self):
        try:
            self.__engine = create_engine(
                'mysql://{}:{}@{}:{}/{}'.format(self.user, self.password, self.host, self.port, self.database))
            self.__conn = self.__engine.connect()

            logger.info("Connected to DB " + self.database + ' at ' +
                        self.user + '@' + self.host + ':' + self.port)

            self.__metadata = MetaData()
            self.__metadata.reflect(bind=self.__engine)

        except Exception as err:
            logger.error("Failed to connect to DB " + self.database + ' at ' +
                         self.user + '@' + self.host + ':' + self.port + '.')
            logger.exception(err)
            raise Exception(err)

    # not used as the desctructor __del__ will take care of it
    def disconnect(self):
        self.__conn.close()
        logger.info("Disconnected to " + self.database + '.')

    def getTableMetadata(self, tableName, alias=None):

        table = None

        if alias:
            table = self.__metadata.tables[tableName].alias(alias)
        else:
            table = self.__metadata.tables[tableName]

        return table

    def logQuery(self, query):
        # logger.info(query.compile(self.__engine))
        # logger.info(query.compile(dialect=mysql.dialect()))
        # logger.info(query.compile(compile_kwargs={"literal_binds": True}))
        # logger.info(query.compile(dialect=mysql.dialect(), compile_kwargs={"literal_binds": True}))
        logger.debug(query.compile(self.__engine,
                                   compile_kwargs={"literal_binds": True}))

    def createTablesFromMetadata(self, table):
        return table.metadata.create_all(self.__engine)

    # Query DB using a string
    def queryToList(self, query):
        dataset = self.__conn.execute(text(query)).fetchall()
        return dataset

    # Quewry DB using an object
    def queryToList2(self, query):
        dataset = self.__conn.execute(query).fetchall()
        return dataset

    def queryWithoutResult(self, query):
        self.__conn.execute(text(query))

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

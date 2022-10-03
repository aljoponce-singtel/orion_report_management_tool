import logging
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.sql import text

logger = logging.getLogger(__name__)


class DBConnection:
    def __init__(self, dbapi, host, port, database, user, password):
        self.dbapi = dbapi
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
                '{}://{}:{}@{}:{}/{}'.format(self.dbapi, self.user, self.password, self.host, self.port, self.database))
            self.__conn = self.__engine.connect()

            logger.info("Connected to DB " + self.database + ' at ' +
                        self.user + '@' + self.host + ':' + self.port)

            self.__metadata = MetaData(self.__engine)

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
            table = Table(tableName, self.__metadata,
                          autoload=True).alias(alias)
        else:
            table = Table(tableName, self.__metadata, autoload=True)

        return table

    def logFullQuery(self, query):
        logger.debug(query.compile(self.__engine,
                                   compile_kwargs={"literal_binds": True}))

    def createTablesFromMetadata(self, table):
        return table.metadata.create_all(self.__engine)

    def queryToList(self, query, data=None):
        queryType = type(query)

        # query has data
        if data:
            return self.__conn.execute(query, data).fetchall()
        # query is a an SQL text
        elif queryType is str:
            return self.__conn.execute(text(query)).fetchall()
        # query is a constructed SQL expression
        elif queryType.__name__ == 'Select':
            return self.__conn.execute(query).fetchall()
        else:
            logger.error("Failed to send email.")
            raise Exception("INVALID QUERY")

    def insertIntoTable(self, statement):
        return self.__conn.execute(statement)

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

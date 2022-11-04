import logging
import pandas as pd
from sqlalchemy import create_engine, MetaData, Table
from sqlalchemy.orm import sessionmaker
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
        self.__sqlSession = None

    def connect(self):
        try:
            self.__engine = create_engine(
                '{}://{}:{}@{}:{}/{}'.format(self.dbapi, self.user, self.password, self.host, self.port, self.database), echo=False)
            self.__conn = self.__engine.connect()

            logger.info("Connected to DB " + self.database + ' at ' +
                        self.user + '@' + self.host + ':' + self.port)

            self.__metadata = MetaData(self.__engine)

            Session = sessionmaker(bind=self.__engine)
            self.__sqlSession = Session()

        except Exception as err:
            logger.error("Failed to connect to DB " + self.database + ' at ' +
                         self.user + '@' + self.host + ':' + self.port + '.')
            logger.exception(err)
            raise Exception(err)

    def getSqlSession(self):
        return self.__sqlSession

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

    def truncateTable(self, tableName):
        table = self.getTableMetadata(tableName)
        self.__conn.execute(table.delete())

    def dropTableFromMetadata(self, table):
        return table.metadata.drop_all(self.__engine)

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
            raise Exception("INVALID QUERY")

    def insertIntoTable(self, statement):
        return self.__conn.execute(statement)

    def insertDataframeToTable(self, dataframe, table):
        logger.info(f'Inserting records to {table} table ...')

        df = pd.DataFrame(dataframe)
        df.to_sql(table,
                  con=self.__engine,
                  index=False,
                  if_exists='append',
                  method='multi')

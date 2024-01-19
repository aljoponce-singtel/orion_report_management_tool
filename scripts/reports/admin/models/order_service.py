from sqlalchemy import Column, PrimaryKeyConstraint, Integer, String, Date
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


def create_order_service_list_class(tablename):
    class OrderServiceList(Base):
        __tablename__ = tablename
        __table_args__ = (PrimaryKeyConstraint('ServiceNumber'),)

        ServiceNumber = Column(String(80))

        def __repr__(self):
            return "<OrderServiceList(ServiceNumber='%s')>" % (self.ServiceNumber)

    return OrderServiceList


def create_order_service_match_class(tablename):
    class OrderServiceMatch(Base):
        __tablename__ = tablename
        id = Column(Integer, primary_key=True, autoincrement=True)
        OrderId = Column(Integer, index=True)
        OrderCode = Column(String(20))
        ServiceNumber = Column(String(80))
        CreatedDate = Column(Date())

        def __repr__(self):
            return "<OrderServiceMatch(OrderCode='%s')>" % (self.OrderCode)

    return OrderServiceMatch

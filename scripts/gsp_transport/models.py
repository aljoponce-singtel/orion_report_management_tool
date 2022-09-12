"""SQLAlchemy Data Models."""
from sqlalchemy import func, Column, PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import String, Date, DateTime

TransportBase = declarative_base()


class Transport(TransportBase):
    __tablename__ = 't_GSP_ip_transport_dev'
    __table_args__ = (PrimaryKeyConstraint(
        'OrderCode', 'update_time'),)

    Service = Column(String(20))
    OrderCode = Column(String(20), nullable=False)
    CRD = Column(Date(), nullable=False)
    ServiceNumber = Column(String(80))
    OrderStatus = Column(String(20), nullable=False)
    OrderType = Column(String(20), nullable=False)
    ProductCode = Column(String(20))
    PreConfig_GroupID = Column(String(10))
    PreConfig_ActName = Column(String(50))
    PreConfig_ActStatus = Column(String(10))
    PreConfig_ActDueDate = Column(Date())
    PreConfig_COM_Date = Column(Date())
    Coordination_GroupID = Column(String(10))
    Coordination_ActName = Column(String(50))
    Coordination_ActStatus = Column(String(10))
    Coordination_ActDueDate = Column(Date())
    Coordination_COM_Date = Column(Date())
    update_time = Column(DateTime(timezone=True),
                         server_default=func.now())

    def __repr__(self):
        return "<Transport(OrderCode='%s', update_time='%s')>" % (self.OrderCode, self.update_time)

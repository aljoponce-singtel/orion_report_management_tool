"""SQLAlchemy Data Models."""
from sqlalchemy import func, Column, PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import String, Date, DateTime

Base = declarative_base()


class Sgo(Base):
    __tablename__ = 't_GSP_ip_sgo_ollc_transport'
    __table_args__ = (PrimaryKeyConstraint(
        'Workorder', 'update_time'),)

    Workorder = Column(String(20), nullable=False)
    ServiceNo = Column(String(20))
    CustomerName = Column(String(80))
    ActivityName = Column(String(50))
    GroupID = Column(String(10))
    CRD = Column(Date(), nullable=False)
    OrderType = Column(String(20), nullable=False)
    PONo = Column(String(80))
    NPC = Column(String(20))
    OrderCreationDate = Column(Date())
    ActStatus = Column(String(10))
    CommDate = Column(Date())
    OriginatingCountry = Column(String(20))
    OriginatingCarrier = Column(String(80))
    MainSvcType = Column(String(50))
    MainSvcNo = Column(String(80))
    LLCPartnerReference = Column(String(250))
    GroupOwner = Column(String(80))
    PerformerID = Column(String(80))
    update_time = Column(DateTime(timezone=True),
                         server_default=func.now())

    def __repr__(self):
        return "<Sgo(Workorder='%s', update_time='%s')>" % (self.Workorder, self.update_time)

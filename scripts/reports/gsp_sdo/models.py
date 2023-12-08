"""SQLAlchemy Data Models."""
from sqlalchemy import func, Column, PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import String, Date, DateTime, BigInteger, SmallInteger

SdoBase = declarative_base()


class Sdo(SdoBase):
    __tablename__ = 't_GSP_ip_sdo'
    __table_args__ = (PrimaryKeyConstraint(
        'OrderCode', 'update_time'),)

    OrderCode = Column(String(20), nullable=False)
    ServiceNumber = Column(String(20))
    ProductCode = Column(String(20))
    CRD = Column(Date(), nullable=False)
    CustomerName = Column(String(80))
    OrderCreated = Column(Date(), nullable=False)
    OrderType = Column(String(20), nullable=False)
    OrderCodeNew = Column(String(20))
    CRDNew = Column(Date())
    ServiceNoNew = Column(String(80))
    StepNo_SS = Column(SmallInteger)
    ActName_SS = Column(String(50))
    GrpID_SS = Column(String(10))
    DueDate_SS = Column(Date())
    RDY_Date_SS = Column(Date())
    EXC_Date_SS = Column(Date())
    DLY_Date_SS = Column(Date())
    COM_Date_SS = Column(Date())
    StepNo_RI = Column(SmallInteger)
    ActName_RI = Column(String(50))
    GrpID_RI = Column(String(10))
    DueDate_RI = Column(Date())
    RDY_Date_RI = Column(Date())
    EXC_Date_RI = Column(Date())
    DLY_Date_RI = Column(Date())
    COM_Date_RI = Column(Date())
    StepNo_TI = Column(SmallInteger)
    ActName_TI = Column(String(50))
    GrpID_TI = Column(String(10))
    DueDate_TI = Column(Date())
    RDY_Date_TI = Column(Date())
    EXC_Date_TI = Column(Date())
    DLY_Date_TI = Column(Date())
    COM_Date_TI = Column(Date())
    ProjectManager = Column(String(80))
    SS_to_CRD = Column(BigInteger)
    RI_to_CRD = Column(BigInteger)
    TI_to_CRD = Column(BigInteger)
    report_id = Column(String(20))
    update_time = Column(DateTime(timezone=True),
                         server_default=func.now())

    def __repr__(self):
        return "<Sdo(OrderCode='%s', update_time='%s')>" % (self.OrderCode, self.update_time)

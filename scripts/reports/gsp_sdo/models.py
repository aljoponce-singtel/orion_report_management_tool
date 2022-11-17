"""SQLAlchemy Data Models."""
from sqlalchemy import func, Column, PrimaryKeyConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.types import String, SmallInteger, Date, DateTime

SdoBase = declarative_base()


class Sdo(SdoBase):
    __tablename__ = 't_GSP_ip_sdo_test'
    __table_args__ = (PrimaryKeyConstraint(
        'OrderCode', 'report_id', 'update_time'),)

    OrderCode = Column(String(20))
    ServiceNumber = Column(String(80))
    ProductCode = Column(String(20))
    CRD = Column(Date(), nullable=False)
    CustomerName = Column(String(80))
    OrderCreated = Column(Date(), nullable=False)
    OrderType = Column(String(20))
    OrderCodeNew = Column(String(20))
    CRDNew = Column(Date())
    ServiceNoNew = Column(String(80))
    StepNo_SS = Column(String(50))
    ActName_SS = Column(String(50))
    GrpID_SS = Column(String(10))
    DueDate_SS = Column(Date())
    RDY_Date_SS = Column(Date())
    EXC_Date_SS = Column(Date())
    DLY_Date_SS = Column(Date())
    COM_Date_SS = Column(Date())
    StepNo_RI = Column(String(50))
    ActName_RI = Column(String(50))
    GrpID_RI = Column(String(10))
    DueDate_RI = Column(Date())
    RDY_Date_RI = Column(Date())
    EXC_Date_RI = Column(Date())
    DLY_Date_RI = Column(Date())
    COM_Date_RI = Column(Date())
    StepNo_TI = Column(String(50))
    ActName_TI = Column(String(50))
    GrpID_TI = Column(String(10))
    DueDate_TI = Column(Date())
    RDY_Date_TI = Column(Date())
    EXC_Date_TI = Column(Date())
    DLY_Date_TI = Column(Date())
    COM_Date_TI = Column(Date())
    ProjectManager = Column(String(100))
    SS_to_CRD = Column(SmallInteger())
    RI_to_CRD = Column(SmallInteger())
    TI_to_CRD = Column(SmallInteger())
    report_id = Column(SmallInteger())
    update_time = Column(DateTime(timezone=True), server_default=func.now())

    def __repr__(self):
        return "<Transport(OrderCode='%s', update_time='%s')>" % (self.OrderCode, self.update_time)

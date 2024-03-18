from sqlalchemy import Column, PrimaryKeyConstraint, Integer, String
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


def create_project_tracker_test_group_class(group_name=None):
    class ProjectTrackerTestGroup(Base):

        if group_name:
            __tablename__ = f"project_tracker_test_{group_name}"
        else:
            __tablename__ = "project_tracker_test"
        __table_args__ = (PrimaryKeyConstraint('order_code'),)

        project_code = Column(String(20))
        project_tracker_site_id = Column(String(20))
        project_tracker_group_name = Column(String(20))
        circuit_code = Column(String(100))
        service_order_number = Column(String(20))
        product_category = Column(String(20))
        circuit_layer = Column(String(20))
        product_name = Column(String(40))
        type_of_product = Column(String(40))
        type_of_work = Column(String(40))
        order_code = Column(String(20), nullable=False)
        service_number = Column(String(80))
        service_type = Column(String(80))
        order_product_description = Column(String(80))
        npp_product_description = Column(String(80))
        project_id = Column(Integer)
        circuit_id = Column(Integer)
        order_id = Column(Integer)
        npp_id = Column(Integer)
        product_id = Column(Integer)

        def __repr__(self):
            return "<ProjectTrackerTestGroup(project_tracker_group_name='%s')>" % (self.project_tracker_group_name)

    return ProjectTrackerTestGroup

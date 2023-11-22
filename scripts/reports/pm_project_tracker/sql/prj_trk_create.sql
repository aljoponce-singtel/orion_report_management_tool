CREATE TABLE tmp_project_tracker (
    order_id int(11),
    work_order_no varchar(100),
    circuit_tie varchar(4000),
    billing_dependency_id varchar(300),
    INDEX order_id_index (order_id)
);
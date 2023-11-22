UPDATE
    tmp_project_tracker TRK
    JOIN RestInterface_order ORD ON ORD.order_code = TRK.work_order_no
SET
    TRK.order_id = ORD.id;
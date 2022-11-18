# Import built-in packages
import os
from datetime import datetime
import logging

# Import third-party packages
import numpy as np
import pandas as pd
from sqlalchemy import select, case, and_, or_, null, func

# Import local packages
import constants as const
from scripts.helpers import utils
from scripts.orion_report import OrionReport

logger = logging.getLogger(__name__)
configFile = os.path.join(os.path.dirname(__file__), 'config.ini')


def generate_report():

    report = OrionReport(configFile)

    query = ("""
                SELECT
                    CUS.name             AS 'CustomerName'
                    , BRN.brn              AS 'BRN'
                    , ORD.order_code       AS 'OrderCode'
                    , ORD.service_number   AS 'ServiceNumber'
                    , ORD.taken_date       AS 'OrderCreationDate'
                    , ORD.current_crd      AS 'CRD'
                    , CKT.circuit_code     AS 'CircuitCode'
                    , DATE(ORD.created_at) AS 'DateAddedToOrion'
                    FROM
                        RestInterface_order ORD
                        JOIN
                            RestInterface_customerbrnmapping BRN
                            ON
                                BRN.id = ORD.customer_brn_id
                        LEFT JOIN
                            RestInterface_customer CUS
                            ON
                                CUS.id = BRN.customer_id
                        LEFT JOIN
                            RestInterface_circuit CKT
                            ON
                                CKT.id = ORD.circuit_id
                    WHERE
                        BRN.brn IN ( '199405882Z'        , '197600379E', '201924026H', '198401908G'
                                , '0107-01-019678-0001', '199001413D', '8980140', 'S73FC2287H'
                                , '200104750M'         , '10384803110', '199701117H', '214-86-18758'
                                , '200208943K'         , '08980140', 'F   02287Z' )
                        AND DATE(ORD.created_at) BETWEEN DATE_SUB(DATE(NOW()), INTERVAL 1 DAY) AND DATE(NOW())
                    ORDER BY
                        'DateAddedToOrion'
                    , 'CustomerName'
                    , 'BRN'
                    , 'OrderCode'
                ;
            """)

    result = report.orion_db.query_to_list(query)

    if result:
        df = pd.DataFrame(data=result, columns=const.MAIN_COLUMNS)

        # set columns to datetime type
        df[const.DATE_COLUMNS] = df[const.DATE_COLUMNS].apply(
            pd.to_datetime)

        # Send email

        email_subject = 'UDF (Selected) Customers Report'

        # Change starting index from 0 to 1 for proper table presentation
        df.index += 1

        email_body_text = ("""
        Hello,

        Please see below the list of workorders added to Orion today and yesterday.

        {}

        Best regards,
        The Orion Team
                        """).format(str(df))

        email_body_html = ("""\
            <html>
            <p>Hello,</p>
            <p>Please see below the list of workorders added to Orion today and yesterday.</p>
            <p>{}</p>
            <p>&nbsp;</p>
            <p>Best regards,</p>
            <p>The Orion Team</p>
            </html>
            """).format(df.to_html())

        report.set_email_subject(report.add_timestamp(email_subject))
        report.set_email_body_text(email_body_text)
        report.set_email_body_html(email_body_html)
        report.send_email()

    else:
        logger.warn("NO NEW WORKORDERS ADDED TODAY")

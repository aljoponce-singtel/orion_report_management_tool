# Import built-in packages
import os
import logging

# Import third-party packages
import pandas as pd

# Import local packages
import constants as const
from scripts.orion_report import OrionReport

logger = logging.getLogger(__name__)
config_file = os.path.join(os.path.dirname(__file__), 'config.ini')


def generate_report():

    report = OrionReport(config_file)
    report.set_email_subject('GSP OLLC CRD Change Report', add_timestamp=True)
    report.set_filename('gsp_ollc_crd_change_report')
    report.set_reporting_date()

    query = f"""
                SELECT
                    DISTINCT ORD.arbor_disp,
                    ORD.current_crd AS crd_changed_to,
                    ORD.order_type,
                    ORD.order_status,
                    ORD.taken_date,
                    AAA.workorderno,
                    PRJ.project_code,
                    ORD.assignee,
                    BBB.last_crd AS crd_changed_from,
                    CCC.customerrequired AS today_crd,
                    CCC.updated_at AS crd_updated_in_orion
                FROM
                    (
                        SELECT
                            workorderno,
                            MAX(HIST.customerrequired) last_crd
                        FROM
                            o2pprod.Order_CRD_History HIST
                        WHERE
                            DATE_FORMAT(HIST.updated_at, '%y-%m-%d') < DATE_FORMAT('{report.report_date}', '%y-%m-%d')
                        GROUP BY
                            workorderno
                    ) BBB,
                    (
                        SELECT
                            workorderno,
                            COUNT(1)
                        FROM
                            o2pprod.Order_CRD_History
                        GROUP BY
                            workorderno
                        having
                            COUNT(1) > 1
                    ) AAA,
                    (
                        SELECT
                            workorderno,
                            customerrequired,
                            updated_at
                        FROM
                            o2pprod.Order_CRD_History HIST
                        WHERE
                            DATE_FORMAT(HIST.updated_at, '%y-%m-%d') = DATE_FORMAT('{report.report_date}', '%y-%m-%d')
                    ) CCC,
                    RestInterface_order ORD
                    LEFT JOIN RestInterface_project PRJ ON PRJ.id = ORD.project_id,
                    RestInterface_npp NPP,
                    RestInterface_product PROD
                WHERE
                    AAA.workorderno = BBB.workorderno
                    AND AAA.workorderno = CCC.workorderno
                    AND BBB.workorderno = CCC.workorderno
                    AND ORD.order_code = CCC.workorderno
                    AND ORD.id = NPP.order_id
                    AND NPP.product_id = PROD.id
                    AND ORD.order_type = 'Cease'
                    AND ORD.order_status NOT IN (
                        'Cancelled',
                        'Closed',
                        'Completed',
                        'Pending Cancellation'
                    )
                    AND PROD.product_type_id = 1082;
            """

    result = report.orion_db.query_to_list(query)

    if result:
        df_raw = pd.DataFrame(data=result, columns=const.RAW_COLUMNS)

        # set columns to datetime type
        df_raw[const.DATE_COLUMNS] = df_raw[const.DATE_COLUMNS].apply(
            pd.to_datetime)

        # set the final columns for the report
        df_main = df_raw[const.MAIN_COLUMNS]

        # Export df_main to CSV
        csv_file = report.create_csv_from_df(df_main)

        df_pm = df_raw[df_raw['assignee'] == 'PM']
        df_pm = df_pm[const.MAIN_COLUMNS]
        df_pm = df_pm.drop(['assignee'], axis=1)
        # Reset and change starting index from 0 to 1 for proper table presentation
        df_pm.reset_index(drop=True, inplace=True)
        df_pm.index += 1
        df_non_pm = df_raw[df_raw['assignee'] == 'Non-PM']
        df_non_pm = df_non_pm[const.MAIN_COLUMNS]
        df_non_pm = df_non_pm.drop(['project_code', 'assignee'], axis=1)
        # Reset and change starting index from 0 to 1 for proper table presentation
        df_non_pm.reset_index(drop=True, inplace=True)
        df_non_pm.index += 1

        email_body_text = f"""
        Hello,

        Please see below the list of OLLC workorders with their CRD changed today.

        {str(df_main)}

        Best regards,
        The Orion Team
                        """

        email_body_html = f"""\
            <html>
            <p>Hello,</p>
            <p>Please see below the list of OLLC workorders with their CRD changed today.</p>
            <p>Non-PM:</p>
            <p>{df_non_pm.to_html()}</p>
            <p>PM:</p>
            <p>{df_pm.to_html()}</p>
            <p>&nbsp;</p>
            <p>Best regards,</p>
            <p>The Orion Team</p>
            </html>
            """

        report.set_email_body_text(email_body_text)
        report.set_email_body_html(email_body_html)
        report.attach_file_to_email(csv_file)
        report.send_email()
    else:
        logger.info("No OLLC workorders where their CRD was changed today.")

    return

# Import built-in packages
import logging
from datetime import datetime
from typing import List

# Import third-party packages
import pandas as pd

# Import local packages
import constants as const
from scripts.helpers import utils
from scripts.orion_report import OrionReport

logger = logging.getLogger()


class GspReport(OrionReport):

    def __init__(self, config_file, report_name='Orion Report'):

        self.gsp_report_name = 'GSP'
        self.first_groupid_list = []
        self.second_groupid_list = []
        self.first_activity_list = []
        self.second_activity_list = []

        super().__init__(config_file, report_name)

    def set_gsp_report_name(self, name: str):
        self.gsp_report_name = name

    def set_first_groupid_list(self, groupid_list: List[str]):
        self.first_groupid_list = groupid_list

    def set_second_groupid_list(self, groupid_list: List[str]):
        self.second_groupid_list = groupid_list

    def set_first_activity_list(self, activities: List[str]):
        self.first_activity_list = activities

    def set_second_activity_list(self, activities: List[str]):
        self.second_activity_list = activities

    # Legacy/deprecated function
    def get_current_datetime(self):
        # sample format - '050622_1845'
        return datetime.now().strftime("%d%m%y_%H%M")

    # Legacy/deprecated function
    def add_timestamp(self, subject):
        # sample format - '[<SUBJECT>] 2023sep1 6pm'
        today_datetime = datetime.now()
        day = today_datetime.strftime('%d').lstrip('0')
        hour = today_datetime.strftime('%I').lstrip('0')
        ampm = today_datetime.strftime('%p').lower()
        year = today_datetime.strftime('%Y')
        month = today_datetime.strftime('%b').lower()
        subject = "[{}] {}{}{} {}{}".format(
            subject, year, month, day, hour, ampm)

        return subject

    def generate_report(self, main_group: str = 'first') -> pd.DataFrame:

        primary_groupid = self.first_groupid_list
        primary_activities = self.first_activity_list
        secondary_groupid = self.second_groupid_list
        secondary_activities = self.second_activity_list

        if main_group == 'second':
            primary_groupid = self.second_groupid_list
            primary_activities = self.second_activity_list
            secondary_groupid = self.first_groupid_list
            secondary_activities = self.first_activity_list

        query = f"""
                    SELECT
                        DISTINCT ORD.order_code,
                        ORD.service_number,
                        PRD.network_product_code,
                        PRD.network_product_desc,
                        CUS.name AS customer_name,
                        ORD.order_type,
                        ORD.current_crd,
                        ORD.taken_date,
                        PER.role AS group_id,
                        CAST(ACT.activity_code AS SIGNED INTEGER) AS act_step_no,
                        ACT.name AS act_name,
                        ACT.due_date AS act_due_date,
                        ACT.ready_date AS act_rdy_date,
                        DATE(ACT.exe_date) AS act_exe_date,
                        DATE(ACT.dly_date) AS act_dly_date,
                        ACT.completed_date AS act_com_date
                    FROM
                        RestInterface_order ORD
                        JOIN RestInterface_activity ACT ON ACT.order_id = ORD.id
                        JOIN RestInterface_person PER ON PER.id = ACT.person_id
                        LEFT JOIN RestInterface_customer CUS ON CUS.id = ORD.customer_id
                        LEFT JOIN RestInterface_npp NPP ON ORD.id = NPP.order_id
                        AND NPP.level = 'MainLine'
                        AND NPP.status != 'Cancel'
                        LEFT JOIN RestInterface_product PRD ON NPP.product_id = PRD.id
                    WHERE
                        ORD.id IN (
                            SELECT
                                DISTINCT ORD.id
                            FROM
                                RestInterface_order ORD
                                JOIN RestInterface_activity ACT ON ACT.order_id = ORD.id
                                JOIN RestInterface_person PER ON PER.id = ACT.person_id
                            WHERE
                                PER.role IN ({utils.list_to_string(primary_groupid)})
                                AND ACT.completed_date BETWEEN '{self.start_date}'
                                AND '{self.end_date}'
                                AND ACT.name IN (
                                    {utils.list_to_string(primary_activities)}
                                )
                        )
                        AND (
                            (
                                PER.role IN ({utils.list_to_string(primary_groupid)})
                                AND ACT.completed_date BETWEEN '{self.start_date}'
                                AND '{self.end_date}'
                                AND ACT.name IN (
                                    {utils.list_to_string(primary_activities)}
                                )
                            )
                            OR (
                                PER.role IN ({utils.list_to_string(secondary_groupid)})
                                AND ACT.name IN (
                                    {utils.list_to_string(secondary_activities)}
                                )
                            )
                        )
                    ORDER BY
                        ORD.order_code,
                        act_step_no;
                """

        result = self.orion_db.query_to_list(
            query, query_description=f"{self.gsp_report_name} records")

        df_raw = pd.DataFrame(data=result, columns=const.RAW_COLUMNS)
        # Convert columns to date
        for column in const.DATE_COLUMNS:
            df_raw[column] = pd.to_datetime(df_raw[column]).dt.date

        # New dataframe for the final report
        df_report = pd.DataFrame(columns=const.MAIN_COLUMNS_DOUBLE)
        # Get the list of unique workorders
        unique_orders = df_raw['Workorder no'].unique()

        # Iterate through the records of each uniqe workorders
        for order in unique_orders:
            # Initialize group 1 and 2 dataframe and dictionary
            df_top_group_1 = pd.DataFrame(columns=const.RAW_COLUMNS)
            df_top_group_2 = pd.DataFrame(columns=const.RAW_COLUMNS)
            group_1_dict = {
                col: None if col not in const.DATE_COLUMNS else pd.NaT for col in const.RAW_COLUMNS}
            group_2_dict = {
                col: None if col not in const.DATE_COLUMNS else pd.NaT for col in const.RAW_COLUMNS}
            # Get the records of an order
            df_order: pd.DataFrame
            df_order = df_raw[df_raw['Workorder no'] == order]
            # Get the records that matches group 1
            df_group_1 = df_order[df_order['Group ID'].isin(
                self.first_groupid_list) & df_order['Activity Name'].isin(self.first_activity_list)]
            # Get the records that matches group 2
            df_group_2 = df_order[df_order['Group ID'].isin(
                self.second_groupid_list) & df_order['Activity Name'].isin(self.second_activity_list)]
            # Check if group 1 is not empty
            if not df_group_1.empty:
                # Check if group 1 has more than 1 records
                if len(df_group_1) > 1:
                    # Get the top records based on priority and sequence
                    df_top_group_1 = self.__select_top_queue(
                        df_group_1, 'Activity Name', self.first_activity_list)
                else:
                    df_top_group_1 = df_group_1
            # Check if group 2 is not empty
            if not df_group_2.empty:
                # Check if group 2 has more than 1 records
                if len(df_group_2) > 1:
                    # Get the top records based on priority and sequence
                    df_top_group_2 = self.__select_top_queue(
                        df_group_2, 'Activity Name', self.second_activity_list)
                else:
                    df_top_group_2 = df_group_2

            # Convert the dataframe to a dictionary
            order_dict = df_order.head(1).to_dict(orient='records')[0]
            if not df_group_1.empty:
                group_1_dict = df_top_group_1.to_dict(orient='records')[0]
            if not df_group_2.empty:
                group_2_dict = df_top_group_2.to_dict(orient='records')[0]

            report_data = [
                order_dict['Workorder no'],
                order_dict['Service No'],
                order_dict['Product Code'],
                order_dict['Product Description'],
                order_dict['Customer Name'],
                order_dict['Order Type'],
                order_dict['CRD'],
                order_dict['Order Taken Date'],
                group_1_dict['Group ID'],
                group_1_dict['Activity Name'],
                group_1_dict['DUE'],
                # set RDY date to COM date if RDY date is empty
                group_1_dict['RDY'] if not pd.isna(
                    group_1_dict['RDY']) else group_1_dict['COM'],
                # set EXC date to COM date if EXC date is empty
                group_1_dict['EXC'] if not pd.isna(
                    group_1_dict['EXC']) else group_1_dict['COM'],
                group_1_dict['DLY'],
                group_1_dict['COM'],
                group_2_dict['Group ID'],
                group_2_dict['Activity Name'],
                group_2_dict['DUE'],
                # set RDY date to COM date if RDY date is empty
                group_2_dict['RDY'] if not pd.isna(
                    group_2_dict['RDY']) else group_2_dict['COM'],
                # set EXC date to COM date if EXC date is empty
                group_2_dict['EXC'] if not pd.isna(
                    group_2_dict['EXC']) else group_2_dict['COM'],
                group_2_dict['DLY'],
                group_2_dict['COM']
            ]

            df_to_add = pd.DataFrame(
                data=[report_data], columns=const.MAIN_COLUMNS_DOUBLE)
            df_report = pd.concat([df_report, df_to_add])

        return df_report

    # private method
    def __select_top_queue(self, df_group: pd.DataFrame, column_name, activity_list) -> pd.DataFrame:
        logger.warn(
            ("Workorder has MULTIPLE queues.\n{}")
            .format(df_group[['Workorder no', 'Group ID', 'Step No', 'Activity Name', 'COM']].to_string(index=False)))
        # Get the top records based on priority and sequence
        df_top_group = utils.sort_df_by_priority_sequence(
            df_group, column_name, activity_list)
        if len(df_top_group) > 1:
            logger.warn(
                "Getting the top records based on priority and sequence.")
            logger.warn(
                ("Workorder has DUPLICATE queues.\n{}")
                .format(df_top_group[['Workorder no', 'Group ID', 'Step No', 'Activity Name', 'COM']].to_string(index=False)))
            df_top_group = df_top_group.sort_values(
                by=['Step No'], ascending=[False]).drop_duplicates(subset=['Activity Name'], keep='first')
            logger.warn(
                ("Selecting the queue with the highest step no.\n{}")
                .format(df_top_group[['Workorder no', 'Group ID', 'Step No', 'Activity Name', 'COM']].to_string(index=False)))
        else:
            logger.warn(
                ("Getting the top records based on priority and sequence.\n{}")
                .format(df_top_group[['Workorder no', 'Group ID', 'Step No', 'Activity Name', 'COM']].to_string(index=False)))

        return df_top_group

# Import built-in packages
import os
import logging

# Import local packages
from scripts.helpers import utils
from gsp_report import GspReport

logger = logging.getLogger(__name__)
config_file = os.path.join(os.path.dirname(__file__), 'config.ini')


def generate_sdwan_report():

    report = GspReport(config_file, 'SDWAN Report')
    report.set_filename('sdwan_report')
    report.set_prev_month_first_last_day_date()

    gsp_group_list = [
        'GSP_SDN',
        'GSP_NFV'
    ]

    gsp_act_list = [
        # 'NFV Staging & Onboarding',
        # 'SDWAN Onsite Installation',
        # 'Cease SD-WAN Service'
    ]

    gsdt_group_list = [
        'GSDT_SDN',
        'GSDT_NFV'
    ]

    gsdt_act_list = [
        # 'GSDT Co-ordination Work'
    ]

    report.set_first_groupid_list(gsp_group_list)
    report.set_first_activity_list(gsp_act_list)
    report.set_second_groupid_list(gsdt_group_list)
    report.set_second_activity_list(gsdt_act_list)
    current_datetime = utils.get_current_datetime()
    zip_filename = ("{}_{}.zip").format(report.filename, current_datetime)

    # CNP
    report.set_gsp_report_name("gsp")
    df_report = report.generate_report_two_group(
        main_group='first', only_group_id=True)
    csv_file = report.create_csv_from_df(df_report, filename=(
        "{}_{}.csv").format(report.gsp_report_name, current_datetime))
    zip_file = report.add_to_zip_file(
        csv_file, zip_filename=zip_filename, add_timestamp=True)

    # GSDT6
    report.set_gsp_report_name("gsdt")
    df_report = report.generate_report_two_group(
        main_group='second', only_group_id=True)
    csv_file = report.create_csv_from_df(df_report, filename=(
        "{}_{}.csv").format(report.gsp_report_name, current_datetime))
    zip_file = report.add_to_zip_file(
        csv_file, zip_filename=zip_filename, add_timestamp=True)

    # Send email
    report.attach_file_to_email(zip_file)
    report.add_email_receiver_to('xv.hema.pawar@singtel.com')
    report.send_email()


def generate_cplus_ip_report():

    report = GspReport(config_file, 'CPlusIP Report')
    report.set_filename('cplusip_report')
    report.set_prev_month_first_last_day_date()

    cnp_group_list = [
        'CNP',
        'CNP1',
        'CNP10',
        'CNP11',
        'CNP12',
        'CNP13',
        'CNP2',
        'CNP20',
        'CNP3',
        'CNP30',
        'CNP31',
        'CNP32',
        'CNP33',
        'CNP34',
        'CNP35',
        'CNP36',
        'CNP37',
        'CNP38',
        'CNP39',
        'CNP4',
        'CNP40',
        'CNP41',
        'CNP42',
        'CNP43',
        'CNP44',
        'CNP45',
        'CNP5',
        'CNP6',
        'CNP7',
        'CNP8',
        'CNP9'
    ]

    cnp_act_list = [
        'LLC Accepted by Singtel',
        'Change C+ IP',
        'De-Activate C+ IP',
        'DeActivate Video Exch Svc',
        'LLC Received from Partner',
        'Activate C+ IP',
        'Cease Resale SGO',
        'OLLC Site Survey',
        'De-Activate TOPSAA on PE',
        'De-Activate RAS',
        'De-Activate RLAN on PE',
        'Pre-Configuration on PE',
        'De-Activate RMS on PE',
        'GSDT Co-ordination Work',
        'Change Resale SGO',
        'Pre-Configuration',
        'Cease MSS VPN',
        'Recovery - PNOC Work',
        'De-Activate RMS for IP/EV',
        'GSDT Co-ordination OS LLC',
        'Change RAS',
        'Extranet Config',
        'Cease Resale SGO JP',
        'm2m EVC Provisioning',
        'Activate RMS/TOPS IP/EV',
        'Config MSS VPN',
        'De-Activate RMS on CE-BQ',
        'OLLC Order Ack',
        'Cease Resale SGO CHN'
    ]

    gsdt6_group_list = [
        'GSDT6',
        'GSDT61',
        'GSDT610',
        'GSDT611',
        'GSDT612',
        'GSDT613',
        'GSDT62',
        'GSDT620',
        'GSDT63',
        'GSDT630',
        'GSDT631',
        'GSDT632',
        'GSDT633',
        'GSDT634',
        'GSDT635',
        'GSDT636',
        'GSDT637',
        'GSDT638',
        'GSDT639',
        'GSDT64',
        'GSDT640',
        'GSDT641',
        'GSDT642',
        'GSDT643',
        'GSDT644',
        'GSDT645',
        'GSDT65',
        'GSDT66',
        'GSDT67',
        'GSDT68',
        'GSDT69'
    ]

    gsdt6_act_list = [
        'GSDT Co-ordination OS LLC',
        'GSDT Co-ordination Work',
        'De-Activate C+ IP',
        'Cease Monitoring of IPPBX',
        'GSDT Partner Cloud Access',
        'Cease In-Ctry Data Pro',
        'Change Resale SGO',
        'Ch/Modify In-Country Data',
        'De-Activate RMS on PE',
        'Disconnect RMS for FR',
        'Change C+ IP',
        'Activate C+ IP',
        'LLC Accepted by Singtel',
        'GSDT Co-ordination - BQ',
        'LLC Received from Partner',
        'In-Country Data Product',
        'OLLC Site Survey',
        'GSDT Co-ordination-RMS',
        'Pre-Configuration on PE',
        'Cease Resale SGO',
        'Disconnect RMS for ATM'
    ]

    report.set_first_groupid_list(cnp_group_list)
    report.set_first_activity_list(cnp_act_list)
    report.set_second_groupid_list(gsdt6_group_list)
    report.set_second_activity_list(gsdt6_act_list)
    current_datetime = report.get_current_datetime()
    zip_filename = ("{}_{}.zip").format(report.filename, current_datetime)

    # CNP
    report.set_gsp_report_name("CNP")
    df_report = report.generate_report_two_group(main_group='first')
    csv_file = report.create_csv_from_df(df_report, filename=(
        "{}_{}.csv").format(report.gsp_report_name, current_datetime))
    zip_file = report.add_to_zip_file(csv_file, zip_filename=zip_filename)

    # GSDT6
    report.set_gsp_report_name("GSDT6")
    df_report = report.generate_report_two_group(main_group='second')
    csv_file = report.create_csv_from_df(df_report, filename=(
        "{}_{}.csv").format(report.gsp_report_name, current_datetime))
    zip_file = report.add_to_zip_file(csv_file, zip_filename=zip_filename)

    # Send email
    report.attach_file_to_email(zip_file)
    report.send_email()

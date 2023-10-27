# Import built-in packages
import logging
import os

# Import local packages
from scripts.helpers import utils
from scripts.reports.gsp.helpers import GspReport

logger = logging.getLogger(__name__)

# Add the config file path
config = os.path.join(os.path.dirname(__file__), "config.ini")


def generate_sdwan_new_report():

    report = GspReport('SDWAN Report', config)
    report.set_filename('sdwan_report')
    report.set_gsp_billing_month_start_end_date()

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

    report = GspReport('CPlusIP Report', config)
    report.set_filename('cplusip_report')
    report.set_gsp_billing_month_start_end_date()

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


def generate_megapop_report():

    report = GspReport('MegaPop Report', config)
    report.set_filename('megapop_report')
    report.set_gsp_billing_month_start_end_date()

    mpp_group_list = [
        'MPP',
        'MPP1',
        'MPP10',
        'MPP11',
        'MPP12',
        'MPP13',
        'MPP2',
        'MPP3',
        'MPP4',
        'MPP5',
        'MPP6',
        'MPP7',
        'MPP70',
        'MPP71',
        'MPP72',
        'MPP73',
        'MPP74',
        'MPP75',
        'MPP76',
        'MPP77',
        'MPP78',
        'MPP79',
        'MPP8',
        'MPP80',
        'MPP81',
        'MPP82',
        'MPP83',
        'MPP84',
        'MPP85',
        'MPP885',
        'MPP9'
    ]

    mpp_act_list = [
        'Circuit Configuration',
        'Activate E-Access',
        'Activate EVPL',
        'Activate RMS/TOPS - MP',
        'Cease IaaS Connectivity',
        'Cessation of PE Port',
        'Cessation of UTM',
        'Change C+ IP',
        'Config PE Port',
        'Config UTM',
        'De-Activate E-Access',
        'De-Activate RMS on PE',
        'End to End PNOC Test',
        'Extranet Config',
        'GSDT Co-ordination Work',
        'm2m EVC Provisioning',
        'mLink EVC Provisioning',
        'mLink EVC Termination',
        'Pre-Configuration on PE',
        'Re-config E-Access',
        'Reconfiguration',
        'Recovery - PNOC Work',
        'SD-WAN Config',
        'SD-WAN Svc Provisioning'
    ]

    gsdt8_group_list = [
        'GSDT8',
        'GSDT81',
        'GSDT810',
        'GSDT811',
        'GSDT812',
        'GSDT813',
        'GSDT82',
        'GSDT820',
        'GSDT83',
        'GSDT84',
        'GSDT85',
        'GSDT86',
        'GSDT87',
        'GSDT870',
        'GSDT871',
        'GSDT872',
        'GSDT873',
        'GSDT874',
        'GSDT875',
        'GSDT876',
        'GSDT877',
        'GSDT878',
        'GSDT879',
        'GSDT88',
        'GSDT880',
        'GSDT881',
        'GSDT882',
        'GSDT883',
        'GSDT884',
        'GSDT885',
        'GSDT89'
    ]

    gsdt8_act_list = [
        'GSDT Co-ordination OS LLC',
        'GSDT Co-ordination Work',
        'Cease IaaS Connectivity',
        'Connection of RMS for MP',
        'GSDT Co-ordination Wk-BQ',
        'GSDT Co-ordination Wrk-BQ'
    ]

    report.set_first_groupid_list(mpp_group_list)
    report.set_first_activity_list(mpp_act_list)
    report.set_second_groupid_list(gsdt8_group_list)
    report.set_second_activity_list(gsdt8_act_list)
    current_datetime = report.get_current_datetime()
    zip_filename = ("{}_{}.zip").format(report.filename, current_datetime)

    # CNP
    report.set_gsp_report_name("MPP")
    df_report = report.generate_report_two_group(main_group='first')
    csv_file = report.create_csv_from_df(df_report, filename=(
        "{}_{}.csv").format(report.gsp_report_name, current_datetime))
    zip_file = report.add_to_zip_file(csv_file, zip_filename=zip_filename)

    # GSDT8
    report.set_gsp_report_name("GSDT8")
    df_report = report.generate_report_two_group(main_group='second')
    csv_file = report.create_csv_from_df(df_report, filename=(
        "{}_{}.csv").format(report.gsp_report_name, current_datetime))
    zip_file = report.add_to_zip_file(csv_file, zip_filename=zip_filename)

    # Send email
    report.attach_file_to_email(zip_file)
    report.send_email()


def generate_singnet_report():

    report = GspReport('Singnet Report', config)
    report.set_filename('singnet_report')
    report.set_gsp_billing_month_start_end_date()

    sgx_group_list = [
        'SGX1',
        'SGX11',
        'SGX110',
        'SGX111',
        'SGX112',
        'SGX12',
        'SGX13',
        'SGX14',
        'SGX15',
        'SGX16',
        'SGX17',
        'SGX18',
        'SGX19',
        'SGX3',
        'SGX37',
        'SGX38'
    ]

    sgx_act_list = [
        'Cease SG Cct @PubNet ',
        'Cease SingNet Svc',
        'Provision SingNet Svc',
        'Modify SingNet Svc',
        'Circuit Configuration',
        'Activate E-Access',
        'Comn SgNet PubNet Wk',
        'Comn SingNet PubNet Wk',
        'De-Activate E-Access',
        'DNS Set-Up',
        'GSDT Co-ordination Work',
        'IP Verification - MegaPOP',
        'Modify Microsoft Direct',
        'Reconfiguration',
        'Recovery - PNOC Work',
        'SN Evolve Static IP Work'
    ]

    gsdt7_group_list = [
        'GSDT7',
        'GSDT71',
        'GSDT710',
        'GSDT711',
        'GSDT712',
        'GSDT72',
        'GSDT73',
        'GSDT74',
        'GSDT75',
        'GSDT76',
        'GSDT77',
        'GSDT78',
        'GSDT79'
    ]

    gsdt7_act_list = [
        'Circuit Configuration',
        'GSDT Coordination',
        'GSDT Co-ordination WK-BQ',
        'GSDT Co-ordination Work',
        'GSDT Co-ordination Wrk-BQ'
    ]

    report.set_first_groupid_list(sgx_group_list)
    report.set_first_activity_list(sgx_act_list)
    report.set_second_groupid_list(gsdt7_group_list)
    report.set_second_activity_list(gsdt7_act_list)
    current_datetime = report.get_current_datetime()
    zip_filename = ("{}_{}.zip").format(report.filename, current_datetime)

    # CNP
    report.set_gsp_report_name("SGX1")
    df_report = report.generate_report_two_group(main_group='first')
    csv_file = report.create_csv_from_df(df_report, filename=(
        "{}_{}.csv").format(report.gsp_report_name, current_datetime))
    zip_file = report.add_to_zip_file(csv_file, zip_filename=zip_filename)

    # GSDT8
    report.set_gsp_report_name("GSDT7")
    df_report = report.generate_report_two_group(main_group='second')
    csv_file = report.create_csv_from_df(df_report, filename=(
        "{}_{}.csv").format(report.gsp_report_name, current_datetime))
    zip_file = report.add_to_zip_file(csv_file, zip_filename=zip_filename)

    # Send email
    report.attach_file_to_email(zip_file)
    report.send_email()


def generate_internet_report():

    report = GspReport('Internet Report', config)
    report.set_filename('internet_report')
    report.set_gsp_billing_month_start_end_date()

    internet_group_list = [
        'GSDT_PS21',
        'GSDT_PS22',
        'GSDT_PS23',
        'GSDT_PS24'
    ]

    internet_act_list = [
        'GSDT Co-ordination Work',
        'GSDT GI Coordination Work'
    ]

    report.set_first_groupid_list(internet_group_list)
    report.set_first_activity_list(internet_act_list)
    current_datetime = report.get_current_datetime()
    zip_filename = ("{}_{}.zip").format(report.filename, current_datetime)

    # CNP
    report.set_gsp_report_name("GSDT_PS21_GSDT_PS23")
    df_report = report.generate_report_one_group()
    csv_file = report.create_csv_from_df(df_report, filename=(
        "{}_{}.csv").format(report.gsp_report_name, current_datetime))
    zip_file = report.add_to_zip_file(csv_file, zip_filename=zip_filename)

    # Send email
    report.attach_file_to_email(zip_file)
    report.send_email()


def generate_stix_report():

    report = GspReport('STIX Report', config)
    report.set_filename('stix_report')
    report.set_gsp_billing_month_start_end_date()

    stix_group_list = [
        'GSDT9',
        'GSDT91',
        'GSDT92',
        'GSDT93',
        'GSDT94',
        'GSDT95',
        'GSDT96'
    ]

    stix_act_list = [
        'GSDT Co-ordination OS LLC',
        'GSDT Co-ordination Work'
    ]

    report.set_first_groupid_list(stix_group_list)
    report.set_first_activity_list(stix_act_list)
    current_datetime = report.get_current_datetime()
    zip_filename = ("{}_{}.zip").format(report.filename, current_datetime)

    # CNP
    report.set_gsp_report_name("GSDT9")
    df_report = report.generate_report_one_group()
    csv_file = report.create_csv_from_df(df_report, filename=(
        "{}_{}.csv").format(report.gsp_report_name, current_datetime))
    zip_file = report.add_to_zip_file(csv_file, zip_filename=zip_filename)

    # Send email
    report.attach_file_to_email(zip_file)
    report.send_email()


def generate_sdwan_report():

    report = GspReport('SDWAN Report', config)
    report.set_filename('sdwan_report')
    report.set_gsp_billing_month_start_end_date()

    sdwan_group_list = [
        'GSP_SDN_TM',
        'GSDT_TM'
    ]

    sdwan_act_list = [
        'GSDT Co-ordination Wk-BQ',
        'GSDT Co-ordination Wrk-BQ',
        'GSP-TM Coordination Work'
    ]

    report.set_first_groupid_list(sdwan_group_list)
    report.set_first_activity_list(sdwan_act_list)
    current_datetime = report.get_current_datetime()
    zip_filename = ("{}_{}.zip").format(report.filename, current_datetime)

    # CNP
    report.set_gsp_report_name("GSP_SDN_TM_GSDT_TM")
    df_report = report.generate_report_one_group()
    csv_file = report.create_csv_from_df(df_report, filename=(
        "{}_{}.csv").format(report.gsp_report_name, current_datetime))
    zip_file = report.add_to_zip_file(csv_file, zip_filename=zip_filename)

    # Send email
    report.attach_file_to_email(zip_file)
    report.send_email()

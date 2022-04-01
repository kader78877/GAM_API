# Import libraries
from googleads import ad_manager, errors
from datetime import date, timedelta
import tempfile
import pandas as pd
from google.cloud import storage
from io import StringIO, BytesIO
import os


def authentificate_ad_manager(path_yaml):
    """
    This function enables the connection to Ad Manager API
    :param path_yaml : path to googleads.yaml configuration file for authentification
    :return: AdManagerClient object
    """
    ad_manager_client = ad_manager.AdManagerClient.LoadFromStorage(path=path_yaml)
    return ad_manager_client


def get_report_data(ad_manager_client, report_date):
    """
    This function extractq data from Ad Manager reports
    :param ad_manager_client: AdManagerClient object related to Ad Manager account
    :param report_date: date of observation
    :return: reporting file containing impression, revenue and request data by ad unit and date
    """
    # Initialize a DataDownloader.
    report_downloader = ad_manager_client.GetDataDownloader()

    report_job = {
        'reportQuery': {
            'dimensions': ['DATE', 'AD_UNIT_ID', 'AD_UNIT_NAME'],
            'adUnitView': 'FLAT',
            'columns': ['TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS',
                        'TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE',
                        'TOTAL_AD_REQUESTS'],
            'dateRangeType': 'CUSTOM_DATE',
            'startDate': report_date,
            'endDate': report_date
        }
    }

    try:
        report_job_id = report_downloader.WaitForReport(report_job)
    except errors.AdManagerReportError as e:
        print('Failed to generate report. Error was: %s' % e)

    with tempfile.NamedTemporaryFile(suffix='.csv.gz', mode='wb', delete=False) as report_file:
        report_downloader.DownloadReportToFile(report_job_id, 'CSV_DUMP', report_file)
    return report_file


def transform_report_data(report_file):
    """
    This function processes data from report file to pandas dataframe
    :param report_file: temporary file containing reporting data
    :return: reporting pandas dataframe
    """

    df_report = pd.read_csv(report_file.name)
    regex_pattern = r'([a-zA-Z_.0-9]+)\s\([0-9]+\)'

    # Transformation
    df_report['AD_UNIT_ALL_LEVEL'] = df_report['Dimension.AD_UNIT_NAME'].str.split('»')
    df_report['ad_unit_level_0'] = df_report['AD_UNIT_ALL_LEVEL'].str[0].str.extract(regex_pattern)
    df_report['section'] = df_report['AD_UNIT_ALL_LEVEL'].str[1].str.extract(regex_pattern)
    df_report['sub_section'] = df_report['AD_UNIT_ALL_LEVEL'].str[2].str.extract(regex_pattern)
    df_report['ad_unit_name'] = df_report['AD_UNIT_ALL_LEVEL'].str[-1].str.extract(regex_pattern)
    df_report['device_type'] = df_report['ad_unit_level_0']
    df_report['date'] = pd.to_datetime(df_report['Dimension.DATE'], format='%Y-%m-%d')

    # Formatting
    df_report['revenue'] = df_report['Column.TOTAL_LINE_ITEM_LEVEL_ALL_REVENUE'].astype(float)
    df_report['device_type'].replace({"20minutes_web": "Desktop",
                                      "20minutes_web_video_P2": "Desktop",
                                      "20minutes_mobile": "Mobile Web"}, inplace=True)

    # Renaming
    df_report = df_report.rename(columns={"Column.TOTAL_LINE_ITEM_LEVEL_IMPRESSIONS": "impressions",
                                          "Column.TOTAL_AD_REQUESTS": 'total_ad_requests',
                                          "AD_UNIT_ALL_LEVEL": "ad_unit_all_level"})

    # Filtering
    # Filter 1 : Keep web and mobile adunit revenue :
    filter_1 = (df_report['ad_unit_level_0'] == '20minutes_web') | \
               (df_report['ad_unit_level_0'] == '20minutes_web_video_P2') | \
               (df_report['ad_unit_level_0'] == '20minutes_mobile')

    # Filter 2 : Keep section=amp or sub_section=Diapo or sub_section=art
    filter_2 = (df_report['sub_section'].str.endswith('_Diapo')) | \
               (df_report['sub_section'].str.endswith('_art')) | \
               (df_report['section'] == 'amp')

    df_report = df_report[filter_1 & filter_2]
    columns_to_display = ['date', 'device_type', 'section', 'sub_section',
                          'impressions', 'revenue', 'total_ad_requests']
    df_report = df_report[columns_to_display]
    df_report = df_report.groupby(['date', 'device_type', 'section', 'sub_section']).sum().reset_index()

    return df_report


def send_to_gs(df_report, name_file):
    """
    This function sends the dataframe in csv format to google cloud storage
    :param df_report: report dataframe to send to google cloud storage
    :param name_file: name of the file
    """
    f = StringIO()
    df_report.to_csv(f, index=False)
    f.seek(0)
    f = BytesIO(f.getvalue().encode())
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'src/key.json'
    gcs = storage.Client()

    gcs.bucket('solar.20mn.net').blob(
        'internal/applications/solar/gam/'+name_file).upload_from_file(f, content_type='text/csv')


def data_processing(param):
    """
    This function helps to extract, transform and load the data to google cloud storage
    :param param: a list of a parameters allowing to create, process and send the data to google cloud storage
    """
    date_ = param[-1]
    report_file = get_report_data(*param)
    df_report = transform_report_data(report_file)

    # send to gs
    name_file = 'solar_20mn_ads.gam_revenue_page_' + date_.strftime('%Y-%m-%d' + '.csv')
    send_to_gs(df_report, name_file)


if __name__ == '__main__':

    # data ingestion option
    historical = False

    # authentification
    path_yaml = "src/googleads.yaml"
    client = authentificate_ad_manager(path_yaml)
    print("Authentification : Done")
    if not historical:
        # daily extraction
        date_ = date.today() - timedelta(1)
        param = [client, date_]
        print("Extraction")
        data_processing(param)
        print(date_)

    else:
        # historical extraction
        from_date = date(2021, 7, 20)
        to_date = date.today() - timedelta(1)
        n = (to_date - from_date).days + 1
        print("Extraction")
        for i in range(n):

            # iterate through date
            date_ = from_date + timedelta(i)

            # generate data and send to gs
            param = [client, date_]
            data_processing(param)
            print(date_)



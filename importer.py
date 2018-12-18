# Modules
import pysftp
import argparse
import os
import csv
import sys
import calendar
import logging
import glob
import zipfile
import time
import datetime
import json

from influxdb import InfluxDBClient

# Functions

def get_unix_ts(str_dt):
    arr_date = str(str_dt[0]).split("-")
    arr_time = str(str_dt[1]).split(":")
    dt = datetime.datetime(int(arr_date[0]), int(arr_date[1]), int(arr_date[2]),
                           int(arr_time[0]), int(arr_time[1]), int(float(arr_time[2])))
    return calendar.timegm(dt.timetuple())

def append_send_data(dps, point, max_len, wait, logger):
    dps.append(point)

    if len(dps) >= int(max_len):
        logger.info('Insert %i data into InfluxDB' % len(dps))
        influx_client.write_points(dps, time_precision='s')

        dps = []
        time.sleep(wait)
    return dps

def insert_data_generation(localpath, cfg, influx_client, logger):
    influxdb_data_points = []

    logger.info('Read data from %s' % localpath)
    with open(localpath, 'r', encoding='utf-16') as f:
        reader = csv.reader((line.replace('\0', '') for line in f))

        # skip the headers
        next(reader, None)

        for row in reader:
            if len(row) > 0:
                tmp = row[0].split('\t')

                # Replace the '/' in the Production type tags (e.g. Fossil Brown coal/Lignite)
                if '/' in tmp[9]:
                    tmp[9] = tmp[9].replace('/', '-')

                if is_float(tmp[10]) is True:
                    # create the UNIX timestamp
                    ts = get_unix_ts(str_dt=str(tmp[3]).split(" "))

                    # create the data point
                    point = {
                                'time': ts,
                                'measurement': cfg['influxdb']['measurementGeneration'],
                                'fields': dict(value=float(tmp[10].strip(' '))),
                                'tags': dict(map_code=tmp[8].strip(' '),
                                             map_code_desc=tmp[7].strip(' '),
                                             type=tmp[9].strip(' '),
                                             area_code=tmp[5].strip(' '),
                                             area_type_code=tmp[6].strip(' '))
                            }
                    # append and (eventually) send data
                    influxdb_data_points = append_send_data(dps=influxdb_data_points, point=point,
                                                            max_len=cfg['influxdb']['maxLinesPerInsert'],
                                                            wait=cfg['influxdb']['waitAfterInsert'],
                                                            logger=logger)

        # send remaining data
        logger.info('Insert %i data into InfluxDB' % len(influxdb_data_points))
        influx_client.write_points(influxdb_data_points, time_precision='s')

def insert_data_flow(localpath, cfg, influx_client, logger):
    influxdb_data_points = []

    logger.info('Read data from %s' % localpath)
    with open(localpath, 'r', encoding='utf-16') as f:
        reader = csv.reader((line.replace('\0', '') for line in f))

        # skip the headers
        next(reader, None)

        for row in reader:
            if len(row) > 0:
                tmp = row[0].split('\t')

                if is_float(tmp[13]) is True:
                    # create the UNIX timestamp
                    ts = get_unix_ts(str_dt=str(tmp[3]).split(" "))

                    # create the data point
                    point = {
                                'time': ts,
                                'measurement': cfg['influxdb']['measurementCrossBorderFlow'],
                                'fields': dict(value=float(tmp[13].strip(' '))),
                                'tags': dict(out_map_code=tmp[8].strip(' '),
                                             out_map_code_desc=tmp[7].strip(' '),
                                             out_area_code=tmp[5].strip(' '),
                                             out_area_type_code=tmp[6].strip(' '),
                                             in_map_code=tmp[12].strip(' '),
                                             in_map_code_desc=tmp[11].strip(' '),
                                             in_area_code=tmp[9].strip(' '),
                                             in_area_type_code=tmp[10].strip(' '))
                              }
                    # append and (eventually) send data
                    influxdb_data_points = append_send_data(dps=influxdb_data_points, point=point,
                                                            max_len=cfg['influxdb']['maxLinesPerInsert'],
                                                            wait=cfg['influxdb']['waitAfterInsert'],
                                                            logger=logger)

        # send remaining data
        logger.info('Insert %i data into InfluxDB' % len(influxdb_data_points))
        influx_client.write_points(influxdb_data_points, time_precision='s')


# Download zip and store locally
def download_zip(case, cfg, influx_client, logger):

    # open the SFTP connection to ENTSO-E server
    srv = pysftp.Connection(host=cfg['entsoe']['host'], port=cfg['entsoe']['port'],
                            username=cfg['entsoe']['user'], password=cfg['entsoe']['password'])

    # change directory
    if case == 'generation':
        srv.cwd(cfg['entsoe']['remotePathGeneration'])
    elif case == 'cross_border_flow':
        srv.cwd(cfg['entsoe']['remotePathCrossBorderFlow'])

    # list files in directory
    filelist = srv.listdir()

    # get current and last year and month in format "YYYY_MM_"
    current = datetime.date.today()

    first = current.replace(day=1)
    last_month = first - datetime.timedelta(days=1)

    if sys.platform == 'win32':
        current_month = current.strftime('%Y_%#m_')
        last_year = last_month.strftime('%Y_%#m_')
    else:
        current_month = current.strftime('%Y_%-m_')
        last_year = last_month.strftime('%Y_%-m_')

    # define files to download
    files_to_get = [s for s in filelist if current_month in s]
    files_to_get += [s for s in filelist if last_year in s]

    # download zipped file
    for zip_file in files_to_get:
        # define the file to download
        local_path = '%s/%s' % (srv.getcwd(), zip_file)
        logger.info("Downloading file ==> %s" % local_path)
        local_file = '%s/%s' % (cfg['entsoe']['localPath'], zip_file)

        # download file
        srv.get(zip_file, local_file)

        # unzip the archive
        with zipfile.ZipFile(local_file, 'r') as zip_ref:
            zip_ref.extractall(cfg['entsoe']['localPath'])
        zip_ref.close()

        input_data_file = glob.glob('%s/*.csv' % cfg['entsoe']['localPath'])[0]

        # handle generation data
        if case == 'generation':
            insert_data_generation(localpath=input_data_file, cfg=cfg, influx_client=influx_client, logger=logger)

        # handle cross border flow data
        elif case == 'cross_border_flow':
            insert_data_flow(localpath=input_data_file, cfg=cfg, influx_client=influx_client, logger=logger)

        else:
            logger.info('Please give the correct path file')

        # delete files
        logger.info('Delete file %s' % input_data_file)
        os.unlink(input_data_file)
        logger.info('Delete file %s' % local_file)
        os.unlink(local_file)

    # close the SFTP connection
    srv.close()

def is_float(string):
    try:
        float(string)
        return True
    except ValueError:
        return False


# main
if __name__ == '__main__':

    # get input arguments
    arg_parser = argparse.ArgumentParser()
    arg_parser.add_argument('-c', help='configuration file')
    arg_parser.add_argument('-l', help='log file')
    args = arg_parser.parse_args()

    # set configuration dictionary
    cfg = json.loads(open(args.c).read())

    # set logging object
    if not args.l:
        log_file = None
    else:
        log_file = args.l

    # define logger
    logger = logging.getLogger()
    logging.basicConfig(format='%(asctime)-15s::%(levelname)s::%(funcName)s::%(message)s', level=logging.INFO,
                        filename=log_file)

    # starting program
    logging.info('Starting program')

    # open an InfluxDB client
    try:
        influx_client = InfluxDBClient(host=cfg['influxdb']['host'],
                                       port=cfg['influxdb']['port'],
                                       password=cfg['influxdb']['password'],
                                       username=cfg['influxdb']['user'],
                                       database=cfg['influxdb']['database'])
    except Exception as e:
        logger.error('EXCEPTION: %s' % str(e))
        sys.exit(3)
    logger.info('Connection successful')

    # get generation data
    download_zip(case='generation', cfg=cfg, influx_client=influx_client, logger=logger)

    # get cross-border flow data
    download_zip(case='cross_border_flow', cfg=cfg, influx_client=influx_client, logger=logger)

    # exit program
    logging.info('Exit program')

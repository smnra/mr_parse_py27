import os
import sys
import shutil
import time
import datetime
import numpy as np
import pandas as pd
import xml.etree.cElementTree as ET
from Logger import mylog
from OperationFile import Operation
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

sys.path.append('../config')
from config import Task, Path, Backup

# config
gz_path = Path.gz_path
xml_path = Path.xml_path
mdt_path = Path.mdt_path
bk_path = Path.bk_path
log_path = Path.log_path
gz_error = Path.gz_error
xml_error = Path.xml_error
bk_gz = Backup.bk_gz
city = Task.city
batch_grade = Task.batch_grade

with open('../config/sdate.txt') as file:
    sdatelist = file.read().splitlines()

sdatename = '-'.join(sdatelist)

logger = mylog('../dir_log', 'parse.log')


def parse_xml(file):
    basename = os.path.basename(file)
    (filename, extension) = os.path.splitext(basename)
    if extension == '.xml':
        try:
            logger.info('提取MDT：{}'.format(filename))
            tree = ET.parse(file)
            root = tree.getroot()
            enb = root.find("eNB")
            enbid = enb.get("id")
            measurement = enb.find("measurement")
            smr = measurement.find("smr")
            columns = smr.text.replace(',', '').split()
            mr_object = measurement.findall("object")
            # 全网图层 & 基站级图层
            if batch_grade != 2:
                listmdtall = []
                for group in mr_object:
                    eci = group.get("id")
                    timestamp = group.get("TimeStamp")
                    timestamp1 = datetime.datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f")
                    timestamp2 = timestamp1.strftime('%Y/%m/%d %H:%M:%S')
                    v = group.findall("v")
                    mr = v[0].text.split()
                    rsrp_s = (int(mr[0]) - 140 if mr[0].isdigit() else 'NIL')
                    rsrq_s = ((int(mr[2]) - 40) / 2 if mr[2].isdigit() else 'NIL')
                    if mr[38] != 'NIL':
                        lon = mr[38]
                        lat = mr[39]
                        listmdt = [city, timestamp2, enbid, eci, rsrp_s, rsrq_s, lon, lat]
                        listmdtall.append(listmdt)
                    elif 'MR.LteScSinrUL' not in columns and mr[37] != 'NIL':
                        lon = mr[37]
                        lat = mr[38]
                        listmdt = [city, timestamp2, enbid, eci, rsrp_s, rsrq_s, lon, lat]
                        listmdtall.append(listmdt)
                mdt = pd.DataFrame(listmdtall)
                mdt = mdt.replace('NIL', np.nan)
                mdt = mdt.dropna()
                # 全网图层
                if batch_grade == 0:
                    mdt.to_csv(mdt_path + city + '-MDT-' + str(sdatename) + '.csv', index=False, header=False, mode='a')
                # 基站级图层
                elif batch_grade == 1:
                    mdt.to_csv(mdt_path + city + '-{}'.format(enbid) + '-MDT-' + str(sdatename) + '.csv', index=False,
                               header=False, mode='a')
            # 小区级图层
            elif batch_grade == 2:
                listmdtall = []
                for group in mr_object:
                    eci = group.get("id")
                    timestamp = group.get("TimeStamp")
                    timestamp1 = datetime.datetime.strptime(timestamp, "%Y-%m-%dT%H:%M:%S.%f")
                    timestamp2 = timestamp1.strftime('%Y/%m/%d %H:%M:%S')
                    v = group.findall("v")
                    mr = v[0].text.split()
                    rsrp_s = (int(mr[0]) - 140 if mr[0].isdigit() else 'NIL')
                    rsrq_s = ((int(mr[2]) - 40) / 2 if mr[2].isdigit() else 'NIL')
                    if mr[38] != 'NIL':
                        lon = mr[38]
                        lat = mr[39]
                        listmdt = [city, timestamp2, enbid, eci, rsrp_s, rsrq_s, lon, lat]
                        listmdtall = [listmdt]
                    elif 'MR.LteScSinrUL' not in columns and mr[37] != 'NIL':
                        lon = mr[37]
                        lat = mr[38]
                        listmdt = [city, timestamp2, enbid, eci, rsrp_s, rsrq_s, lon, lat]
                        listmdtall = [listmdt]
                    mdt = pd.DataFrame(listmdtall)
                    mdt = mdt.replace('NIL', np.nan)
                    mdt = mdt.dropna()
                    mdt.to_csv(
                        mdt_path + city + '-{}'.format(enbid) + '-{}'.format(eci) + '-MDT-' + str(sdatename) + '.csv',
                        index=False, header=False, mode='a')

            os.remove(file)

        except Exception as error:
            logger.error(error)
            shutil.move(file, xml_error)


if __name__ == '__main__':
    xmllist = Operation().findfile(xml_path, '.xml')
    if xmllist:

        maxworkers = Task.ThreadPool
        p = ThreadPoolExecutor(max_workers=maxworkers)
        p.map(parse_xml, xmllist)
        p.shutdown()

    else:
        info = '{} 路径下没有文件！'.format(xml_path)
        logger.info(info)
        time.sleep(10)

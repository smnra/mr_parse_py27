# coding = utf-8
# -*- coding: utf-8 -*-

import os
import shutil
import gzip
import time,arrow
import datetime
import numpy as np
import pandas as pd
import xml.etree.cElementTree as ET



# 城市
city = 'XIAN'

# 是否批量生成单站/小区图层,0为否(即生成全网图层),1为单站图层(即每个站都生成1张图层),2为单小区图层(即每个小区都生成1张图层)
batch_grade = 0

# 时间
sdatename = arrow.now().shift(days=-1).format("YYYYMMDD")



def gz_xml(dataDir):
    zipDir = os.path.join(os.path.abspath('./'),'mr_data\\zip')
    xmlDir = os.path.join(os.path.abspath('./'),'mr_data\\xml')
    csvDir = mdt_path = os.path.join(os.path.abspath('./'),'mr_data\\csv')


    zipList = [[tmp,None] for tmp in os.listdir(zipDir)]
    xmlList = [[tmp,[]] for tmp in os.listdir(zipDir)]

    for i,site in enumerate(zipList):
        zipList[i][1] = [zipDir + '\\' + site[0] + '\\' +  filePath for filePath in os.listdir(zipDir + '\\' + zipList[i][0])]
    print(zipList)


    def unzipGzFile(gzFilePath,xmlFilePath):
        # 解压缩gz文件到 文件夹,第一个参数是gz文件名, 第二个参数是 保存 xml文件的文件夹路径

        # 创建文件夹
        if not os.path.exists(os.path.split(xmlFilePath)[0]):
            os.makedirs(os.path.split(xmlFilePath)[0])

        try:
            with gzip.open(gzFilePath, 'rb') as f_in:
                with open(xmlFilePath, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
                    print('解压缩：{}'.format(gzFilePath))
                    return xmlFilePath
        except Exception as error:
            print(str(gzFilePath) + ' ' + str(error))
            return None




    def parse_xml(file):
        basename = os.path.basename(file)
        (filename, extension) = os.path.splitext(basename)
        if extension == '.xml':
            try:
                print('提取MDT：{}'.format(filename))
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
                        ci = mr[4].replace('4600000','')

                        lon = ''
                        lat = ''

                        if len(mr) > 37 and mr[38] != 'NIL':
                            lon = mr[38]
                            lat = mr[39]
                        elif len(mr) > 37 and 'MR.LteScSinrUL' not in columns and mr[37] != 'NIL':
                            lon = mr[37]
                            lat = mr[38]

                        listmdt = [city, timestamp2, enbid, eci, rsrp_s, rsrq_s, lon, lat, ci]
                        listmdtall.append(listmdt)
                    mdt = pd.DataFrame(listmdtall)
                    mdt = mdt.replace('NIL', np.nan)
                    mdt = mdt.dropna()
                    # 全网图层
                    if batch_grade == 0:
                        mdt.to_csv(mdt_path + city + '-MDT-' + str(sdatename) + '.csv', index=False, header=False,
                                   mode='a')
                    # 基站级图层
                    elif batch_grade == 1:
                        mdt.to_csv(mdt_path + city + '-{}'.format(enbid) + '-MDT-' + str(sdatename) + '.csv',
                                   index=False,
                                   header=False, mode='a')

                os.remove(file)

            except Exception as error:
                print(error)




    for s,site in enumerate(zipList):
        for i, gzFilePath in enumerate(site[1]):
            gzFileName = os.path.split(gzFilePath)[1]
            xmlFileName = os.path.splitext(gzFileName)[0]
            xmlFilePath = os.path.join(xmlDir, site[0], xmlFileName)

            xmlList[s][1].append(unzipGzFile(gzFilePath, xmlFilePath))


    for site in xmlList:
        for xml in site[1]:
            parse_xml(xml)

if __name__ == '__main__':
    gz_xml('./mr_data/zip')
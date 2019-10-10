# coding = utf-8
# -*- coding: utf-8 -*-
# python2.7.5


import os,commands
import shutil
import gzip
import datetime
import xml.etree.cElementTree as ET

# startTime = datetime.datetime.now()


# 获取MR服务器IP地址
(_, serverName) = commands.getstatusoutput("ifconfig em1 | grep 10.100 |awk '{print $2}'")

# 时间
sdatename = (datetime.datetime.now()+datetime.timedelta(days= -1)).strftime('%Y%m%d')


# 初始化目录
baseDir = '/tmp/mr_data/'
zipDir = '/tmp/mr_data/zip/'
xmlDir = '/tmp/mr_data/xml/'
csvDir = '/tmp/mr_data/csv/'

if not os.path.exists(zipDir): os.makedirs(zipDir)
if not os.path.exists(xmlDir): os.makedirs(xmlDir)
if not os.path.exists(csvDir): os.makedirs(csvDir)

# 输出CSV文件路径
csvFileName = csvDir + '/' + serverName + '-MDT-' + str(sdatename) + '.csv'
if os.path.exists('/tmp/'): os.chdir('/tmp/')  # 更改当前目录


def copyMrData(sdatename):
    # 参数为日期的字符串,表示取某一日的MRO数据
    # mr 数据保存的路径
    mrDataPath = '/home/richuser/l3fw_mr/kpi_import/{}/'.format(sdatename)
    fileCount = 0
    for site in  os.listdir(mrDataPath):
        # mr 数据来源目录
        sourceSiteDir = mrDataPath + site + '/'

        # mr 要保存的目录 new
        newSiteDir = zipDir + site + '/'

        #  创建site 文件夹
        if not os.path.exists(newSiteDir):
            os.makedirs(newSiteDir)

        for gzFileName in os.listdir(sourceSiteDir):
            if '_MRO_' in gzFileName:
                shutil.copyfile(sourceSiteDir + gzFileName, newSiteDir + gzFileName)
                fileCount += 1
    return {'siteCount':len(os.listdir(mrDataPath)), 'fileCount': fileCount, 'gzFileDir':zipDir}



# 是否批量生成单站/小区图层,0为否(即生成全网图层),1为单站图层(即每个站都生成1张图层),2为单小区图层(即每个小区都生成1张图层)
batch_grade = 0


def gz_xml(dataDir):
    zipList = [[tmp,None] for tmp in os.listdir(zipDir)]
    xmlList = [[tmp,[]] for tmp in os.listdir(zipDir)]

    for i,site in enumerate(zipList):
        zipList[i][1] = [zipDir + '/' + site[0] + '/' +  filePath for filePath in os.listdir(zipDir + '/' + zipList[i][0])]
    # print zipList


    def unzipGzFile(gzFilePath,xmlFilePath):
        # 解压缩gz文件到 文件夹,第一个参数是gz文件名, 第二个参数是 保存 xml文件的文件夹路径

        # 创建文件夹
        if not os.path.exists(os.path.split(xmlFilePath)[0]):
            os.makedirs(os.path.split(xmlFilePath)[0])

        try:
            with gzip.open(gzFilePath, 'rb') as f_in:
                with open(xmlFilePath, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
                    print '解压缩：{}'.format(gzFilePath.replace(zipDir,''))
                    return xmlFilePath
        except Exception as error:
            print str(gzFilePath) + ' ' + str(error)
            return None




    def parse_xml(file):
        basename = os.path.basename(file)
        (filename, extension) = os.path.splitext(basename)
        if extension == '.xml':
            try:
                print '提取MDT：{}'.format(filename.decode(encoding='gbk'))
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
                        rsrp_s = str((int(mr[0]) - 140 if mr[0].isdigit() else 'NIL'))
                        rsrq_s = str((int(mr[2]) - 40) / 2 if mr[2].isdigit() else 'NIL')
                        ci = mr[28].replace('4600000','')

                        lon = ''
                        lat = ''

                        if len(mr) > 37 and mr[38] != 'NIL':
                            lon = mr[38]
                            lat = mr[39]
                        elif len(mr) > 37 and 'MR.LteScSinrUL' not in columns and mr[37] != 'NIL':
                            lon = mr[37]
                            lat = mr[38]

                        listmdt = [serverName, timestamp2, enbid, eci, rsrp_s, rsrq_s, lon, lat, ci+'\n']
                        listmdtall.append(listmdt)

                    with open(csvFileName, mode='a+') as f:  # 将采集进度写入文件
                        lines = [','.join(lineStr) for lineStr in listmdtall]
                        f.writelines(lines)

            except Exception, error:
                print error



    for s,site in enumerate(zipList):
        for i, gzFilePath in enumerate(site[1]):
            gzFileName = os.path.split(gzFilePath)[1]
            xmlFileName = os.path.splitext(gzFileName)[0]
            xmlFilePath = os.path.join(xmlDir, site[0], xmlFileName)

            xmlList[s][1].append(unzipGzFile(gzFilePath, xmlFilePath))
            print str(s) + '\n'


    for sIndex ,site in enumerate(xmlList):
        for xml in site[1]:
            parse_xml(xml)
        print str(sIndex) + '\n'


if __name__ == '__main__':
    startTime = datetime.datetime.now()
    gzFile = copyMrData(sdatename)
    print gzFile
    gz_xml(gzFile['gzFileDir'])
    
    if os.system('rm -r /tmp/mr_data/zip/*')==0:
        print '删除压缩成功'
    if os.system('rm -r /tmp/mr_data/xml/*')==0:
        print '删除xml成功'
    if os.path.exists(csvFileName):
        os.chdir(csvDir)
        if os.system('gzip -9 -c csvFileName')==0:
            print '压缩CSV文件成功:{}'.format(csvFileName)
    endTime = datetime.datetime.now()      
    print gzFile
    
    con = endTime - startTime
    
    print "花费时间: {}秒".format(con.seconds)
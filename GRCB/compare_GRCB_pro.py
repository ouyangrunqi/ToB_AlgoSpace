# -*- coding: utf-8 -*-
import csv
import heapq
import operator
import os
import re
import time

import pymysql as pymysql
import requests
import json
import xlrd


class Comparedata:
    def __init__(self):
        self.iuid_mapping_filepath = r'D:\GRCB\13\iuid_mapping_GRCB.xlsx'
        self.model_info_filepath = r'D:\GRCB\13\model_info.csv'
        self.model_weight_filepath = r'D:\GRCB\13\model_weight.csv'
        self.model_distribution_filepath = r'D:\GRCB\13\model_distribution.csv'
        self.model_projections_filepath = r'D:\GRCB\13\model_projections.csv'
        self.model_backtesting_filepath = r'D:\GRCB\13\model_backtesting.csv'
        self.model_benchmark_filepath = r'D:\GRCB\13\benchmark.csv'

        # self.iuid_mapping_filepath = r'/Users/hangchong/PycharmProjects/autotests/13/iuid_mapping_GRCB.xlsx'
        # self.model_info_filepath = r'/Users/hangchong/PycharmProjects/autotests/13/model_info.csv'
        # self.model_weight_filepath = r'/Users/hangchong/PycharmProjects/autotests/13/model_weight.csv'
        # self.model_distribution_filepath = r'/Users/hangchong/PycharmProjects/autotests/13/model_distribution.csv'
        # self.model_projections_filepath = r'/Users/hangchong/PycharmProjects/autotests/13/model_projections.csv'
        # self.model_backtesting_filepath = r'/Users/hangchong/PycharmProjects/autotests/13/model_backtesting.csv'
        # self.model_benchmark_filepath = r'/Users/hangchong/PycharmProjects/autotests/13/benchmark.csv'

        self.algo_type_id = '17'
        self.algo_type_id_raas = '228'
        self.model_info_version = '13'
        # projections,每5个一行
        self.splice_length = 5

        self.iuid_mapping_url = f'https://algo-internal.aqumon.com/algo-space/v3/algo-space/algo_info_mapping?algo_type_id={self.algo_type_id}'
        # 通过algo_type查typeid
        self.algo_type_url = f'https://algo-internal.aqumon.com/algo-space/v3/algo-space/algo_type_version/list?algo_type_id={self.algo_type_id}&requires_active=true'
        # self.model_weight_url = 'https://algo-internal.aqumon.com/algo-space/v3/algo-space/algo_control/5286/weights?extends_result=false'
        # self.model_distribution_url = 'https://algo-internal.aqumon.com/algo-space/v3/algo-space/algo_control/5286/distributions'
        # self.model_projection_url = 'https://algo-internal.aqumon.com/algo-space/v3/algo-space/algo_control/5286/projections'
        # self.model_backtesting_url = 'https://algo-internal.aqumon.com/algo-space/v3/algo-space/algo_control/5286/backtestings?start_date=2021-04-01'
        self.headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36'
        }
        self.use_iuid_mapping = ['iuid', 'productCode', 'nameSimplified', 'fundType', 'divType']

    def req_typeid(self):
        '''
        get type id
        :return:
        '''
        res = requests.get(self.algo_type_url, headers=self.headers)
        # json.loads,读取字符串并转为python对象
        res_json = json.loads(res.text)
        return res_json['data']['id']

    def write_control_ids(self):
        type_id = self.req_typeid()
        model_info_url = f'https://algo-internal.aqumon.com/algo-space/v3/algo-space/algo_control/list?algo_type_version_id={type_id}'
        res = requests.get(model_info_url, headers=self.headers)
        res_json = json.loads(res.text)
        for data_dics in res_json['data']:
            for k, v in data_dics.items():
                if k == 'id':
                    with open('control_ids.txt', 'a+', encoding='utf-8')as f:
                        f.write(f'{v}\n')

    def get_time(self):
        times = time.strftime("%Y%m%d%H%MS", time.localtime())
        return times

    def get_control_ids(self):
        cons = []
        with open('control_ids.txt', 'r', encoding='utf-8')as f:
            for data in f.readlines():
                # 文件里是读出来是5293\n，replace(A, B)表示将A替换成B
                cons.append(data.replace('\n', ''))
        return cons

    def req_weight(self):
        cmis = self.get_control_model_id()
        ddd_out = []
        for cm in cmis:
            print(f'now in weight request {cm} ----->')
            cm = cm.split('==')
            control_id = cm[0]
            model_id = cm[1]
            model_weight_url = f'https://algo-internal.aqumon.com/algo-space/v3/algo-space/algo_control/{control_id}/weights?extends_result=false'
            res = requests.get(model_weight_url, headers=self.headers)
            res_json = json.loads(res.text)
            # print(res_json['data'])
            for data_dics in res_json['data']:
                ddd = []
                for k, v in data_dics.items():
                    if k == 'iuid':
                        ddd.append(model_id)
                        ddd.append(v)
                    if k == 'weight':
                        # aa = str(v).split('.')
                        # aa = '.'.join(aa)
                        aa = str(v).split('.')
                        bb = aa[1]
                        cc = ''
                        if len(bb) == 1:
                            cc = f'{str(v)}00000'
                        if len(bb) == 2:
                            cc = f'{str(v)}0000'
                        if len(bb) == 3:
                            cc = f'{str(v)}000'
                        if len(bb) == 4:
                            cc = f'{str(v)}00'
                        if len(bb) == 5:
                            cc = f'{str(v)}0'
                        ddd.append(cc)
                ddd.sort()
                ddd_out.append(ddd)
        return ddd_out

    def req_projections(self):
        cmis = self.get_control_model_id()
        ddd_out = []
        for cm in cmis:
            print(f'now in weight request {cm} ----->')
            cm = cm.split('==')
            control_id = cm[0]
            model_id = cm[1]
            model_projections_url = f'https://algo-internal.aqumon.com/algo-space/v3/algo-space/algo_control/{control_id}/projections'
            res = requests.get(model_projections_url, headers=self.headers)
            res_json = json.loads(res.text)
            # print(res_json['data'])
            oldlist = res_json['data']
            z = len(oldlist)
            # z % self.splice_length = 0，除得尽就是True，除不尽的最后放一个列表里
            if not z % self.splice_length:
                x = int(z / self.splice_length)
            else:
                x = int(z / self.splice_length) + 1
            ne_list = []
            nee_list = []
            j = 0
            for m, i in enumerate(oldlist):
                if m == x:
                    break
                if m == 0:
                    # ne_list = oldlist[m:self.splice_length * (m + 1)]
                    ne_list = oldlist[m:self.splice_length]
                    nee_list.append(ne_list)
                else:
                    m = self.splice_length * (j + 1)
                    ne_list = oldlist[m:m + self.splice_length]
                    nee_list.append(ne_list)
                    j += 1
            # print(nee_list)
            for nn in nee_list:
                ddd = []
                #     print(nn)
                #     print(len(nn))
                # time.sleep(3000)
                ii = 0
                for data_dics in nn:
                    for k, v in data_dics.items():
                        #每5个一组，ii==0~4，时间都是一样，ii==5,相当于第二个id的ii==0(实际不能到5，限制返回5个)
                        if ii == 1:
                            if k == 'projectionDate':
                                v = v.split('T')[0]
                                ddd.append(v)
                                ddd.append(model_id)
                        if k == 'projectionValue':
                            aa = str(v).split('.')
                            bb = aa[1]
                            cc = ''
                            if len(bb) == 1:
                                cc = f'{str(v)}00000'
                            if len(bb) == 2:
                                cc = f'{str(v)}0000'
                            if len(bb) == 3:
                                cc = f'{str(v)}000'
                            if len(bb) == 4:
                                cc = f'{str(v)}00'
                            if len(bb) == 5:
                                cc = f'{str(v)}0'
                            ddd.append(cc)
                        ii += 1
                ddd.sort()
                ddd_out.append(ddd)
        return ddd_out

    def req_data(self, req_url):
        '''
        get iuid mapping response data
        :param req_url:
        :return:
        '''
        req_data_list = []
        res = requests.get(req_url, headers=self.headers)
        res_json = json.loads(res.text)
        for da in res_json['data']:
            data_list = []
            for k, v in da.items():
                if k in self.use_iuid_mapping:
                    if v == 'REINVESTMENT':
                        v = '0'
                    if v == 'CASH':
                        v = '1'
                    if v == 'INDEX':
                        v = '0'
                    if v == 'STOCK':
                        v = '1'
                    if v == 'BOND':
                        v = '2'
                    if v == 'MIXED':
                        v = '3'
                    if v == 'CURRENCY':
                        v = '4'
                    if v == 'ALTERNATIVE':
                        v = '5'
                    data_list.append(v)
                    data_list.sort()
            req_data_list.append(data_list)
        return req_data_list

    def req_modelinfo(self):
        '''
        请求 model_info 接口获取数据
        :return:
        '''
        modelinfo_out_list = []
        type_id = self.req_typeid()
        model_info_url = f'https://algo-internal.aqumon.com/algo-space/v3/algo-space/algo_control/list?algo_type_version_id={type_id}'
        res = requests.get(model_info_url, headers=self.headers)
        res_json = json.loads(res.text)
        for data_dics in res_json['data']:
            modelinfo_list = []
            for k, v in data_dics.items():
                if k == 'id':
                    # self.idlist.append(v)
                    model_id = self.getdata_fromdb(v)
                    modelinfo_list.append(model_id)
                    # modelinfo_list.append(self.algo_type_id)
                    modelinfo_list.append(self.algo_type_id_raas)
                    modelinfo_list.append(self.model_info_version)
                if k == 'sector':
                    modelinfo_list.append(v)
                if k == 'region':
                    modelinfo_list.append(v)
                if k == 'riskRatio':
                    modelinfo_list.append(str(v).split('.')[0])
                if k == 'fundType':
                    # print(v)
                    v = json.loads(v)
                    vlist = v['2020-10-31']
                    # print(vlist)
                    # print('=======================')
                    new_vlist = []
                    j = 0
                    for kk in vlist:
                        new_vdic = {}
                        # print(type(kk))
                            #{'FundSectorRate':8.18,'FundSectorName':'原材料'}
                        kkk = kk.keys()#{FundSectorRate,FundSectorName}
                        kkk = list(kkk)#[FundSectorRate,FundSectorName]
                        if j <= 10:
                            kkk.reverse()#[FundSectorName,FundSectorRate]
                            new_vdic[kkk[0]] = kk[kkk[0]]#new_vdic[FundSectorName] = 原材料
                            new_vdic[kkk[1]] = kk[kkk[1]]#new_vdic[FundSectorRate] = 8.18
                            # print(new_vdic)
                            new_vlist.append(new_vdic)#['FundSectorName':'原材料','FundSectorRate':8.18]
                        else:
                            new_vlist.append(kk)
                        j += 1

                        '''
                        https://blog.csdn.net/weixin_30786657/article/details/94790120
                        三.包含字典dict的列表list的排序方法与dict的排序类似, 如下：

                        方法一：使用python模块：operator

                        import operator

                        x = [{'name': 'Homer', 'age': 39}, {'name': 'Bart', 'age': 10}]

                        sorted_x = sorted(x, key=operator.itemgetter('name'))
                        print(sorted_x)  # [{'age': 10, 'name': 'Bart'}, {'age': 39, 'name': 'Homer'}]

                        sorted_x = sorted(x, key=operator.itemgetter('name'), reverse=True)
                        print(sorted_x)  # [{'age': 39, 'name': 'Homer'}, {'age': 10, 'name': 'Bart'}]
                        方法二：使用lambda表达式

                        x = [{'name': 'Homer', 'age': 39}, {'name': 'Bart', 'age': 10}]

                        sorted_x = sorted(x, key=lambda x: x['name'])
                        print（sorted_x）  # [{'age': 10, 'name': 'Bart'}, {'age': 39, 'name': 'Homer'}]

                        sorted_x = sorted(x, key=lambda x: x['name'], reverse=True)
                        print(sorted_x)  # [{'age': 39, 'name': 'Homer'}, {'age': 10, 'name': 'Bart'}]
                        
                        n1接口返回，n2表格返回
                        # n1 = [{'FundSectorRate' : 1, 'FundSectorName' : 2} , {'FundBondRate' : 1, 'FundBondSector' : 2} , {'FundTypeRate' : 1,'FundTypeName' : 2}]
                        # n2 = [{'FundSectorRate' : 1, 'FundSectorName' : 2} , {'FundBondSector' : 2, 'FundBondRate' : 1} , {'FundTypeName' : 2, 'FundTypeRate': 1}]
                    '''

                    modelinfo_list.append(str(new_vlist).replace(': ', ':').replace(' \'', '\'').replace(' {', '{'))
                    modelinfo_list.sort()
            # modelinfo_list = sorted(modelinfo_list,key=operator.itemgetter('FundBondSector'),reverse=True)
            # modelinfo_list = sorted(modelinfo_list,key=lambda modelinfo_list : modelinfo_list['name'],reverse=True)
            modelinfo_out_list.append(modelinfo_list)
        # print(f'model_info 接口共返回{len(modelinfo_out_list)}条数据------>')
        # print(modelinfo_list)
        # time.sleep(2222)
        return modelinfo_out_list

    def req_benchmark(self):
        '''
        从数据库获取history_model_id
        :param native_mode_id:
        :return:
        '''
        db = pymysql.connect(host="rm-6nn035o35cidvrnme.mysql.rds.aliyuncs.com", user="raas",
                             password="79i5VVSgTEkEMBtQ", db="raas_dev", port=3306)
        # 通过获取到的数据库连接cursor下的cursor()方法来创建游标
        cursor = db.cursor()
        table = 'algo_type'
        # 通过游标cur 操作execute()方法可以写入纯sql语句
        cursor.execute(f'SELECT * FROM {table} WHERE id = {self.algo_type_id}')
        selectResultList = cursor.fetchall()
        iuiddd = ''
        iuiddd2 = ''
        for i in range(len(selectResultList)):
            if len(selectResultList[i]):
                if selectResultList[0]:
                    iuiddd = selectResultList[0][8]
                    iuiddd2 = selectResultList[0][9]
                else:
                    return 'error!!! ===> select db no data'
        pricelistt = []
        cursor.execute(f"select * FROM market_info where iuid= '{iuiddd}'")
        selectResultList = cursor.fetchall()
        # print(len(selectResultList))
        for i in range(len(selectResultList)):
            pricelist = []
            if len(selectResultList[i]):
                a = selectResultList[i][7]
                pricelist.append(str(selectResultList[i][8]))
                pricelist.append(str(a))
                pricelistt.append(pricelist)
            else:
                return 'error!!! ===> select db no data'
        print(pricelistt)
        price2listt = []
        cursor.execute(f"select * FROM market_info where iuid= '{iuiddd2}'")
        selectResultList = cursor.fetchall()
        print('================================')
        # print(len(selectResultList))
        for i in range(len(selectResultList)):
            price2list = []
            if len(selectResultList[i]):
                price2list.append(str(selectResultList[i][7]))
                price2listt.append(price2list)
            else:
                return 'error!!! ===> select db no data'
        print(price2listt)
        aaa = []
        for j in range(len(price2listt)):
            aaa.append(pricelistt[j] + price2listt[j])
        return aaa

    def getdata_fromdb(self, native_mode_id):
        '''
        从数据库获取history_model_id
        :param native_mode_id:
        :return:
        '''
        print(f'now search from database ---->native model id: {native_mode_id}')
        db = pymysql.connect(host="rm-6nn035o35cidvrnme.mysql.rds.aliyuncs.com", user="raas",
                             password="79i5VVSgTEkEMBtQ", db="raas_dev", port=3306)
        cursor = db.cursor()
        table = 'algo_model'
        cursor.execute(f'SELECT id,history_model_id FROM {table} WHERE native_model_id = {native_mode_id}')

        # 5.遍历结果，获取查询的结果
        selectResultList = cursor.fetchall()
        for i in range(len(selectResultList)):
            if len(selectResultList[i]):
                if selectResultList[0][1]:
                    return selectResultList[0][1]
                else:
                    return selectResultList[0][0]

    def read_xlsx(self):
        workbook = xlrd.open_workbook(self.iuid_mapping_filepath)
        Data_sheet = workbook.sheets()[0]  # 通过索引获取工作表
        rowNum = Data_sheet.nrows  # sheet行数
        colNum = Data_sheet.ncols  # sheet列数
        xlsx_data_dic = {}
        for i in range(1, rowNum):#去表头，range+1
            xlsx_data_list = []
            for j in range(colNum):
                xlsx_data_list.append(Data_sheet.cell_value(i, j))
            new_list = list(filter(None, xlsx_data_list))#去掉空数据，如Alternative
            trans_new_list = new_list
            # print(len(trans_new_list))
            trans_new_list1 = trans_new_list[:8]
            trans_new_list2 = trans_new_list[9:]
            trans_new_list1.sort()
            trans_new_list2.sort()
            xlsx_data_dic[f'第{i}行第一条数据不一样'] = trans_new_list1
            # xlsx_data_dic[f'第{i}行第二条数据不一样'] = trans_new_list2
        # print(f'对应表格共{len(xlsx_data_dic) * 8}条数据')
        # print(f'new xlsx data: {xlsx_data_dic}')
        return xlsx_data_dic

    def read_csv(self, filename):
        weight_csv_dic = {}
        with open(filename, 'r', encoding='UTF-8') as f:
            reader = csv.reader(f)
            i = 0
            for row in reader:#此时输出的是一行行的列表
                if i == 0:
                    pass
                else:
                    row.sort()
                    weight_csv_dic[f'第{i}行数据不一样'] = row
                i += 1
        return weight_csv_dic

    def read_benchmark_csv(self, filename):
        weight_csv_dic = {}
        with open(filename, 'r') as f:
            reader = csv.reader(f)
            i = 1
            for row in reader:
                row = row[:3]
                # print(row)
                weight_csv_dic[f'第{i}行数据不一样'] = row
                i += 1
        return weight_csv_dic

    def read_modelinfo_csv(self):
        modelinfo_csv_dic = {}
        with open(self.model_info_filepath, 'r', encoding='UTF-8') as f:
            reader = csv.reader(f)
            i = 0
            for row in reader:
                # file = []
                if i == 0:
                    pass
                else:
                    row = row[0:6] + row[7:]
                    row.sort()
                    # print(row)
                    # file.append(row)
                    modelinfo_csv_dic[f'第{i}行数据不一样'] = row
                i += 1
            return modelinfo_csv_dic

    # def trans_five(self, data_list):
    #     new_data_list = []
    #     for da in data_list:
    #         if re.findall('^\d{4}$', da):
    #             da = f'0{da}'
    #             new_data_list.append(da)
    #         else:
    #             new_data_list.append(da)
    #     return new_data_list

    def req_backtesting(self):
        cmis = self.get_control_model_id()
        i = 0
        for cm in cmis:
            print(f'now in weight request {cm} ----->')
            ddd_out = []
            cm = cm.split('==')
            control_id = cm[0]
            model_id = cm[1]
            model_backtesting_url = f'https://algo-internal.aqumon.com/algo-space/v3/algo-space/algo_control/{control_id}/backtestings'
            res = requests.get(model_backtesting_url, headers=self.headers)
            res_json = json.loads(res.text)
            # print(res_json['data'])
            oldlist = res_json['data']
            i += len(oldlist)
        return i

    def write_cpmpare_data(self, dirpath_name, cons, times):
        '''
        写入比较后的数据结果
        :param dirpath_name:
        :param cons:
        :param times:
        :return:
        '''
        pwd = os.getcwd()
        dirpath = os.path.join(pwd, dirpath_name)
        isExists = os.path.exists(dirpath)
        if not isExists:
            with open(f'{dirpath_name}{times}.txt', 'a+', encoding='utf-8')as f:
                f.write(f'{cons}\n')
        else:
            with open(f'{dirpath_name}{times}.txt', 'a+', encoding='utf-8')as f:
                f.write(f'{cons}\n')

    def write_control_model_id(self):
        '''
        写入control_id 和对应的 history_model_id
        :return:
        '''
        data_list = []
        cids = self.get_control_ids()
        for cid in cids:
            print(f'now id {cid}')
            model_id = self.getdata_fromdb(cid)
            print('====================')
            data_list.append(f'{cid}=={str(model_id)}\n')

        with open('control_model_ids.txt', 'a+', encoding='utf-8')as f:
            for dl in data_list:
                f.write(dl)

    def get_control_model_id(self):
        '''
        获取 control===model id
        :return:
        '''
        cmi = []
        with open('control_model_ids.txt', 'r', encoding='utf-8')as f:
            for da in f.readlines():
                cmi.append(da.replace('\n', ''))
        return cmi

    def req_distribution(self):
        cmis = self.get_control_model_id()
        ddd_out = []
        for cm in cmis:
            print(f'now in distribution request {cm} ----->')
            cm = cm.split('==')
            control_id = cm[0]
            model_id = cm[1]
            model_distrubution_url = f'https://algo-internal.aqumon.com/algo-space/v3/algo-space/algo_control/{control_id}/distributions'
            res = requests.get(model_distrubution_url, headers=self.headers)
            res_json = json.loads(res.text)
            for k, v in res_json['data'].items():
                if k == '300':
                    # print(v)
                    # time.sleep(3333)
                    for kk, vv in v.items():
                        ddd = []
                        ddd.append('300')
                        ddd.append(kk)
                        # aa = str(vv).split('.')
                        # aa = '.'.join(aa)
                        aa = str(vv).split('.')
                        bb = aa[1]
                        cc = ''
                        if len(bb) == 1:
                            cc = f'{str(vv)}00000'
                        if len(bb) == 2:
                            cc = f'{str(vv)}0000'
                        if len(bb) == 3:
                            cc = f'{str(vv)}000'
                        if len(bb) == 4:
                            cc = f'{str(vv)}00'
                        if len(bb) == 5:
                            cc = f'{str(vv)}0'
                        ddd.append(cc)
                        ddd.append(model_id)
                        ddd.sort()
                        ddd_out.append(ddd)
                if k == '700':
                    # print(v)
                    # time.sleep(3333)
                    for kk, vv in v.items():
                        ddd = []
                        ddd.append('700')
                        ddd.append(kk)
                        # aa = str(vv).split('.')
                        # aa = '.'.join(aa)
                        aa = str(vv).split('.')
                        bb = aa[1]
                        cc = ''
                        if len(bb) == 1:
                            cc = f'{str(vv)}00000'
                        if len(bb) == 2:
                            cc = f'{str(vv)}0000'
                        if len(bb) == 3:
                            cc = f'{str(vv)}000'
                        if len(bb) == 4:
                            cc = f'{str(vv)}00'
                        if len(bb) == 5:
                            cc = f'{str(vv)}0'
                        ddd.append(cc)
                        ddd.append(model_id)
                        ddd.sort()
                        ddd_out.append(ddd)
                if k == '800':
                    # print(v)
                    # time.sleep(3333)
                    for kk, vv in v.items():
                        ddd = []
                        ddd.append('800')
                        ddd.append(kk)
                        # aa = str(vv).split('.')
                        # aa = '.'.join(aa)
                        aa = str(vv).split('.')
                        bb = aa[1]
                        cc = ''
                        if len(bb) == 1:
                            cc = f'{str(vv)}00000'
                        if len(bb) == 2:
                            cc = f'{str(vv)}0000'
                        if len(bb) == 3:
                            cc = f'{str(vv)}000'
                        if len(bb) == 4:
                            cc = f'{str(vv)}00'
                        if len(bb) == 5:
                            cc = f'{str(vv)}0'
                        ddd.append(cc)
                        ddd.append(model_id)
                        ddd.sort()
                        ddd_out.append(ddd)
        # print(ddd_out)
        return ddd_out

    def main_compare_iuid_mapping(self):
        print('正在比较iuid_mapping文件=======>>>')
        times = self.get_time()
        req_data = self.req_data(compare_data.iuid_mapping_url)
        s1 = f'iuid maping     接口共返回>>>>>>>>>>>>>>>>>>>>>>>>{len(req_data)}条数据'
        print(s1)
        for rd in req_data:
            print(rd)
        xlsx_data = self.read_xlsx()
        # print(xlsx_data)
        s2 = f'iuid maping     表格共返回>>>>>>>>>>>>>>>>>>>>>>>>{len(xlsx_data)}条数据'
        print(s2)
        for xx, xd in xlsx_data.items():
            print(xd)
        if len(req_data) == len(xlsx_data):
            j = 0
            # print(operator.eq(req_data,xlsx_data))
            for kk, vv in xlsx_data.items():
                i = 0
                for reqdatda_list in req_data:
                    if operator.eq(reqdatda_list, vv):
                        i += 1
                        j += 1
                    else:
                        pass
                if i != 1:
                    self.write_cpmpare_data('model_iuid_mapping-', kk, times)
                    print(kk)
            if j == len(req_data):
                print('\niuid_mapping >>>校验通过，数据一致!')
        else:
            print('行数不相同')
            self.write_cpmpare_data('model_iuid_mapping-', s1, times)
            self.write_cpmpare_data('model_iuid_mapping-', s2, times)

    def main_compare_model_info(self):
        print('正在比较model_info文件=======>>>')
        times = self.get_time()
        modelinfo_list = self.req_modelinfo()
        print(modelinfo_list)
        s1 = f'\nmodel info     接口共返回>>>>>>>>>>>>>>>>>>>>>>>>{len(modelinfo_list)}条数据'
        print(s1)
        # for rd in modelinfo_list:
        #     print(rd)
        csv_data = self.read_modelinfo_csv()
        print(csv_data)
        s2 = f'\nmodel info     表格共返回>>>>>>>>>>>>>>>>>>>>>>>>{len(csv_data)}条数据'
        print(s2)
        # for cc,cs in csv_data.items():
        #     print(cs)
        # print(operator.eq(req_data,xlsx_data))
        if len(modelinfo_list) == len(csv_data):
            j = 0
            for kk, vv in csv_data.items():
                i = 0
                for reqdatda_list in modelinfo_list:
                    if operator.eq(reqdatda_list, vv):
                        i += 1
                        j += 1
                    else:
                        pass

                if i != 1:
                    # cons = f'接口数据{reqdatda_list}===>表格数据{vv}\n'
                    self.write_cpmpare_data('model_info-', kk, times)
                    print(kk)
            if j == len(modelinfo_list):
                print('\nmodel_info >>>校验通过，数据一致!')
        else:
            print('行数不一样')
            self.write_cpmpare_data('model_info-', s1, times)
            self.write_cpmpare_data('model_info-', s2, times)

    def main_compare_weight_info(self):
        print('正在比较model_weight文件=======>>>')
        times = self.get_time()
        modelinfo_list = self.req_weight()
        print(modelinfo_list)
        s1 = f'model weight     接口共返回>>>>>>>>>>>>>>>>>>>>>>>>{len(modelinfo_list)}条数据'
        print(s1)
        csv_data = self.read_csv(self.model_weight_filepath)
        print(csv_data)
        s2 = f'model weight     表格共返回>>>>>>>>>>>>>>>>>>>>>>>>{len(csv_data)}条数据'
        print(s2)
        j = 0
        if len(modelinfo_list) == len(csv_data):
            for kk, vv in csv_data.items():
                i = 0
                for reqdatda_list in modelinfo_list:
                    # print('========')
                    # time.sleep(3333)
                    # print(reqdatda_list)
                    # print(vv)
                    # print('======')
                    if operator.eq(reqdatda_list, vv):
                        # print('yes')
                        i += 1
                        j += 1
                    else:
                        pass
                if i != 1:
                    # cons = f'接口数据{reqdatda_list}===>表格数据{vv}\n'
                    self.write_cpmpare_data('model_weight-', kk, times)
                    print(kk)
            if j == len(modelinfo_list):
                print('\nmodel_weight>>>校验通过，数据一致!')
        else:
            print('行数不一样')
            self.write_cpmpare_data('model_weight-', s1, times)
            self.write_cpmpare_data('model_weight-', s2, times)

    def main_compare_projections_info(self):
        print('正在比较model_projections文件---------->')
        times = self.get_time()
        modelinfo_list = self.req_projections()
        print(modelinfo_list)
        csv_data = self.read_csv(self.model_projections_filepath)
        print(csv_data)
        print(modelinfo_list)
        print(csv_data)
        # print(operator.eq(req_data,xlsx_data))
        if len(modelinfo_list) == len(csv_data):
            j = 0
            for kk, vv in csv_data.items():
                i = 0
                for reqdatda_list in modelinfo_list:
                    if operator.eq(reqdatda_list, vv):
                        i += 1
                        j += 1
                    else:
                        pass
                if i != 1:
                    # cons = f'接口数据{reqdatda_list}===>表格数据{vv}\n'
                    self.write_cpmpare_data('model_projections.txt', kk, times)
                    print(kk)
            if j == len(modelinfo_list):
                print('\nmodel_projection>>>校验通过，数据一致!')
        else:
            print('行数不一样')
            self.write_cpmpare_data('model_projections.txt', '行数不一样', times)

    def main_compare_distribution(self):
        '''
        比较 distribution 文件
        :return:
        '''
        times = self.get_time()
        print('正在比较model_distribution文件---------->')
        modelinfo_list = self.req_distribution()
        print(modelinfo_list)
        print(len(modelinfo_list))
        csv_data = self.read_csv(self.model_distribution_filepath)
        print(csv_data)
        print(len(csv_data))
        # print(operator.eq(req_data,xlsx_data))
        if len(modelinfo_list) == len(csv_data):
            j = 0
            for kk, vv in csv_data.items():
                i = 0
                for reqdatda_list in modelinfo_list:
                    if operator.eq(reqdatda_list, vv):
                        i += 1
                        j += 1
                    else:
                        pass
                if i != 1:
                    # cons = f'接口数据{reqdatda_list}===>表格数据{vv}\n'
                    self.write_cpmpare_data('model_distribution.txt', kk, times)
                    print(kk)
            if j == len(modelinfo_list):
                print('\nmodel_distribution >>>校验通过，数据一致!')
        else:
            print('行数不一样')
            self.write_cpmpare_data('model_iuid_distribution.txt', '行数不一样', times)

    def main_compare_backtesting(self):
        '''
        比较 backtesting 文件
        :return:
        '''
        times = self.get_time()
        print('正在比较model_backtesting文件---------->')
        modelinfo_list = self.req_backtesting()
        # print(modelinfo_list)
        print(modelinfo_list)
        csv_data = self.read_csv(self.model_backtesting_filepath)
        # print(csv_data)
        # print(len(modelinfo_list))
        print(csv_data)
        # print(operator.eq(req_data,xlsx_data))
        # for kk, vv in csv_data.items():
        #     i = 0
        #     for reqdatda_list in modelinfo_list:
        #
        #         if operator.eq(reqdatda_list, vv):
        #             i += 1
        #         else:
        #             pass
        #     if i != 1:
        #         # cons = f'接口数据{reqdatda_list}===>表格数据{vv}\n'
        #         self.write_cpmpare_data('model_distribution.txt', kk, times)
        #         print(kk)
        if modelinfo_list == len(csv_data):
            print('\nmodel_backtesting >>>校验通过，数据量一致!')
            self.write_cpmpare_data('model_backtestings.txt', '数据一样', times)
        else:
            print('\nmodel_backtesting >>>校验不通过，数据量不一致!')
            self.write_cpmpare_data('model_backtestings.txt', '数据不一样', times)

    def main_compare_benchmark(self):
        '''
        比较 benchmark文件
        :return:
        '''
        times = self.get_time()
        print('正在比较model_benchmark文件---------->')
        modelinfo_list = self.req_benchmark()
        print(modelinfo_list)
        print(len(modelinfo_list))
        csv_data = self.read_benchmark_csv(self.model_benchmark_filepath)
        print(csv_data)
        print(len(csv_data))
        # print(operator.eq(req_data,xlsx_data))
        if len(modelinfo_list) == len(csv_data):
            j = 0
            for kk, vv in csv_data.items():
                i = 0
                for reqdatda_list in modelinfo_list:
                    if operator.eq(reqdatda_list, vv):
                        i += 1
                        j += 1
                    else:
                        pass
                if i != 1:
                    # cons = f'接口数据{reqdatda_list}===>表格数据{vv}\n'
                    self.write_cpmpare_data('benchmark-', kk, times)
                    print(kk)
            if j == len(modelinfo_list):
                print('\nbenchmark >>>校验通过，数据一致!')
        else:
            print('行数不一样')
            self.write_cpmpare_data('benchmark-', '行数不一样', times)


if __name__ == '__main__':
    compare_data = Comparedata()
    # 文件夹里没有control_id.txt,control_model_id.txt,运行生成后注释
    # compare_data.write_control_ids()
    # compare_data.write_control_model_id()

    # 逐个比较
    # 1，生产数据一致
    # compare_data.main_compare_iuid_mapping()

    # 2,生产数据一致
    # compare_data.main_compare_model_info()

    # 3，生产数据一致
    # compare_data.main_compare_weight_info()

    # 4，生产数据一致
    # compare_data.main_compare_distribution()

    # 5，生产数据一致
    # compare_data.main_compare_projections_info()

    # 6，生产数据一致，抽取几组数据比较也一致
    compare_data.main_compare_backtesting()

    # 7，生产数据一致
    # compare_data.main_compare_benchmark()

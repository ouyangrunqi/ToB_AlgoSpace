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
import decimal
from decimal import Decimal, getcontext


class Comparedata:
    def __init__(self):

        self.model_CRB_filepath = r'D:\algo_space\CRB\3\algo_file_CRB.xlsx'
        self.algo_type_id = '19'
        self.algo_type_id_raas = '3'
        self.model_info_version = '3'
        # projections,每5个一行
        self.splice_length = 5

        self.iuid_mapping_url = f'https://algo-internal.aqumon.com/algo-space/v3/algo-space/algo_info_mapping?algo_type_id={self.algo_type_id}'
        # 通过algo_type查typeid
        self.algo_type_url = f'https://algo-internal.aqumon.com/algo-space/v3/algo-space/algo_type_version/list?algo_type_id={self.algo_type_id}&requires_active=true'
        self.headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36'
        }

        self.use_iuid_mapping = ['iuid', 'productCode','nameSimplified','descriptionSimplified','fundType']
        dd = self.get_date()
        self.instrument_info_url = f'https://algo-internal.aqumon.com/algo-space/v3/algo-space/algo_info_mapping/instrument_info?algo_type_id={self.algo_type_id}&date={dd}'
        self.not_use_instrument_info = ['exRatio','exDate']

    #获取本地当前系统时间（2021-08-16）
    def get_date(self):
        dd = time.strftime('%Y-%m-%d', time.localtime(time.time()))
        return dd

    def req_typeid(self):
        '''
        get type id
        :return:
        '''
        res = requests.get(self.algo_type_url, headers=self.headers)
        # json.loads,读取字符串并转为python对象
        res_json = json.loads(res.text)
        return res_json['data']['id'] #211

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

    #2021-07-02 161616
    def get_time(self):
        times = time.strftime("%Y-%m-%d %H-%M-%S", time.localtime())
        return times

    def get_control_ids(self):
        cons = []
        with open('control_ids.txt', 'r', encoding='utf-8')as f:
            for data in f.readlines():
                # 文件里是读出来是4688\n，replace(A, B)表示将A替换成B
                cons.append(data.replace('\n', ''))
        return cons

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

    def read_xlsx(self, num):
        workbook = xlrd.open_workbook(self.model_CRB_filepath)
        Data_sheet = workbook.sheets()[num]  # 通过索引获取第x个sheet
        rowNum = Data_sheet.nrows  # sheet行数
        colNum = Data_sheet.ncols  # sheet列数
        xlsx_data_dic = {}
        for i in range(1, rowNum):
            xlsx_data_list = []
            for j in range(colNum):
                xlsx_data_list.append(Data_sheet.cell_value(i, j))
            new_list = list(filter(None, xlsx_data_list))    #使用filter过滤None值
            new_list.sort()
            # xlsx_data_dic[f'第{i}行第一条数据不一样'] = new_list
            xlsx_data_dic[f'第{i}行'] = new_list
        return xlsx_data_dic


    def write_compare_data(self, sheet_name, cons, times):
        '''
        把比较后的数据结果写入txt
        :param dirpath_name:
        :param cons:
        :param times:
        :return:
        '''
        pwd = os.getcwd()
        dirpath = os.path.join(pwd, sheet_name)
        isExists = os.path.exists(dirpath)
        # a+ 打开一个文件用于读写，如果文件存在，则追加模式；文件不存在，新建文件，用于读写；
        if not isExists:
            with open(f'{sheet_name}{times}.txt', 'a+', encoding='utf-8')as f:
                f.write(f'{cons}\n')
        else:
            with open(f'{sheet_name}{times}.txt', 'a+', encoding='utf-8')as f:
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
        i = 0
        for data_dics in res_json['data']:
            modelinfo_list = []
            for k, v in data_dics.items():
                if k == 'id':
                    model_ids = self.get_control_model_id()
                    model_id = model_ids[i].split('==')[1]
                    modelinfo_list.append(model_id)
                    modelinfo_list.append(self.algo_type_id_raas)
                    modelinfo_list.append(self.model_info_version)
                if k == 'sector':
                    modelinfo_list.append(v)
                if k == 'region':
                    modelinfo_list.append(v)
                if k == 'riskRatio':
                    #接口返回40.00
                    modelinfo_list.append(str(v).split('.')[0])
            modelinfo_list.sort()
            i += 1
            modelinfo_out_list.append(modelinfo_list)
        return modelinfo_out_list

    def req_model_keyindex(self):

        db = pymysql.connect(host="rm-6nncv53w4dl5x7874.mysql.rds.aliyuncs.com", user="raas_rw",
                             password="LvSdi3vL2vIcg7pZl69S", db="raas", port=3306)
        cursor = db.cursor()
        table = 'algo_model'
        cursor.execute(
            f'SELECT * FROM {table} WHERE algo_type = {self.algo_type_id_raas} and version = {self.model_info_version}')
        selectResultList = cursor.fetchall()
        iuiddd = []
        for i in range(len(selectResultList)):
            if len(selectResultList[i]):
                iuiddd.append(selectResultList[i][13])
            else:
                return 'error!!! ===> model_code == none'

        islist = []
        for idd in iuiddd:
            if idd == '3NONNONDNULL0':
                model_id = "ratio_0"
            if idd == '3NONNONDNULL40':
                model_id = "ratio_40"
            if idd == '3NONNONDNULL50':
                model_id = "ratio_50"
            if idd == '3NONNONDNULL70':
                model_id = "ratio_70"

            cursor.execute(f"select * FROM algo_model_kpi where model_code= '{idd}'")
            selectResultList = cursor.fetchall()
            for i, sss in enumerate(selectResultList):
                if selectResultList[i]:
                    if i == 0:
                        iuiddds = sss[4:8]
                        # ius = []
                        # for iu in iuiddds:
                        #     ius.append(str(iu))
                        # 列表推导式（构建列表快捷方式）会遍历后面的可迭代对象, 然后按照for前的表达式进行运算, 生成最终的列表
                        ius = [str(iu) for iu in iuiddds]
                    else:
                        # 取5y的2个数据
                        # ESTIMATED_RETURN == 5y的HISTORICAL_RETURN
                        # ESTIMATED_VOLATILITY == 5y的HISTORICAL_VOLATILITY
                        iuiddds2 = sss[4:6]
                        ius2 = [str(iu2) for iu2 in iuiddds2]
            iii = ius + ius2
            iii.append(model_id)
            iii.sort()
            islist.append(iii)

        return islist

    def req_model_distribution(self):
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

            if res_json['data']:
                for k, v in res_json['data'].items():
                    if k == '400':
                        for kk, vv in v.items():
                            ddd = []
                            ddd.append('400')
                            ddd.append(kk)
                            aa = str(vv).split('.')
                            bb = aa[1]
                            cc = ''
                            # 表格中DIST_VALUE保留小数点后六位，接口返回的v，不足六位补0
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
                            if len(bb) == 6:
                                cc = str(vv)
                            ddd.append(cc)
                            ddd.append(model_id)
                            ddd.sort()
                            ddd_out.append(ddd)

                    if k == '300':
                        for kk, vv in v.items():
                            ddd = []
                            ddd.append('300')
                            ddd.append(kk)
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
                            if len(bb) == 6:
                                cc = str(vv)
                            ddd.append(cc)
                            ddd.append(model_id)
                            ddd.sort()
                            ddd_out.append(ddd)

                    if k == '800':
                        for kk, vv in v.items():
                            ddd = []
                            ddd.append('800')
                            ddd.append(kk)
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
                            if len(bb) == 6:
                                cc = str(vv)
                            ddd.append(cc)
                            ddd.append(model_id)
                            ddd.sort()
                            ddd_out.append(ddd)
        return ddd_out


    def req_model_weight(self):
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
            for data_dics in res_json['data']:
                ddd = []
                for k, v in data_dics.items():
                    if k == 'iuid':
                        ddd.append(model_id)
                        ddd.append(v)
                    if k == 'weight':
                        aa = str(v).split('.')
                        bb = aa[1]
                        cc = ''
                        # 表格中weight保留小数点后六位，接口返回的v，不足六位补0
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
                        if len(bb) == 6:
                            cc = v
                        ddd.append(cc)
                ddd.sort()
                ddd_out.append(ddd)
        return ddd_out

    def req_model_projections(self):
        cmis = self.get_control_model_id()
        ddd_out = []
        qc = []
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
            if oldlist:
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
                        ne_list = oldlist[m:self.splice_length * (m + 1)]
                        nee_list.append(ne_list)
                    else:
                        m = self.splice_length * (j + 1)
                        ne_list = oldlist[m:m + self.splice_length]
                        nee_list.append(ne_list)
                        j += 1
                # print(nee_list)
                #方法一
                for nn in nee_list:
                    ddd = []
                    ii = 0
                    for data_dics in nn:
                        for k, v in data_dics.items():
                            # 初始化0，循环一次ii+1,即每5个一组，5个数据中有1个取了时间，其他4个循环，ii==2、3...即不会再取时间
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
                  #方法2，将时间都加进来后去重
        #         for nn in nee_list:
        #             ddd = []
        #             qc = []
        #             for data_dics in nn:
        #                 for k, v in data_dics.items():
        #                     if k == 'projectionDate':
        #                         v = v.split('T')[0]
        #                         qc.append(v)
        #                         for kk in qc:
        #                             if not kk in ddd:
        #                                 ddd.append(kk)
        #                     if k == 'projectionValue':
        #                         aa = str(v).split('.')
        #                         bb = aa[1]
        #                         cc = ''
        #                         if len(bb) == 1:
        #                             cc = f'{str(v)}00000'
        #                         if len(bb) == 2:
        #                             cc = f'{str(v)}0000'
        #                         if len(bb) == 3:
        #                             cc = f'{str(v)}000'
        #                         if len(bb) == 4:
        #                             cc = f'{str(v)}00'
        #                         if len(bb) == 5:
        #                             cc = f'{str(v)}0'
        #                         ddd.append(cc)
        #             ddd.append(model_id)
        #             ddd.sort()
        #             ddd_out.append(ddd)
        # return ddd_out


    def req_model_backtesting(self):
        model_backtesting_lists = []
        cmis = self.get_control_model_id()
        for cm in cmis:
            print(f'now in backtesting request {cm} ----->')
            cm = cm.split('==')
            control_id = cm[0]
            bbb = cm[1]
            model_backtesting_url = f'https://algo-internal.aqumon.com/algo-space/v3/algo-space/algo_control/{control_id}/backtestings'
            res = requests.get(model_backtesting_url, headers=self.headers)
            res_json = json.loads(res.text)
            for data in res_json['data']:
                model_backtesting_list = []
                for k,v in data.items():
                    if k == 'algoControlId':
                        model_backtesting_list.append(bbb)
                    if k == 'backtestDate':
                        model_backtesting_list.append(v.split('T')[0])
                    # if k == 'backtestValue':
                    #     aa = str(v).split('.')
                    #     bb = aa[1]
                    #     cc = str(round(v, 6))
                    #     # 表格中backtestValue保留6位小数，接口中多的有8位，少的只有1位
                    #     if len(bb) == 1:
                    #         cc = f'{str(v)}00000'
                    #     if len(bb) == 2:
                    #         cc = f'{str(v)}0000'
                    #     if len(bb) == 3:
                    #         cc = f'{str(v)}000'
                    #     if len(bb) == 4:
                    #         cc = f'{str(v)}00'
                    #     if len(bb) == 5:
                    #         cc = f'{str(v)}0'
                    #     if len(bb) == 6:
                    #         cc = str(v)
                    if k == 'backtestValue':
                        # aa = decimal.Decimal(str(v))
                        # bb = round(aa,6)
                        bb = Decimal(str(v)).quantize(Decimal('0.000000'),rounding = 'ROUND_HALF_UP')

                        model_backtesting_list.append(str(bb))
                        model_backtesting_list.sort()
                        model_backtesting_lists.append(model_backtesting_list)
        return model_backtesting_lists

    def req_standby_fund(self):
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
            for data_dics in res_json['data']:
                ddd = []
                for k, v in data_dics.items():
                    if k == 'iuid':
                        ddd.append(v)
                    if k == 'backupList':
                        ddd.append(v)
                ddd.sort()
                ddd_out.append(ddd)
        #利用集合去重
        ddd_out_set = set(tuple(s) for s in ddd_out)
        print(ddd_out_set)
        #再把集合转成列表
        fund_list = [list(t) for t in ddd_out_set]
        print(fund_list)
        return fund_list



    def req_iuid_mapping(self):
        req_data_list = []
        res = requests.get(self.iuid_mapping_url, headers=self.headers)
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

    # def req_INSTRMENT_INFO(self):
    #     req_data_list = []
    #     res = requests.get(self.instrument_info_url, headers=self.headers)
    #     res_json = json.loads(res.text)
    #     for da in res_json['data']:
    #         data_list = []
    #         for k, v in da.items():
    #             # 只要k不是'exRatio','exDate'，就把其他字段对应的值加进去
    #             if k not in self.not_use_instrument_info:
    #                 data_list.append(str(v))
    #                 data_list.sort()
    #         req_data_list.append(data_list)
    #     return req_data_list


    def main_compare_model_info(self):
        print('正在比较算法模型文件=======>>>')
        times = self.get_time()
        modelinfo_list = self.req_modelinfo()
        print(modelinfo_list)
        s1 = f'算法模型     接口共返回>>>>>>>>>>>>>>>>>>>>>>>>{len(modelinfo_list)}条数据\n'
        print(s1)
        csv_data = self.read_xlsx(0)
        print(csv_data)
        s2 = f'算法模型     表格共返回>>>>>>>>>>>>>>>>>>>>>>>>{len(csv_data)}条数据\n'
        print(s2)
        if len(modelinfo_list) == len(csv_data):
            j = 0
            for kk, vv in csv_data.items():
                i = 0
                for reqdata_list in modelinfo_list:
                    # 数据比较，如有相同，i,j计数+1
                    if operator.eq(reqdata_list, vv):
                        i += 1
                        j += 1
                    else:
                        pass
                #如果一直是0，即数据不同，没有一个能匹配上
                if i != 1:
                    self.write_compare_data('算法模型-', kk, times)
                    print(kk)
            print('>>>以上数据不一致，如有')
            if j == len(modelinfo_list):
                print('\n算法模型 >>>校验通过，数据一致!')
        else:
            print('行数不一样')
            self.write_compare_data('算法模型-', s1, times)
            self.write_compare_data('算法模型-', s2, times)


    def main_compare_model_keyindex(self):
        print('正在比较模型关键指标文件=======>>>')
        times = self.get_time()
        model_keyindex_list = self.req_model_keyindex()
        print(model_keyindex_list)
        s1 = f'模型关键指标     接口共返回>>>>>>>>>>>>>>>>>>>>>>>>{len(model_keyindex_list)}条数据\n'
        print(s1)
        csv_data = self.read_xlsx(1)
        print(csv_data)
        s2 = f'模型关键指标     表格共返回>>>>>>>>>>>>>>>>>>>>>>>>{len(csv_data)}条数据\n'
        print(s2)
        j = 0
        if len(model_keyindex_list) == len(csv_data):
            for kk, vv in csv_data.items():
                i = 0
                for reqdata_list in model_keyindex_list:
                    # 数据比较相同，i,j计数+1
                    if operator.eq(reqdata_list, vv):
                        i += 1
                        j += 1
                    else:
                        pass
                #一直是0，即数据不同
                if i != 1:
                    self.write_compare_data('模型关键指标-', kk, times)
                    print(kk)
            print('>>>以上数据不一致，如有')
            if j == len(model_keyindex_list):
                print('\n模型关键指标>>>校验通过，数据一致!')
        else:
            print('行数不一样')
            self.write_compare_data('模型关键指标-', s1, times)
            self.write_compare_data('模型关键指标-', s2, times)


    def main_compare_model_distribution(self):
        print('正在比较模型投资分布文件=======>>>')
        times = self.get_time()
        model_distribution_list = self.req_model_distribution()
        print(model_distribution_list)
        s1 = f'模型投资分布     接口共返回>>>>>>>>>>>>>>>>>>>>>>>>{len(model_distribution_list)}条数据\n'
        print(s1)
        csv_data = self.read_xlsx(2)
        print(csv_data)
        s2 = f'模型投资分布     表格共返回>>>>>>>>>>>>>>>>>>>>>>>>{len(csv_data)}条数据\n'
        print(s2)
        j = 0
        if len(model_distribution_list) == len(csv_data):
            for kk, vv in csv_data.items():
                i = 0
                for reqdata_list in model_distribution_list:
                    # 数据比较相同，i,j计数+1
                    if operator.eq(reqdata_list, vv):
                        i += 1
                        j += 1
                    else:
                        pass
                #一直是0，即数据不同
                if i != 1:
                    self.write_compare_data('模型投资分布-', kk, times)
                    print(kk)
            print('>>>以上数据不一致，如有')
            if j == len(model_distribution_list):
                print('\n模型投资分布>>>校验通过，数据一致!')
        else:
            print('行数不一样')
            self.write_compare_data('模型投资分布-', s1, times)
            self.write_compare_data('模型投资分布-', s2, times)

    def main_compare_model_weight(self):
        print('正在比较算法模型权重文件=======>>>')
        times = self.get_time()
        modelinfo_list = self.req_model_weight()
        print(modelinfo_list)
        s1 = f'算法模型权重     接口共返回>>>>>>>>>>>>>>>>>>>>>>>>{len(modelinfo_list)}条数据\n'
        print(s1)
        csv_data = self.read_xlsx(3)
        print(csv_data)
        s2 = f'算法模型权重     表格共返回>>>>>>>>>>>>>>>>>>>>>>>>{len(csv_data)}条数据\n'
        print(s2)
        j = 0
        if len(modelinfo_list) == len(csv_data):
            for kk, vv in csv_data.items():
                i = 0
                for reqdata_list in modelinfo_list:
                    # 数据比较相同，i,j计数+1
                    if operator.eq(reqdata_list, vv):
                        i += 1
                        j += 1
                    else:
                        pass
                #一直是0，即数据不同
                if i != 1:
                    # print('数据不一致：')
                    self.write_compare_data('算法模型权重-', kk, times)
                    print(kk)
            print('>>>以上数据不一致，如有')
            if j == len(modelinfo_list):
                print('\n算法模型权重>>>校验通过，数据一致!')
        else:
            print('行数不一样')
            self.write_compare_data('算法模型权重-', s1, times)
            self.write_compare_data('算法模型权重-', s2, times)

    def main_compare_model_projections(self):
        print('正在比较算法预测文件=======>>>')
        times = self.get_time()
        modelinfo_list = self.req_model_projections()
        print(modelinfo_list)
        if modelinfo_list:
            s1 = f'算法预测     接口共返回>>>>>>>>>>>>>>>>>>>>>>>>{len(modelinfo_list)}条数据\n'
        else:
            s1 = f'算法预测     接口返回data无数据！！！\n'
        print(s1)
        csv_data = self.read_xlsx(4)
        print(csv_data)
        if csv_data:
            s2 = f'算法预测     表格共返回>>>>>>>>>>>>>>>>>>>>>>>>{len(csv_data)}条数据\n'
        else:
            s2 = f'算法预测     表格无数据！！！\n'
        print(s2)
        j = 0
        if modelinfo_list or csv_data:
            if len(modelinfo_list) == len(csv_data):
                for kk, vv in csv_data.items():
                    i = 0
                    for reqdata_list in modelinfo_list:
                        # 数据比较相同，i,j计数+1
                        if operator.eq(reqdata_list, vv):
                            i += 1
                            j += 1
                        else:
                            pass
                    # 一直是0，即数据不同
                    if i != 1:
                        self.write_compare_data('算法预测-', kk, times)
                        print(kk)
                print('>>>以上数据不一致，如有')
                if j == len(modelinfo_list):
                    print('\n算法预测>>>校验通过，数据一致!')
            else:
                print('行数不一样')
                self.write_compare_data('算法预测-', s1, times)
                self.write_compare_data('算法预测-', s2, times)
        else:
            print("no data!")


    # def main_compare_model_backtesting(self):
    #     times = self.get_time()
    #     print('正在比较算法回溯测试文件---------->')
    #     modelinfo_list = self.req_model_backtesting()
    #     print(modelinfo_list)
    #     csv_data = self.read_xlsx(5)
    #     # print(csv_data)
    #     print(len(csv_data))
    #     if modelinfo_list == len(csv_data):
    #         print('\n算法回溯测试 >>>校验通过，数据量一致!')
    #         # self.write_compare_data('算法回溯测试.txt', '数据一样', times)
    #     else:
    #         print('\n算法回溯测试 >>>校验不通过，数据量不一致!')
    #         # self.write_compare_data('算法回溯测试.txt', '数据不一样', times)
    #         self.write_compare_data('算法回溯测试_', '数据不一样', times)

    def main_compare_model_backtesting(self):
        print('正在比较算法回溯测试文件---------->')
        times = self.get_time()
        modelinfo_list = self.req_model_backtesting()
        print(modelinfo_list)
        s1 = f'算法回溯测试     接口共返回>>>>>>>>>>>>>>{len(modelinfo_list)}条数据\n'
        print(s1)
        csv_data = self.read_xlsx(5)
        print(csv_data)
        s2 = f'算法回溯测试     表格共返回>>>>>>>>>>>>>>{len(csv_data)}条数据\n'
        print(s2)
        if len(modelinfo_list) == len(csv_data):
            j = 0
            for kk,vv in csv_data.items():
                i = 0
                for reqdata_list in modelinfo_list:
                    # 数据比较相同，i,j计数+1
                    if operator.eq(reqdata_list,vv):
                        i += 1
                        j += 1
                    else:
                        pass
                #一直是0，即数据不同
                if i != 1:
                    self.write_compare_data('算法回溯测试-',kk,times)
                    print(kk)
            print('>>>以上数据不一致，如有')
            if j == len(modelinfo_list):
                print('\n算法回溯测试 >>>校验通过，数据量一致!')

        else:
            print('\n算法回溯测试 >>>校验不通过，数据量不一致!')
            self.write_compare_data('算法回溯测试-',s1, times)
            self.write_compare_data('算法回溯测试-',s2, times)
        # else:
        #     print('\n算法回溯测试 >>>校验不通过，数据量不一致!')
        #     self.write_compare_data('算法模型-',s1, times)
        #     self.write_compare_data('算法模型-',s2, times)
        #     j = 0
        #     for kk,vv in csv_data.items():
        #         i = 0
        #         for reqdata_list in modelinfo_list:
        #             # 数据比较相同，i,j计数+1
        #             if operator.eq(reqdata_list,vv):
        #                 i += 1
        #                 j += 1
        #             else:
        #                 pass
        #         #一直是0，即数据不同
        #         if i != 1:
        #             self.write_compare_data('算法模型-',kk,times)
        #             print(kk)

    def main_compare_standby_fund(self):
        print('正在比较后备基金名单文件=======>>>')
        times = self.get_time()
        standby_fund_list = self.req_standby_fund()
        print(standby_fund_list)
        s1 = f'后备基金名单     接口共返回>>>>>>>>>>>>>>>>>>>>>>>>{len(standby_fund_list)}条数据\n'
        print(s1)
        csv_data = self.read_xlsx(6)
        print(csv_data)
        s2 = f'后备基金名单     表格共返回>>>>>>>>>>>>>>>>>>>>>>>>{len(csv_data)}条数据\n'
        print(s2)
        j = 0
        if len(standby_fund_list) == len(csv_data):
            for kk, vv in csv_data.items():
                i = 0
                for reqdata_list in standby_fund_list:
                    # 数据比较相同，i,j计数+1
                    if operator.eq(reqdata_list, vv):
                        i += 1
                        j += 1
                    else:
                        pass
                #一直是0，即数据不同
                if i != 1:
                    # print('数据不一致：')
                    self.write_compare_data('后备基金名单-', kk, times)
                    print(kk)
            print('>>>以上数据不一致，如有')
            if j == len(standby_fund_list):
                print('\n后备基金名单>>>校验通过，数据一致!')
        else:
            print('行数不一样')
            self.write_compare_data('后备基金名单-', s1, times)
            self.write_compare_data('后备基金名单-', s2, times)


    def main_compare_iuid_mapping(self):
        print('正在比较基金文件=======>>>')
        times = self.get_time()
        req_data = self.req_iuid_mapping()
        s1 = f'\n基金     接口共返回>>>>>>>>>>>>>>>>>>>>>>>>{len(req_data)}条数据'
        print(s1)
        print(req_data)
        # for rd in req_data:
        #     print(rd)
        xlsx_data = self.read_xlsx(7)
        # print(xlsx_data)
        s2 = f'\n基金     表格共返回>>>>>>>>>>>>>>>>>>>>>>>>{len(xlsx_data)}条数据'
        print(s2)
        print(xlsx_data)
        # for xx, xd in xlsx_data.items():
        #     print(xd)
        if len(req_data) == len(xlsx_data):
            j = 0
            # print(operator.eq(req_data,xlsx_data))
            for kk, vv in xlsx_data.items():
                i = 0
                for reqdata_list in req_data:
                    # 数据比较相同，i,j计数+1
                    if operator.eq(reqdata_list, vv):
                        i += 1
                        j += 1
                    else:
                        pass
                #一直是0，即数据不同
                if i != 1:
                    self.write_compare_data('基金-', kk, times)
                    print(kk)
            print('>>>以上数据不一致，如有')
            if j == len(req_data):
                print('\n基金 >>>校验通过，数据一致!')
        else:
            print('行数不相同')
            self.write_compare_data('基金-', s1, times)
            self.write_compare_data('基金-', s2, times)


    # def main_compare_INSTRMENT_INFO(self):
    #     print('正在比较INSTRMENT_INFO文件=======>>>')
    #     times = self.get_time()
    #     req_data = self.req_INSTRMENT_INFO()
    #     s1 = f'\nINSTRMENT_INFO     接口共返回>>>>>>>>>>>>>>>>>>>>>>>>{len(req_data)}条数据'
    #     print(s1)
    #     print(req_data)
    #     for rd in req_data:
    #         print(rd)
    #     xlsx_data = self.read_xlsx(7)
    #     s2 = f'\nINSTRMENT_INFO     表格共返回>>>>>>>>>>>>>>>>>>>>>>>>{len(xlsx_data)}条数据'
    #     print(s2)
    #     print(xlsx_data)
    #     for xx, xd in xlsx_data.items():
    #         print(xd)
    #     if len(req_data) == len(xlsx_data):
    #         j = 0
    #         # print(operator.eq(req_data,xlsx_data))
    #         for kk, vv in xlsx_data.items():
    #             i = 0
    #             for reqdata_list in req_data:
    #                 # 数据比较相同，i,j计数+1
    #                 if operator.eq(reqdata_list, vv):
    #                     i += 1
    #                     j += 1
    #                 else:
    #                     pass
    #             #一直是0，即数据不同
    #             if i != 1:
    #                 self.write_compare_data('INSTRMENT_INFO-', kk, times)
    #                 print(kk)
    #         if j == len(req_data):
    #             print('\nINSTRMENT_INFO >>>校验通过，数据一致!')
    #     else:
    #         print('行数不相同')
    #         self.write_compare_data('INSTRMENT_INFO-', s1, times)
    #         self.write_compare_data('INSTRMENT_INFO-', s2, times)


if __name__ == '__main__':
    compare_data = Comparedata()

    # 文件夹里没有control_id.txt,control_model_id.txt,运行生成后注释
    # compare_data.write_control_ids()
    # compare_data.write_control_model_id()

    # 0.比较算法模型，数据一致
    # compare_data.main_compare_model_info()

    # 1.比较模型关键指标，数据一致
    # compare_data.main_compare_model_keyindex()

    # 2.比较模型投资分布，数据一致
    # compare_data.main_compare_model_distribution()

    # 3.比较算法模型权重，数据一致
    # compare_data.main_compare_model_weight()

    # 4.比较算法预测，数据一致
    compare_data.main_compare_model_projections()

    # 5.比较算法回溯测试，数据一致
    # compare_data.main_compare_model_backtesting()

    # 6.比较后备基金名单，数据一致
    # compare_data.main_compare_standby_fund()

    # 7.比较基金，数据一致
    # compare_data.main_compare_iuid_mapping()


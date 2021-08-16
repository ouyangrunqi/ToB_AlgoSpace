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
        self.iuid_mapping_filepath = r'D:\CMBWLB\CMBWLB\oss\iuid_mapping_WingLung.xls'
        self.model_info_filepath = r'D:\CMBWLB\CMBWLB\oss\model_info.csv'
        self.model_weight_filepath = r'D:\CMBWLB\CMBWLB\oss\model_weight.csv'
        self.model_distribution_filepath = r'D:\CMBWLB\CMBWLB\oss\model_distribution.csv'
        self.model_projections_filepath = r'D:\CMBWLB\CMBWLB\oss\model_projections.csv'
        self.model_backtesting_filepath = r'D:\CMBWLB\CMBWLB\oss\model_backtesting.csv'

        self.req_iuid = '223'
        self.model_info_version = '13'

        self.iuid_mapping_url = f'https://algo-dev.aqumon.com/algo-space/v3/algo-space/algo_info_mapping?algo_type_id={self.req_iuid}'
        self.algo_type_url = f'https://algo-dev.aqumon.com/algo-space/v3/algo-space/algo_type_version/list?algo_type_id={self.req_iuid}&requires_active=true'
        self.model_weight_url = 'https://algo-dev.aqumon.com/algo-space/v3/algo-space/algo_control/5286/weights?extends_result=false'
        self.model_distribution_url = 'https://algo-dev.aqumon.com/algo-space/v3/algo-space/algo_control/5286/distributions'
        self.model_projection_url = 'https://algo-dev.aqumon.com/algo-space/v3/algo-space/algo_control/5286/projections'
        self.model_backtesting_url = 'https://algo-dev.aqumon.com/algo-space/v3/algo-space/algo_control/5286/backtestings?start_date=2021-04-01'
        self.headers = {
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.212 Safari/537.36'
        }
        self.use_iuid_mapping = ['iuid', 'productCode', 'isin', 'name', 'nameTraditional', 'descriptionSimplified',
                                 'fundType', 'divType']

    def write_control_ids(self):
        type_id = self.req_typeid()
        model_info_url = f'https://algo-dev.aqumon.com/algo-space/v3/algo-space/algo_control/list?algo_type_version_id={type_id}'
        res = requests.get(model_info_url, headers=self.headers)
        res_json = json.loads(res.text)
        # print(res_json['data'])
        # for res in res_json['data']:
        #     print(res.values())
        for data_dics in res_json['data']:
            for k, v in data_dics.items():
                if k == 'id':
                    with open('control_ids.txt', 'a+', encoding='utf-8')as f:
                        f.write(f'{v}\n')


    def get_time(self):
        tims = time.strftime("%Y%m%d%H%MS", time.localtime())
        return tims

    def get_control_ids(self):
        cons = []
        with open('control_ids.txt', 'r', encoding='utf-8')as f:
            for data in f.readlines():
                cons.append(data.replace('\n', ''))
        return cons

    def req_weight(self):
        cmis = self.get_control_model_id()

        for cm in cmis:
            print(f'now in weight request {cm} ----->')
            ddd_out = []
            cm = cm.split('==')
            control_id = cm[0]
            model_id = cm[1]
            model_weight_url = f'https://algo-dev.aqumon.com/algo-space/v3/algo-space/algo_control/{control_id}/weights?extends_result=false'
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
                        ddd.append(str(v))
                ddd.sort()
                ddd_out.append(ddd)
        return ddd_out

    def req_projections(self):
        cmis = self.get_control_model_id()

        for cm in cmis:
            print(f'now in weight request {cm} ----->')
            ddd_out = []
            cm = cm.split('==')
            control_id = cm[0]
            model_id = cm[1]
            model_weight_url = f'https://algo-dev.aqumon.com/algo-space/v3/algo-space/algo_control/{control_id}/projections'
            res = requests.get(model_weight_url, headers=self.headers)
            res_json = json.loads(res.text)
            # print(res_json['data'])
            for data_dics in res_json['data']:
                for k, v in data_dics.items():
                    if k == 'algoControlId' and str(v) == cm:
                        ddd = []
                        ddd.append()

                    if k == 'projectionDate':
                        v = v.split('T')[0]
                        ddd.append(v)
                    if k == 'iuid':
                        ddd.append(model_id)
                        ddd.append(v)
                    if k == 'weight':
                        ddd.append(str(v))
                ddd.sort()
                ddd_out.append(ddd)
        return ddd_out

    def req_data(self, req_url):
        req_data_list = []
        res = requests.get(req_url, headers=self.headers)
        res_json = json.loads(res.text)
        for da in res_json['data']:
            data_list = []
            for k, v in da.items():
                if k in self.use_iuid_mapping:
                    if v == 'REINVESTMENT':
                        v = 'Acc'
                    if v == 'STOCK':
                        v = 'Equity'
                    if v == 'BOND':
                        v = 'Bond'
                    data_list.append(v)
                    data_list.sort()
            req_data_list.append(data_list)
        #
        # print(f'{req_url}---->共返回{len(req_data_list) * len(self.use_iuid_mapping)}条数据')
        # print(f'接口返回数据：  {req_data_list}')
        return req_data_list

    def req_typeid(self):
        res = requests.get(self.algo_type_url, headers=self.headers)
        res_json = json.loads(res.text)
        return res_json['data']['id']

    def req_modelinfo(self):
        '''
        请求 model_info 接口获取数据
        :return:
        '''

        modelinfo_out_list = []
        type_id = self.req_typeid()
        model_info_url = f'https://algo-dev.aqumon.com/algo-space/v3/algo-space/algo_control/list?algo_type_version_id={type_id}'
        res = requests.get(model_info_url, headers=self.headers)
        res_json = json.loads(res.text)
        # print(res_json['data'])
        # for res in res_json['data']:
        #     print(res.values())
        for data_dics in res_json['data']:
            modelinfo_list = []
            for k, v in data_dics.items():
                if k == 'id':
                    # self.idlist.append(v)
                    model_id = self.getdata_fromdb(v)
                    modelinfo_list.append(model_id)
                    modelinfo_list.append(self.req_iuid)
                    modelinfo_list.append(self.model_info_version)
                if k == 'sector':
                    modelinfo_list.append(v)
                if k == 'region':
                    modelinfo_list.append(v)
                if k == 'riskRatio':
                    modelinfo_list.append(str(v))
                if k == 'fundType':
                    print(v)
                    v = json.loads(v)
                    vlist = v['2020-12-31']
                    new_vlist = []
                    new_vdic = {}
                    for kk in vlist:
                        kkk = kk.keys()
                        kkk = list(kkk)
                        kkk.reverse()
                        new_vdic[kkk[0]] = kk[kkk[0]]
                        new_vdic[kkk[1]] = kk[kkk[1]]
                        new_vlist.append(new_vdic)

                    modelinfo_list.append(str(new_vlist))
                    # print(modelinfo_list)
                    modelinfo_list.sort()
            modelinfo_out_list.append(modelinfo_list)
        # print(f'model_info 接口共返回{len(modelinfo_out_list)}条数据------>')
        return modelinfo_out_list

    def getdata_fromdb(self, native_mode_id):
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
        Data_sheet = workbook.sheets()[0]  # 通过索引获取
        rowNum = Data_sheet.nrows  # sheet行数
        colNum = Data_sheet.ncols  # sheet列数
        xlxs_data_dic = {}
        for i in range(1, rowNum):
            xlxs_data_list = []
            for j in range(colNum):
                xlxs_data_list.append(Data_sheet.cell_value(i, j))
            new_list = list(filter(None, xlxs_data_list))
            trans_new_list = self.trans_five(new_list)
            # print(len(trans_new_list))
            trans_new_list1 = trans_new_list[:8]
            trans_new_list2 = trans_new_list[8:]
            trans_new_list1.sort()
            trans_new_list2.sort()
            xlxs_data_dic[f'第{i}行第一条数据不一样'] = trans_new_list1
            xlxs_data_dic[f'第{i}行第二条数据不一样'] = trans_new_list2
        #
        # print(f'对应表格共{len(xlxs_data_dic) * 8}条数据')
        # print(f'new xlsx data: {xlxs_data_dic}')
        return xlxs_data_dic

    def read_csv(self, filename):
        weight_csv_dic = {}
        with open(filename, 'r') as f:
            reader = csv.reader(f)
            i = 0
            for row in reader:
                file = []
                if i == 0:
                    pass
                else:
                    row.sort()
                    file.append(row)
                    weight_csv_dic[f'第{i + 1}行数据不一样'] = file
            return weight_csv_dic

    def read_modelinfo_csv(self):
        modelinfo_csv_dic = {}
        with open(self.model_info_filepath, 'r') as f:
            reader = csv.reader(f)
            i = 0
            for row in reader:
                file = []
                if i == 0:
                    pass
                else:
                    row = row[0:6] + row[7:]
                    row.sort()
                    # print(row)
                    file.append(row)
                    modelinfo_csv_dic[f'第{i + 1}行数据不一样'] = file
                i += 1
            return modelinfo_csv_dic

    def trans_five(self, data_list):
        new_data_list = []
        for da in data_list:
            if re.findall('^\d{4}$', da):
                da = f'0{da}'
                new_data_list.append(da)
            else:
                new_data_list.append(da)
        return new_data_list

    def write_cpmpare_data(self, dirpath_name, cons, times):
        pwd = os.getcwd()
        dirpath = os.path.join(pwd, dirpath_name)
        isExists = os.path.exists(dirpath)
        if not isExists:
            with open(f'{dirpath_name}{times}.txt', 'a+', encoding='utf-8')as f:
                f.write(cons)
        else:
            with open(f'{dirpath_name}{times}.txt', 'a+', encoding='utf-8')as f:
                f.write(cons)

    def write_control_model_id(self):
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
        cmi = []
        with open('control_model_ids.txt', 'r', encoding='utf-8')as f:
            for da in f.readlines():
                cmi.append(da.replace('\n', ''))
        return cmi

    def req_distribution(self):
        cmis = self.get_control_model_id()

        for cm in cmis:
            print(f'now in weight request {cm} ----->')
            ddd_out = []
            cm = cm.split('==')
            control_id = cm[0]
            model_id = cm[1]
            model_distrubution_url = f'https://algo-dev.aqumon.com/algo-space/v3/algo-space/algo_control/{control_id}/distributions'
            res = requests.get(model_distrubution_url, headers=self.headers)
            res_json = json.loads(res.text)
            # print(res_json['data'])
            # for res in res_json['data']:
            #     print(res.val慤爮灥慬散✨湜Ⱗ✧ues())
            for k, v in res_json['data'].items():
                ddd = []
                if k == '200':
                    for kk, vv in v.items():
                        ddd.append('200')
                        ddd.append(kk)
                        ddd.append(str(vv))
                        ddd.append(model_id)
                        ddd.sort()
                        ddd_out.append(ddd)
        return ddd_out

    def main_compare_iuid_mapping(self):
        print('正在比较iuid_mapping文件---------->')
        times = self.get_time()
        req_data = self.req_data(compare_data.iuid_mapping_url)
        xlsx_data = self.read_xlsx()
        if len(req_data) == len(xlsx_data):
            # print(operator.eq(req_data,xlsx_data))
            for kk, vv in xlsx_data.items():
                i = 0
                for reqdatda_list in req_data:
                    if operator.eq(reqdatda_list, vv):
                        i += 1
                    else:
                        pass
                if i != 1:
                    # cons = f'接口数据{reqdatda_list}===>表格数据{vv}\n'
                    self.write_cpmpare_data('model_iuid_mapping.txt', kk,times)
                    print(kk)
        else:
            print('行数不相同')
            self.write_cpmpare_data('model_iuid_mapping.txt', '行数不相同', times)

    def main_compare_model_info(self):
        print('正在比较model_info文件---------->')
        times = self.get_time()
        modelinfo_list = self.req_modelinfo()
        # print(modelinfo_list)
        print(len(modelinfo_list))
        csv_data = self.read_modelinfo_csv()
        # print(csv_data)
        print(len(csv_data))
        # print(operator.eq(req_data,xlsx_data))
        if len(modelinfo_list) == len(csv_data):
            for kk, vv in csv_data.items():
                i = 0
                for reqdatda_list in modelinfo_list:
                    if operator.eq(reqdatda_list, vv):
                        i += 1
                    else:
                        pass
                if i != 1:
                    # cons = f'接口数据{reqdatda_list}===>表格数据{vv}\n'
                    self.write_cpmpare_data('model_info.txt', kk,times)
                    print(kk)
        else:
            print('行数不一样')
            self.write_cpmpare_data('model_info.txt', '行数不一样', times)

    def main_compare_weight_info(self):
        print('正在比较model_weight文件---------->')
        times = self.get_time()
        modelinfo_list = self.req_weight()
        # print(modelinfo_list)
        print(len(modelinfo_list))
        csv_data = self.read_csv(self.model_weight_filepath)
        # print(csv_data)
        print(len(csv_data))
        # print(operator.eq(req_data,xlsx_data))
        if len(modelinfo_list) == len(csv_data):
            for kk, vv in csv_data.items():
                i = 0
                for reqdatda_list in modelinfo_list:
                    if operator.eq(reqdatda_list, vv):
                        i += 1
                    else:
                        pass
                if i != 1:
                    # cons = f'接口数据{reqdatda_list}===>表格数据{vv}\n'
                    self.write_cpmpare_data('model_weight.txt', kk,times)
                    print(kk)
        else:
            print('行数不一样')
            self.write_cpmpare_data('model_weight.txt', '行数不一样', times)

    def main_compare_projections_info(self):
        print('正在比较model_projections文件---------->')
        times = self.get_time()
        modelinfo_list = self.req_projections()
        # print(modelinfo_list)
        csv_data = self.read_csv(self.model_projections_filepath)
        # print(csv_data)
        # print(operator.eq(req_data,xlsx_data))
        if len(modelinfo_list) == len(csv_data):
            for kk, vv in csv_data.items():
                i = 0
                for reqdatda_list in modelinfo_list:

                    if operator.eq(reqdatda_list, vv):
                        i += 1
                    else:
                        pass
                if i != 1:
                    # cons = f'接口数据{reqdatda_list}===>表格数据{vv}\n'
                    self.write_cpmpare_data('model_projections.txt', kk)
                    print(kk)
        else:
            print('行数不一样')
            self.write_cpmpare_data('model_projections.txt', '行数不一样', times)

    def main_compare_distribution_info(self):
        print('正在比较model_distribution文件---------->')
        modelinfo_list = self.req_distribution()
        # print(modelinfo_list)
        print(len(modelinfo_list))
        csv_data = self.read_csv(self.model_distribution_filepath)
        # print(csv_data)
        print(len(csv_data))
        # print(operator.eq(req_data,xlsx_data))
        if len(modelinfo_list) == len(csv_data):

            for kk, vv in csv_data.items():
                i = 0
                for reqdatda_list in modelinfo_list:

                    if operator.eq(reqdatda_list, vv):
                        i += 1
                    else:
                        pass
                if i != 1:
                    # cons = f'接口数据{reqdatda_list}===>表格数据{vv}\n'
                    self.write_cpmpare_data('model_distribution.txt', kk, times)
                print(kk)
        else:
            print('行数不一样')
            self.write_cpmpare_data('model_iuid_distribution.txt', '行数不一样', times)


if __name__ == '__main__':
    compare_data = Comparedata()
    # compare_data.main_compare_iuid_mapping()
    # compare_data.req_typeid(compare_data.algo_type_url)
    # id = compare_data.req_typeid()
    # compare_data.req_modelinfo()
    # compare_data.main_compare_model_info()
    # compare_data.read_modelinfo_csv()
    # compare_data.req_weight()
    # compare_data.write_control_model_id()
    # aa = compare_data.req_weight()
    # print(aa)
    # compare_data.main_compare_model_info()
    # compare_data.main_compare_model_weight()
    compare_data.write_control_ids()
    compare_data.write_control_model_id()
    # compare_data.main_compare_weight_info()
    compare_data.main_compare_distribution_info()



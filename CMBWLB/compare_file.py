import csv
import operator
import os


dir1 = "D:\ToB_AlgoSpace\CMBWLB\oss_223_16"
dir2 = "D:\ToB_AlgoSpace\CMBWLB\oss_223_14"

#遍历文件
def walkFile(file):
    # for root,dirs,files in  os.walk(file):
    #     return files
    files = os.listdir(file)
    return files

#两个文件夹的文件相对应
def file_todic():
    file1_list = walkFile(dir1)
    file_dic = {}
    for f1 in file1_list:
        file_dic[os.path.join(dir1,f1)] = os.path.join(dir2,f1)
    return file_dic

#获取文件名传入比较
def get_tofilename(file_dic):
    for filename1,filename2 in file_dic.items():
        # print(filename1,filename2)
        #切割路径
        fn1 = filename1.split('\\')[-1]
        fn2 = filename2.split('\\')[-1]
        print(f"正在比较{fn1}->>>{fn2}")


        res_file1 = read_csv(filename1)
        res_file2 = read_csv(filename2)
        result = compare_csv(res_file1, res_file2)
        print(result)



def read_csv(filename):
    file = []
    #打开文件
    with open(filename, 'r') as f:
        #创建csv读取器
        reader = csv.reader(f)
        for row in reader:
            file.append(row)
        return file
    # a = open(filename)
    # b =


def compare_csv(res_file1, res_file2):
    error_msg = []
    j = 0
    if res_file1:
        if len(res_file1) == len(res_file2):
            for i in range(len(res_file1)):
                j+=1
                if operator.eq(res_file1[i], res_file2[i]):
                    continue
                else:
                    error_msg.append(f'第{j}行不同:{res_file1[i]}->>>{res_file2[i]}\n')
            if error_msg:
                return ''.join(error_msg)
            return '\t>>>数据校对一致'
        else:
            print("行数不一致")

if __name__ == '__main__':
    file_dic = file_todic()
    get_tofilename(file_dic)
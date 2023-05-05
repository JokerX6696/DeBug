#!/bin/env python3
import pandas as pd


id = 'DZOE2023031333-b1.xlsx'
df_test = pd.read_excel(id,sheet_name=0, engine = 'xlrd')
try:
    m=df_test["序号"]
except KeyError:
    report('ERROR','未能成功读取 {}，请添加一个空白sheet后重新尝试'.format(id))
else:
    print("添加参数 engine = 'xlrd' 后读取成功！")

print("#####################################################################################")
print("#########################   测试分隔符                     ###########################")
print("#####################################################################################")
df = pd.read_excel(id,sheet_name=0)
try:
    m=df["序号"]
except KeyError:
    report('ERROR','未能成功读取 {}，请添加一个空白sheet后重新尝试'.format(id))
else:
    print('未添加参数，读取成功！')

    
# 原始脚本
"""

#!/data/software/install/miniconda3/envs/python.3.7.0/bin/python3
########################################## import ################################################
import argparse, os, sys, re, random, glob
from datetime import datetime
import pandas as pd
bindir = os.path.abspath(os.path.dirname(__file__))
sys.path.append('/data/USER/liujiang/script/lib')
#import
#import pandas as pd
############################################ ___ #################################################
__doc__ = '该脚本可将OA2.0导出的样本测序编号表整理成各产品流程需要的 data.lst文件'
__author__ = 'Zhang Rongchao'
__mail__ = 'rongchao.zhang@oebiotech.com'
__date__ = '2022/11/24 14:26:56'
__version__ = '1.0.0'
############################################ main ##################################################
def report(level,info):
        date_now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        if level == "ERROR":
                sys.stderr.write("{0} - {1} - ERROR - \033[1;31;40m{2}\033[0m\n".format(date_now,os.path.basename(__file__),info))
                sys.exit(1)
        elif level == "INFO":
                sys.stdout.write("{0} - {1} - INFO - {2}\n".format(date_now,os.path.basename(__file__),info))
        elif level == "DEBUG":
                sys.stdout.write("{0} - {1} - DEBUG - {2}\n".format(date_now,os.path.basename(__file__),info))
                sys.exit(1)
        elif level == "WARNING":
                sys.stdout.write("{0} - {1} - WARNING - \033[1;33;40m{2}\033[0m\n".format(date_now,os.path.basename(__file__),info))
        return()

def check_file(file):
        if os.path.exists(file):
                return(os.path.abspath(file))
        else:
                info = "file does not exist: {0}".format(file)
                report("ERROR",info)

def check_dir(dir):
        dir = os.path.abspath(dir)
        if not os.path.exists(dir):
                os.system("mkdir -p {0}".format(dir))
                info = "mkdir: {0}".format(dir)
                report("INFO",info)
        return(dir)

def main():
        proj_type_list=['2bRAD','2bRAD-5','2bRAD-M','2bRAD-M-5','MethylRAD','Super-GBS','单细胞','宏基因组','免疫组库','重测序','普通转录组','多重PCR']
        parser=argparse.ArgumentParser(description=__doc__,
                formatter_class=argparse.RawTextHelpFormatter,
                epilog='author:\t{0}\nmail:\t{1}\ndate:\t{2}\nversion:\t{3}'.format(__author__,__mail__,__date__,__version__))
        parser.add_argument('-i',help='[必选] OA导出编号表，多个编号表使用英文逗号分割',dest='input',type=str,required=True)

        parser.add_argument('-o',help='[可选] 产生流程样本对应表，默认输出文件为 data.lst',dest='output',type=str,required=False,default='data.lst')
        parser.add_argument('-b',help='[可选] 对 完成确认流程编号 列进行筛选，多个编号使用英文逗号分割，默认读取列表全部。',dest='batch',type=str,required=False,default='all')
        parser.add_argument('-c',help='[可选] 对 任务单号 列进行筛选，多个单号使用英文逗号分割，默认读取列表全部。',dest='contract',type=str,required=False,default='all')
        parser.add_argument('-t',help='[可选] 对 项目类型 列进行筛选，多个类型使用英文逗号分割，默认读取列表全部。\n从 [{}] 中选择'.format(", ".join(proj_type_list)),dest='type',type=str,required=False,default='all')

#       parser.add_argument('-iddir',help='[默认] 任务单样本对应表存放目录，默认为 /storge1/fxrwd/bhb_id/*/',dest='iddir',type=str,required=False,default='/storge1/fxrwd/bhb_id/*/')
        parser.add_argument('-rawdata',help='[默认] 原始数据存放目录，默认为 /data/rawdata/*/',dest='rawdata',type=str,required=False,default='/data/rawdata/*/')
        args=parser.parse_args()
#       info = "runing..."
#       report('INFO','根据提供的任务单号查找任务单样本对应表')
#       try:
#               id=glob.glob('{}/{}_id*.xlsx'.format(args.iddir,args.id))[0]
#               report('INFO','已能成功获取任务单号 {} 的样本对应表为 {}'.format(args.id,id))
#       except:
#               report('ERROR','未能成功获取 {} 的样本对应表，请确认任务单存放目录 {} 是否存在该任务单号的对应表'.format(args.id,args.iddir))
        id_lst=args.input.split(',')
        batch_lst=args.batch.split(',')
        contract_lst=args.contract.split(',')
        type_lst=args.type.split(',')

        seq_plat=[]
        enzyme_lst=[]
        sample_name_lst=[]
        gbs_out_lst=[]
        iib_out_lst=[]
        cg_out_lst=[]

        for i in id_lst:
                id = check_file(i) #\033[1;33;40m
                report('INFO',"\033[1;32;40m读取文件 {}\033[0m".format(id))
                df=pd.read_excel(id,sheet_name=0) #读取第一个sheet
                #判断是否读进来，有时候读不进来
                try:
                        m=df["序号"]
                except KeyError:
                        report('ERROR','未能成功读取 {}，请添加一个空白sheet后重新尝试'.format(id))
                for row in df.index.values: #循环文件
                        #批次检查
#                       print(type(str(df.loc[row,"完成确认流程编号"])))
                        if str(df.loc[row,"完成确认流程编号"])=="nan": #跳过没有完成确认的行
                                report('WARNING','第 {} 行，完成确认流程编号为空，已跳过'.format(row+2))
                                continue
                        if batch_lst[0]!="all":
                                if df.loc[row,"完成确认流程编号"] not in batch_lst:
                                        report('WARNING','第 {} 行，完成确认流程编号为 {}，未在提供的参数 -b 列表中，已跳过。'.format(row+2,df.loc[row,"完成确认流程编号"]))
                                        continue

                        #任务单号检查
                        rwdh1=df.loc[row,"OA1.0任务单号"]
                        rwdh2=df.loc[row,"任务单号2.0"]
                        if contract_lst[0]!="all":
                                if rwdh1 not in contract_lst and rwdh2 not in contract_lst:
                                        report('WARNING','第 {} 行，任务单号1.0为 {}，任务单号OA2.0为 {}，未在提供的参数 -c 列表中，已跳过。'.format(row+2,rwdh1,rwdh2))
                                        continue

                        #项目类型检查
                        if type_lst[0]!="all":
                                if df.loc[row,"项目类型"] not in type_lst:
                                        report('WARNING','第 {} 行，项目类型为 {}，未在提供的参数 -t 列表中，已跳过。'.format(row+2,df.loc[row,"项目类型"]))
                                        continue

                        #检查文库和数据R1R2
                        #wk=df.loc[row,"编号"].split('-')[0] #取第一个字段，因为同文库有多个任务单号会有后缀-1，-2，-3
                        wk=df.loc[row,"编号"]
                        #检查文库是否存在，因为原文库补测的话，会重新走一遍流程，在 编号 列后边加后缀
                        try:
                                glob.glob('{}/{}*'.format(args.rawdata,wk))[0]
                        except:
                                report('ERROR','抱歉，第 {} 行，未能成功获取正确的下机文库 {}/{}*，可能是原文库补测导致 编号 列加了后缀，请与项目部确认后，删除该行'.format(row+2,args.rawdata,wk))
                        #检查数据R1R2
                        xuhao="-".join(df.loc[row,"序号"].split("-")[:3]) #取前三个字段，因为GBS同文库有多个任务单号会有后缀-1，-2，-3
                        try:
                                R1=glob.glob('{}/{}*/1.rawdata/*{}*1.f*q.gz'.format(args.rawdata,wk,xuhao))[0]
                        except:
                                report('ERROR','抱歉，第 {} 行，未能成功获取正确的下机数据，可能是下机数据命名有误，无法匹配到 R1 文件 {}/{}*/1.rawdata/*{}*1.f*q.gz，请联系数据管理员'.format(row+2,args.rawdata,wk,xuhao))
                        try:
                                R2=glob.glob('{}/{}*/1.rawdata/*{}*2.f*q.gz'.format(args.rawdata,wk,xuhao))[0]
                        except:
                                report('ERROR','抱歉，第 {} 行，未能成功获取正确的下机数据，可能是下机数据命名有误，无法匹配到 R2 文件 {}/{}*/1.rawdata/*{}*2.f*q.gz，请联系数据管理员'.format(row+2,args.rawdata,wk,xuhao))
        #               R1="R1"
        #               R2="R2"

                        pt=df.loc[row,"项目类型"] #项目类型

                        bz=df.loc[row,"备注"] #备注
                        bz_lst=bz.split(";") #测序平台;样本分析名称;酶;循环数;研发编号
                        seq_plat.append(bz_lst[0]) #测序平台
                        index_name_lst=bz_lst[1].split("|") #index:样本分析名
                        enzyme_lst.append(bz_lst[2]) #酶
                        report('INFO','读取第 {} 行，项目类型为 {}，R1数据文件为 {}，R2数据文件为 {}，共有 {} 个样本。'.format(row+2,pt,R1,R2,len(index_name_lst)))
                        if pt=="Super-GBS": #SuperGBS建库data lst：
                                index_lst=[] #检查同一个文库index是否有重复
                                for i in index_name_lst: #index:样本分析名
                                        index,sample_name=i.split(":") #index 和 样本分析名称
                                        if sample_name=="nan":
                                                report('WARNING','文件第 {} 行，index {} 对应的样本分析名为 {}，为不分析样本，请注意核实。'.format(row+2,index,sample_name))
                                                continue
                                        sample_name_lst.append(sample_name)
                                        index_lst.append(index)
                                        line=f'{sample_name}\t{index}\t{R1}\t{R2}'
                                        gbs_out_lst.append(line)
                                #检查样本index是否有重复
                                if len(index_lst)!=len(set(index_lst)):
                                        report('ERROR','文件第 {} 行，备注列样本 index 有重复，请联系实验室核查'.format(row+2))
                        elif pt=="2bRAD-5" or pt=="2bRAD-M-5": #2brad 五标签建库data lst：R1<tab>R2<tab>样本分析名1<tab>样本分析名2<tab>样本分析名3<tab>样本分析名4<tab>样本分析名5
                                line=f'{R1}\t{R2}'
                                index_lst=[] #检查同一个文库index是否有重复
                                for i in index_name_lst: #index:样本分析名
                                        index,sample_name=i.split(":") #index 和 样本分析名称
                                        if sample_name=="nan": #不分析样本
                                                report('WARNING','文件第 {} 行，index {} 对应的样本分析名为 {}，为不分析样本，请注意核实。'.format(row+2,index,sample_name))
                                                sample_name="-"
                                                line+=f'\t{sample_name}'
                                        else:
                                                sample_name_lst.append(sample_name)
                                                index_lst.append(index)
                                                line+=f'\t{sample_name}'
                                iib_out_lst.append(line)
                                if len(index_lst)!=len(set(index_lst)):
                                        report('ERROR','文件第 {} 行，备注列样本 index 有重复，请联系实验室核查'.format(row+2))
                        elif pt=="2bRAD" or pt=="2bRAD-M": #2brad 单标签建库data lst：R1<tab>R2<tab>样本分析名
                                sample_name_lst.append(bz_lst[1])
                                line=f'{R1}\t{R2}\t{bz_lst[1]}'
                                iib_out_lst.append(line)
                        elif pt=="单细胞" or pt=="MethylRAD" or "重测序" or "普通转录组" or "免疫组库" or "宏基因组" or "多重PCR": #常规data lst：样本分析名<tab>R1<tab>R2
                                sample_name_lst.append(bz_lst[1])
                                line=f'{bz_lst[1]}\t{R1}\t{R2}'
                                cg_out_lst.append(line)
                        else:
                                report('INFO',"读取第 {} 行，项目类型为 {}，不属于约定好的项目类型，请核查。".format(row+2,pt))
        report('INFO',"\033[1;32;40m所有编号表已读取完成\033[0m")
        #检查测序平台是否唯一
        if len(set(seq_plat))!=1:
                report('ERROR','测序平台不唯一，分别为 {}'.format(set(seq_plat)))
        #检查酶切位点是否唯一
        if len(set(enzyme_lst))!=1:
                report('ERROR','实验所用的酶不唯一，分别为 {}'.format(set(enzyme_lst)))

        num=0
        report('INFO','该项目共送测 \033[1;33;40m{}\033[0m 个样本，去除重复后共计 \033[1;33;40m{}\033[0m 个样本'.format(len(sample_name_lst),len(set(sample_name_lst))))
        report('INFO','输出项目样本对应表文件 \033[1;33;40m{}\033[0m'.format(os.path.abspath(args.output)))
        with open(args.output,'w') as OUT:
                OUT.write('#sequencing platform: {}\n'.format(seq_plat[0]))
                OUT.write('#enzyme: {}\n'.format(enzyme_lst[0]).replace('酶:',''))
                if gbs_out_lst:
                        OUT.write('#uniq_id\tsample_name\tindex\tR1_path\tR2_path\n')
                        for i in gbs_out_lst:
                                num+=1
                                OUT.write('supergbs_uniqid_{}\t{}\n'.format(num,i))
                if iib_out_lst:
                        OUT.write('#no\tR1_path\tR2_path\tsample1\tsample2\tsample3\tsample4\tsample5\n')
                        for i in iib_out_lst:
                                num+=1
                                a=''
                                if num<10:
                                        a=f'00{num}'
                                elif 10<=num<100:
                                        a=f'0{num}'
                                else:
                                        a=num
                                OUT.write(f'{a}\t{i}\n')
                if cg_out_lst:
                        OUT.write('#sample_name\tR1_path\tR2_path\n')
                        for i in cg_out_lst:
                                OUT.write(f'{i}\n')
        report('INFO',"格式已转换完成")




if __name__=="__main__":
        main()
        info = "finish!"
#       report("INFO",info)






"""

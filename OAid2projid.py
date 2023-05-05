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

# -*- coding: utf-8 -*-
"""
Created on Tue Jun 23 11:39:34 2020

@author: Ray
"""

import re
import sys
import pymysql
import logging
import argparse
import numpy as np
import pandas as pd
from warnings import filterwarnings

#--------------------------------------------------------------------------报错日志-----------------------------------------------------------------------------------------------#

logging.basicConfig(filename = 'load.log', level=logging.DEBUG, format = "%(levelname)s %(asctime)s:%(name)s:%(message)s", datefmt = '%Y-%m-%d', filemode = 'w')
filterwarnings("error", category = pymysql.Warning) # 捕获mysql错误信息。


#--------------------------------------------------------------------------参数设置-----------------------------------------------------------------------------------------------#

parser = argparse.ArgumentParser(formatter_class = argparse.RawDescriptionHelpFormatter, description = '''This programe is used to load data to mysql automatically''')

parser.add_argument('-data', action = 'store', help = 'TAB delimited file', required = True) # 指定数据源或数据地址
parser.add_argument('-database', help = 'Database name.', required = True) # 指定存储得库名
parser.add_argument('-tableName', help = 'Table name.', required = True) # 指定存储的表名
parser.add_argument('-dropTable', help = 'If exists table, drop table', required = False, default = False, type = bool) # 如果建表时存在相同表名是否删除
parser.add_argument('-primaryKey', nargs = '+', help = 'PrimaryKey column.(Example format: ID or ID Pos)', required = False, default = None) # 设定的主键
parser.add_argument('-index', nargs = '+', help = 'Index column.(Example format: MAF or MAF Pos)', required = False, default = None) # 指定索引字段（默认为空）
parser.add_argument('-indexUnion', help = 'Pass indexUnion dictionary.(Example format: MAF-Pos or MAF-Pos,Chrom-Gene)', required = False, default = None) # 指定了联合索引字段（默认为空)
parser.add_argument('-host', help = 'Host name.', required = True) # 主机名或IP地址
parser.add_argument('-user', help = 'User name.', required = True) # 用户名
parser.add_argument('-password', help = 'Password', required = True) # 密码
args = parser.parse_args()    
wide_connect = None
wide_cursor = None
typeList = [] # 建表语句
priDict = dict() # 定义主键（）
        
#--------------------------------------------------------------------------数据导入-----------------------------------------------------------------------------------------------#

# 判断原始数据是否含有重复名
with open(args.data, 'r') as data:
    for line in data:
        li = line.strip().split('\t')
        break
        
set_lst = set(li)
        
if len(set_lst) == len(li):
    pass    
else:
    logging.warning("Duplicate column names.")
    sys.exit()
    
data = pd.read_table(args.data, sep = '\t', low_memory = False)
if len(data.columns) < 2: # 列名太少
    logging.debug("表格少于两列")
    sys.exit()

# 数据规范
'''
if data.isnull().any().any() == True: # 判断是否存在null值
    for line, j in zip(data.values, range(len(data.values))): # 输出null的位置
        for i in line:
            if pd.isnull(i):
                logging.error("The NULL in the {}".format(j + 1))
                sys.exit()  
 
if data.isna().any().any() == True:
    for line, j in zip(data.values, range(len(data.values))): # 输出na的位置
        for i in line:
            if pd.isna(i):
                logging.error("The NaN in the {}".format(j + 1))
                sys.exit()
'''

if set(['MAF']).issubset(data.columns): # 判断是否存在MAF列，判断是否全小于0.5
    MAF_max = max(i for i in data['MAF'])
    if MAF_max > 0.5:
        logging.error("MAF must be less than 0.5")
        sys.exit()

if set(['Gene']).issubset(data.columns): # 如果用户数据有Gene列，判断其格式
    for i ,j in zip(data['Gene'], range(len(data['Gene']))):
        if re.search(':', i) != None:
            logging.error("The bug in the {} line of Gene column".format(j + 1))
            sys.exit()
        else:
            continue

#--------------------------------------------------------------------------MySQL链接----------------------------------------------------------------------------------------------#

wide_connect = pymysql.connect(host = args.host,
                               user = args.user,
                               password = args.password,
                               port = 3306, charset = 'utf8',
                               local_infile = True) # ***loacl_infile允许导入本地文件***
wide_cursor = wide_connect.cursor()

#--------------------------------------------------------------------------数据类型-----------------------------------------------------------------------------------------------#
      
# 针对整数型数值定义其格式-----------------------------------------------------------------------------------------------------------------------------------------------------------
int_fun = lambda x: 'TINYINT({})'.format(len(str(x))) if -128 <= x <= 127\
            else 'SMALLINT({})'.format(len(str(x))) if -32768<= x <= 32767\
            else 'MEDIUMINT({})'.format(len(str(x))) if -8388608 <= x <= 8388607\
            else 'INT({})'.format(len(str(x))) if -2147483648 <= x <= 2147483647\
            else 'BIGINT({})'.format(len(str(x)))

int_fun_unsigned = lambda x: 'TINYINT({}) UNSIGNED '.format(len(str(x))) if x <= 255\
            else 'SMALLINT({}) UNSIGNED '.format(len(str(x))) if x <= 65535\
            else 'MEDIUMINT({}) UNSIGNED '.format(len(str(x))) if x <= 16777215\
            else 'INT({}) UNSIGNED '.format(len(str(x))) if x <= 4294967295\
            else 'BIGINT({}) UNSIGNED '.format(len(str(x)))
        
              
# 针对浮点型数据定义其格式-----------------------------------------------------------------------------------------------------------------------------------------------------------
def float_fun(column):
    flo = [str(i).split('.') for i in data[column] if pd.notnull(i)]
    m = max([len(i[0]) for i in flo if len(i) >= 2]) # 整数位最大长度
    d = max([len(i[1]) for i in flo if len(i) >= 2]) # 小数位最大长度
    return 'FLOAT ({m}, {d})'.format(m = m + d + 2, d = d + 2)

#--------------------------------------------------------------------------建表语句-----------------------------------------------------------------------------------------------#
typeList = []
for column in data.columns:
    # 如果字段是整数型
    if data[column].dtypes in [np.dtype('int16'), np.dtype('int32'), np.dtype('int64'), np.dtype('uint16'), np.dtype('uint32'), np.dtype('uint64')]:
        if -1 in np.sign(data[column]):
            int_max = data[column].max() # 整数长度
            typeList.append("`{}`".format(column) + int_fun(int_max) + "DEFAULT NULL")
            continue
        else:
            int_max = data[column].max() # 整数长度
            typeList.append("`{}`".format(column) + int_fun_unsigned(int_max) + "DEFAULT NULL")
            continue
    # 如果字段是浮点型
    if data[column].dtype in [np.dtype('float16'), np.dtype('float32'), np.dtype('float64')]:
        typeList.append("`{}`".format(column) + float_fun(column) + "DEFAULT NULL")
        # 如果字段是文本
    else:
        str_len = max([len(str(i)) for i in data[column] if pd.notnull(i)])
        typeList.append("`{}`".format(column) + "VARCHAR({})".format(str_len) + "DEFAULT NULL")
    
# 如果定义了主键，则该字段默认不为空----------------------------------------------------------------------------------------------------------------------------------------------------
if args.primaryKey != None:
	for column, c in zip(data.columns, range(len(data.columns))):
    		if column in args.primaryKey:
        		typeList[c] = typeList[c].replace('DEFAULT NULL', 'NOT NULL')
        		priDict[column] = column
                    
#----------------------------------------------------------------------------建表-------------------------------------------------------------------------------------------------#

# 指定主键（我们一般定义ID为主键）
if args.primaryKey != None:
    if len(args.primaryKey) == 1: # 如果定义一个键是主键
        primaryKey_statement = ["PRIMARY KEY (`{}`)".format(col) for col in data.columns if col in args.primaryKey][0]
    else:
        primaryKey_statement = "PRIMARY KEY ({})".format(','.join(['`' + col + '`'for col in data.columns if col in args.primaryKey]))
        

# 指定索引
if args.index != None and args.indexUnion != None: # 如果创建联合索引，在将args.index中的每个元素单独创建索引的同时，将args.indexUnion中的每一个元素创建索引
    index_statement = ','.join(["KEY `{}` (`{}`)".format(col, col) for col in data.columns if col in args.index])
    index_statement =  index_statement + ',' + ','.join(["KEY `{}` ({})".format(index, ','.join('`' + k + '`' for k in index.split('-'))) for index in args.indexUnion.split(',')])
elif args.index != None and args.indexUnion == None: # 如果不创建联合索引，将self.key中的每个元素创建索引
    index_statement = ','.join(["KEY `{}` (`{}`)".format(col, col) for col in data.columns if col in args.index])
elif args.index == None and args.indexUnion != None:
    index_statement = ','.join(["KEY `{}` ({})".format(index, ','.join('`' + k + '`' for k in index.split('-'))) for index in args.indexUnion.split(',')])
    
# 执行建表语句----------------------------------------------------------------------------------------------------------------------------------------------------------------------
wide_cursor.execute("USE {};".format(args.database)) # 选择数据库

try:
    if args.dropTable: # 是否删除已经存在的表
        wide_cursor.execute("DROP TABLE IF EXISTS {};".format(args.tableName))
except (pymysql.err.Warning, pymysql.err.Error) as e:
    logging.debug(str(e.args[0]) + ":" + e.args[1])
    
try:
    if 'primaryKey_statement' in dir() and 'index_statement' in dir(): 
        wide_cursor.execute("CREATE TABLE IF NOT EXISTS `{table_}`(\
                            {data_demand_statement_},\
                            {primary_key_},\
                            {index_statement_}) ENGINE=MyISAM DEFAULT CHARSET=utf8;".\
                            format(table_ = args.tableName, # 表名
                                   data_demand_statement_ = ','.join(typeList), # 字段格式语句
                                   primary_key_ = primaryKey_statement, # 主键语句
                                   index_statement_ = index_statement # 索引语句
                                   )
                            )
    elif 'primaryKey_statement' not in dir() and 'index_statement' in dir():
        wide_cursor.execute("CREATE TABLE IF NOT EXISTS `{table_}`(\
                            {data_demand_statement_},\
                            {index_statement_}) ENGINE=MyISAM DEFAULT CHARSET=utf8;".\
                            format(table_ = args.tableName, # 表名
                                   data_demand_statement_ = ','.join(typeList), # 字段格式语句
                                   index_statement_ = index_statement # 索引语句
                                   )
                            )
    elif 'primaryKey_statement' in dir() and 'index_statement' not in dir():
        wide_cursor.execute("CREATE TABLE IF NOT EXISTS `{table_}`(\
                            {data_demand_statement_},\
                            {primary_key_}) ENGINE=MyISAM DEFAULT CHARSET=utf8;".\
                            format(table_ = args.tableName, # 表名
                                   data_demand_statement_ = ','.join(typeList), # 字段格式语句
                                   primary_key_ = primaryKey_statement # 索引语句
                                   )
                            )
    else:
         wide_cursor.execute("CREATE TABLE IF NOT EXISTS `{table_}`(\
                         {data_demand_statement_}) ENGINE=MyISAM DEFAULT CHARSET=utf8;".\
                            format(table_ = args.tableName, # 表名
                                   data_demand_statement_ = ','.join(typeList), # 字段格式语句
                                   )
                            )
except (pymysql.err.Warning, pymysql.err.Error) as e:
    logging.debug(str(e.args[0]) + ":" + e.args[1])

    
        
#----------------------------------------------------------------------------存储-------------------------------------------------------------------------------------------------#
wide_cursor.execute("USE {}".format(args.database))

# 导入数据表语句------------------------------------------------------------------------------------------------------------------------------------------------------------
try:
    wide_cursor.execute("LOAD DATA LOCAL INFILE '{}' INTO TABLE {}.{} FIELDS TERMINATED BY '\\t' IGNORE 1 LINES".format(args.data, args.database, args.tableName))
except (pymysql.err.Warning, pymysql.err.Error) as e:
    logging.warning(str(e.args[0]) + ":" + e.args[1])
    try:
        wide_cursor.execute("DROP TABLE IF EXISTS {};".format(args.tableName))
    except (pymysql.err.Warning, pymysql.err.Error) as e:
        logging.warning(str(e.args[0]) + ":" + e.args[1])
    
wide_connect.commit() # 数据提交
wide_cursor.close() # 关闭链接


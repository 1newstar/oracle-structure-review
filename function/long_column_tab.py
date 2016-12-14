#!/usr/bin/python
# -*- coding: utf-8 -*-
"""************************************************************
=====编程规范=====

1.变量命名
    全局变量    GLOBAL_VARIABLE
    程序变量    v_variable
    局部变量    l_variable
    形式参数    p_variable
    
2.规则函数
    命名      f_rule_xxx()
    参数      p_parms list

=====数据库配置文件=====
[ip/domain]
server_ip   = 127.0.0.1
db_user     = testpuser
db_pwd      = testpwd
db_name     = test

=====规则配置文件=====
[rule1]
name        = LONG_COLUMN_TAB                                 #规则名称
description = 表中字段长度记录的字节数超过指定阀值            #规则描述
parm1       = 2                                               #输入参数1-分区数量
parm2       ...                                               #输入参数2...
threshold   = 0.1                                             #扣分阀值
max_value   = 10                                              #扣分上限
status      = ON/OFF                                          #规则状态
title1      = 表名                                            #返回数据标题1
title2      = 行记录定义的字节数                              #返回数据标题2
title3      = 行记录实际平均存储的字节                        #返回数据标题3

************************************************************"""

import pprint
import cx_Oracle

"""************************************************************
名称
    f_rule_long_column_tab
描述
    获得数据库指定用户表中行记录字节数并计算扣分
参数
    p_parms        list                 参数列表
    p_parms[0]     l_dbinfo             list    数据库连接信息
    p_parms[1]     l_username           string  数据库用户(要大写)
    p_parms[2]     l_long_col_tab       int     行记录定义与行实际平均存放字节占比
    p_parms[3]     l_threshold          int     扣分阀值
    p_parms[4]     l_max_value          int     扣分上限
返回值
    dict{'scores':xxx,'records':xxx}
    'scores'    float   扣分
    'records'   list    单表信息(表名,行记录定义字节数,记录实际平均存放字节数)
示例
    v_dbinfo=["localhost","1521","testdb","user1","123456"]
    v_parms=[v_dbinfo,'USER1',2,0.1,10]
    v_result = f_rule_long_column_tab(v_parms)
说明
    函数调用中,有几个参数调用顺序是固定的。分别如下:
    parm1       数据库连接信息
    parm2       数据库用户
    parm3...    规则配置中的参数部分parm
    parm(n-1)   扣分阀值,对应规则配置中的threshold
    parm(n)     扣分上限,对应规则配置中的max_value
************************************************************"""
def f_rule_long_column_tab(p_parms):
    [l_dbinfo,l_username,l_long_col_tab,l_weight,l_max_value]=p_parms
    l_return_stru={"scores":0,"records":[]}
    conn=cx_Oracle.connect(l_dbinfo[3]+'/'+l_dbinfo[4]+'@'+l_dbinfo[0]+':'+l_dbinfo[1]+'/'+l_dbinfo[2])
    cursor = conn.cursor()
    cursor.execute("""select case userenv('language') 
    when 'SIMPLIFIED CHINESE_CHINA.AL32UTF8' then 3
    when 'AMERICAN_AMERICA.AL32UTF8' then 3
    when 'SIMPLIFIED CHINESE_CHINA.ZHS16GBK' then 4
    when 'AMERICAN_AMERICA.ZHS16GBK' then 4
    end
from dual
""")
    rec = cursor.fetchall()
    v_len = rec[0][0]
    
    cursor = conn.cursor()
    cursor.execute("""
    select t.table_name, a.col_sum, t.avg_row_len
      from dba_tables t,
       (select table_name, sum(length) col_sum
          from (select table_name,
                       data_length,
                       column_name,
                       sum(case data_type
                             when 'VARCHAR2' then
                              round(data_length / """+str(v_len)+""", 2)
                             when 'VARCHAR' then
                              round(data_length / """+str(v_len)+""", 2)
                             else
                              data_length
                           end) length
                  from dba_tab_cols
                 where owner='"""+l_username+"""'
                 group by table_name, data_length, column_name) t
         group by table_name) a
     where t.owner='"""+l_username+"""'
     and t.table_name = a.table_name
     and t.avg_row_len / a.col_sum < """+str(l_long_col_tab))
    records = cursor.fetchall()    
    l_return_stru["records"]=records

    if (len(records)*float(l_weight))>float(l_max_value):
        l_return_stru["scores"]=float('%0.2f' %float(l_max_value))
    else:
        l_return_stru["scores"]=float('%0.2f' %(len(records)*float(l_weight)))

    cursor.close()
    
    conn.close()
    return l_return_stru
    
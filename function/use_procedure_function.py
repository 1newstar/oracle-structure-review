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
    命名      f_rule_procedures_num()
    参数      p_parms list

=====数据库配置文件=====
[ip/domain]
server_ip   = 127.0.0.1
db_user     = testpuser
db_pwd      = testpwd
db_name     = test

=====规则配置文件=====
[rule1]
name        = procedures_num                    #规则名称
description = 使用存储和函数                    #规则描述
threshold   = 0.5                               #扣分阀值
max_value   = 10                                #扣分上限
status      = ON/OFF                            #规则状态
title1      =存储和函数的名字                   #返回数据标题1
************************************************************"""

import pprint
import cx_Oracle

"""************************************************************
名称
    f_rule_procedures_num
描述
    获得数据库指定用户的存储和函数名称并计算扣分
参数
    p_parms        list    参数列表
    p_parms[0]     l_dbinfo        list    数据库连接信息
    p_parms[1]     l_username      string  数据库用户(要大写)
    p_parms[2]     l_threshold     int     扣分阀值
    p_parms[3]     l_max_value     int     扣分上限
返回值
    dict{'scores':xxx,'records':xxx}
    'scores'    float   扣分
    'records'   list    存储信息(存储名)
示例
    v_dbinfo=["localhost","1521","xe","hf","123"]
    v_parms=[v_dbinfo,'HF',0.1,1]
    v_result = f_rule_produces_num(v_parms)
说明
    函数调用中,有几个参数调用顺序是固定的。分别如下:
    parm1       数据库连接信息
    parm2       数据库用户
    parm3...    规则配置中的参数部分parm
    parm(n-1)   扣分阀值,对应规则配置中的threshold
    parm(n)     扣分上限,对应规则配置中的max_value
************************************************************"""
def f_rule_use_procedure_function(p_parms):
    [l_dbinfo,l_username,l_weight,l_max_value]=p_parms
    l_return_stru={"scores":0,"records":[]}
    conn=cx_Oracle.connect(l_dbinfo[3]+'/'+l_dbinfo[4]+'@'+l_dbinfo[0]+':'+l_dbinfo[1]+'/'+l_dbinfo[2])
    cursor = conn.cursor()
    cursor.execute("""select object_name,object_type from dba_procedures where object_type in ('PROCEDURE','FUNCTION') and owner='"""+l_username+"""' order by object_type""" )
    records = cursor.fetchall()
    l_return_stru["records"]=records

    
    if (len(records)*float(l_weight))>float(l_max_value):
        l_return_stru["scores"]=float('%0.2f' %float(l_max_value))
    else:
        l_return_stru["scores"]=float('%0.2f' %(len(records)*float(l_weight)))
    
    cursor.close()
    conn.close()
    return l_return_stru
    
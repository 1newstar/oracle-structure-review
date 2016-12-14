#!/usr/bin/python
# -*- coding: utf-8 -*-

import cx_Oracle

"""************************************************************
名称
    f_rule_timestamp
描述
    获得数据库指定用户没有创建时间戳字段的表
参数
    p_parms        list    参数列表
    p_parms[0]     l_dbinfo        list    数据库连接信息
    p_parms[1]     l_username      string  数据库用户(要大写)
    p_parms[2]     l_weight        int     扣分阀值
    p_parms[3]     l_max_value     int     扣分上限
返回值
    dict{'scores':xxx,'records':xxx}
    'scores'    float   扣分
    'records'   list    表名
示例
    v_dbinfo=["localhost","1521","xe","hf","123"]
    v_parms=[v_dbinfo,'HF',0.1,1]
    v_result = f_rule_timestamp(v_parms)
说明
    函数调用中,有几个参数调用顺序是固定的。分别如下:
    parm1       数据库连接信息
    parm2       数据库用户
    parm3...    规则配置中的参数部分parm
    parm(n-1)   扣分阀值,对应规则配置中的weight
************************************************************"""
def f_rule_timestamp(p_parms):
    [l_dbinfo,l_username,l_weight,l_max_value]=p_parms
    l_return_stru={"scores":0,"records":[]}
    conn=cx_Oracle.connect(l_dbinfo[3]+'/'+l_dbinfo[4]+'@'+l_dbinfo[0]+':'+l_dbinfo[1]+'/'+l_dbinfo[2])
    cursor = conn.cursor()
    cursor.execute("""select table_name from dba_tables where owner='"""+l_username+"""' and table_name not in ( select table_name from dba_tab_cols where owner='"""+l_username+"""' and (column_name like 'CREATE%' or column_name like 'UPDATE%') and data_type='DATE')""" )
    records = cursor.fetchall()    
    l_return_stru["records"]=records
    
    if (len(records)*float(l_weight))>float(l_max_value):
        l_return_stru["scores"]=float('%0.2f' %float(l_max_value))
    else:
        l_return_stru["scores"]=float('%0.2f' %(len(records)*float(l_weight)))

    cursor.close()
    conn.close()
    return l_return_stru
    
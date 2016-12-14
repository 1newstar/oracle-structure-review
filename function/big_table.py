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
name        = BIG_TAB                           #规则名称
description = 单表规模超过指定阀值且没有分区    #规则描述
parm1       = 1                                 #输入参数1-表大小(GB)
parm2       ...                                 #输入参数2...
threshold   = 0.1                               #扣分阀值
max_value   = 10                                #扣分上限
status      = ON/OFF                            #规则状态
title1      = 表名称                            #返回数据标题1
title2      = 表大小                            #返回数据标题2
************************************************************"""

import pprint
import cx_Oracle

"""************************************************************
名称
    f_rule_big_tab
描述
    获得数据库指定用户的大表并计算扣分
参数
    p_parms        list            参数列表
    p_parms[0]     l_dbinfo        list    数据库连接信息
    p_parms[1]     l_username      string  数据库用户(要大写)
    p_parms[2]     l_table_size    int     表的大小(单位是G)
    p_parms[3]     l_threshold     int     扣分阀值
    p_parms[4]     l_max_value     int     扣分上限
返回值
    dict{'scores':xxx,'records':xxx}
    'scores'    float   扣分
    'records'   list    大表信息(表名,大小)
示例
    v_dbinfo=["localhost","1521","schema","user1","123456"]
    v_parms=[v_dbinfo,'user1',0,0.1,10]
    v_result = f_rule_big_tab(v_parms)
说明
    函数调用中,有几个参数调用顺序是固定的。分别如下:
    parm1       数据库连接信息
    parm2       数据库用户
    parm3...    规则配置中的参数部分parm
    parm(n-1)   扣分阀值,对应规则配置中的threshold
    parm(n)     扣分上限,对应规则配置中的max_value
************************************************************"""
def f_rule_big_table(p_parms):
    [l_dbinfo,l_username,l_table_size,l_weight,l_max_value]=[p_parms[0],p_parms[1],int(p_parms[2]),int(p_parms[3]),int(p_parms[4])]

    l_return_stru={"scores":0,"records":[]}
    conn=cx_Oracle.connect(l_dbinfo[3]+'/'+l_dbinfo[4]+'@'+l_dbinfo[0]+':'+l_dbinfo[1]+'/'+l_dbinfo[2])
    cursor = conn.cursor()
    cursor.execute("""
        select segment_name,round(bytes/1024/1024/1024,2)
        from dba_segments 
        where segment_type ='TABLE'
        and owner='"""+l_username+"""'
        and bytes>"""+str(l_table_size*1024*1024*1024) +
        """union
        select t.segment_name||':'||s.PARTITION_NAME,round(bytes/1024/1024/1024,2)
        from  dba_tab_PARTITIONS s,dba_segments t
        where t.partition_name=s.partition_name
        and s.SUBPARTITION_COUNT='0'
        and owner='"""+l_username+"""'
        and bytes>"""+str(l_table_size*1024*1024*1024) +
        """union
        select t.segment_name || ':' || s.partition_name || ':' || t.partition_name,round(t.bytes / 1024 / 1024 / 1024, 2)
        from dba_segments t, dba_tab_subpartitions s
        where t.partition_name = s.subpartition_name
        and t.segment_type = 'TABLE SUBPARTITION'
        and t.owner='"""+l_username+"""'
        and t.bytes>"""+str(l_table_size*1024*1024*1024) +
        """union
        select s.table_name || ' : lob_column : ' || s.column_name,round(t.bytes / 1024 / 1024 / 1024, 2)
        from dba_lobs s, dba_segments t
        where s.owner = '"""+l_username+"""'
        and t.owner = '"""+l_username+"""'
        and s.segment_name = t.segment_name
        and t.bytes>"""+str(l_table_size*1024*1024*1024))
    records = cursor.fetchall()
    l_return_stru["records"]=records

    if (len(records)*float(l_weight))>float(l_max_value):
        l_return_stru["scores"]=float('%0.2f' %float(l_max_value))
    else:
        l_return_stru["scores"]=float('%0.2f' %(len(records)*float(l_weight)))

    cursor.close()
    conn.close()
    return l_return_stru
"""
if __name__ == "__main__":
    v_dbinfo=["127.0.0.1","1521","xe","testuser","testpwd"]
    v_username='HF'
    v_parms=[v_dbinfo,v_username,'2','1','10']
    v_function="f_rule_big_table"
    v_result = eval(v_function)(v_parms)
    pprint.pprint(v_result)
"""
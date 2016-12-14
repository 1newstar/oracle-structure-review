#!/usr/bin/python
# -*- coding: utf-8 -*-"""

###************************************************************
###AUTHOR:CRIS ZHENG 20150427
###************************************************************

import cx_Oracle

"""************************************************************
名称
    f_combined_index_percent
描述
    获得数据库指定用户的组合索引数量占总索引数量的百分比并计算扣分
参数
    p_parms        list    参数列表
    p_parms[0]     l_dbinfo        list    数据库连接信息
    p_parms[1]     l_username      string  数据库用户(要大写)
    p_parms[2]     l_combined_index_percent_min     int     指定组合索引占比高于此值开始扣分
    p_parms[3]     l_threshold     int     扣分阀值
    p_parms[4]     l_max_value     int     扣分上限
返回值
    dict{'scores':xxx,'records':xxx}
    'scores'    float   扣分
    'records'   list    索引类型分组数量(索引类型,数量)
示例
    v_dbinfo=["localhost","1521","xe","hf","123"]
    v_parms=[v_dbinfo,'HF',0.1,10]
    v_result = f_combined_index_percent(v_parms)
************************************************************"""
def f_rule_combined_index_percent(p_parms):
    
     ###传进来的数据库连接信息和阈值等信息付给函数本地变量
    [l_dbinfo,l_username,l_combined_index_percent_min,l_threshold,l_max_value]=p_parms

    ###返回值初始化
    l_return_stru={"scores":0,"records":[]}

    ###sql变量，返回传入用户的组合索引数量和所有索引数量
    l_sql_comindnum_allindnum = """SELECT 'COMBINEINDEX', COUNT(DISTINCT IC.INDEX_NAME) AS COMBINEINDEXNUMBER
                                     FROM DBA_IND_COLUMNS IC
                                    WHERE IC.INDEX_OWNER = '"""+l_username+"""'
                                      AND IC.COLUMN_POSITION > 1
                                     UNION ALL
                                   SELECT 'ALLINDEX', COUNT(1) 
                                     FROM DBA_INDEXES I 
                                    WHERE I.OWNER = '"""+l_username+"""'"""
    conn=cx_Oracle.connect(l_dbinfo[3]+'/'+l_dbinfo[4]+'@'+l_dbinfo[0]+':'+l_dbinfo[1]+'/'+l_dbinfo[2])
    cursor = conn.cursor()
    cursor.execute(l_sql_comindnum_allindnum)
    records_comindnum_allinnum = cursor.fetchall()

    ###取组合索引数量和所有索引数量并赋值给本地变量
    for i in records_comindnum_allinnum:
        if i[0] == 'COMBINEINDEX':
            l_comind_num = i[1]
        if i[0] == 'ALLINDEX':
            l_allind_num = i[1]

    ###如果所有索引数量为0，则直接返回，并扣除本规则所有分数。
    if l_allind_num == 0:
        l_return_stru["scores"] = float('%0.2f' %float(l_max_value))
        l_return_stru["records"] = []
        return l_return_stru

    ###计算组合索引占比
    l_comind_percent = ( l_comind_num * 100/ l_allind_num )
    
    ###如果组合索引占比小于等于传入最小值，则不扣分，如果大于则与其做差乘以系数为所扣分，如果所扣分大于最大扣分上限，则返回最大上限值
    if l_comind_percent <= float(l_combined_index_percent_min):
        l_return_stru["scores"] = 0.0
    elif (l_comind_percent - float(l_combined_index_percent_min)) * float(l_threshold) > float(l_max_value):
        l_return_stru["scores"] = float('%0.2f' %float(l_max_value))
    else:
        l_return_stru["scores"] = float('%0.2f' %((l_comind_percent - l_combined_index_percent_min) * float(l_threshold)))
  
    ###sql变量，返回所有组合索引的表名，索引名，列名和列顺序
    l_sql_comind_list = """SELECT I.TABLE_NAME, I.INDEX_NAME, I.COLUMN_NAME, I.COLUMN_POSITION
                             FROM DBA_IND_COLUMNS I
                             JOIN (SELECT DISTINCT IC.INDEX_OWNER, IC.INDEX_NAME
                                     FROM DBA_IND_COLUMNS IC
                                    WHERE IC.COLUMN_POSITION > 1
                                      AND IC.INDEX_OWNER = '"""+l_username+"""') A
                               ON I.INDEX_OWNER = A.INDEX_OWNER
                              AND I.INDEX_NAME = A.INDEX_NAME
                         ORDER BY I.TABLE_NAME, I.INDEX_NAME, I.COLUMN_POSITION"""

    ###取所有组合索引信息
    cursor.execute(l_sql_comind_list)
    l_return_stru["records"] = cursor.fetchall()

    ###关闭数据库游标和连接
    cursor.close()
    conn.close()
    return l_return_stru
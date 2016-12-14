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
name        = COL_WRONG_TYPE                    #规则名称
description = 表字段类型不匹配                  #规则描述
parm1       = 10000                             #输入参数1-表记录数阈值，少于该值的小表全表提取做匹配
parm2       = 20                                #输入参数2-表字段非重复值阈值，用来排除枚举字段
parm3       = 200                               #输入参数3-大表采样提取记录数上限值
weight      = 0.1                               #扣分权重
max_value   = 10                                #扣分上限
status      = ON/OFF                            #规则状态
title1      = 表名称                            #返回数据标题1
title2      = 表字段名                          #返回数据标题2 
title3      = 字段定义类型                      #返回数据标题3
title4      = 字段实际类型                      #返回数据标题4
************************************************************"""
import re
import pprint
import cx_Oracle

def f_rule_col_wrong_type(p_parms):
    pat_date_c  = re.compile('((((19|20)?\d{2})[-/]*((1[0-2])|(0?[1-9]))[-/]*(([12][0-9])|(3[01])|(0?[1-9])))|(((1[0-2])|(0?[1-9]))[-/]*(([12][0-9])|(3[01])|(0?[1-9]))[-/]*((19|20)?\d{2}))|((([12][0-9])|(3[01])|(0?[1-9]))[-/]*((1[0-2])|(0?[1-9]))[-/]*((19|20)?\d{2})))(\s{1,}([01]?\d|2[0-3]):?([0-5]?\d):?([0-5]?\d))?$')
    pat_date_n  = re.compile('((19|20)?\d{2})((1[0-2])|(0[1-9]))(([12][0-9])|(3[01])|(0[1-9]))(([01]\d|2[0-3])([0-5]\d)([0-5]\d))?$')
    pat_phone   = re.compile('(\+?0?86\-?)?1[3|4|5|7|8][0-9]\d{8}$')
    pat_fax     = re.compile('(((0?10)|(0?[2-9]\d{2,3}))[-\s]?)?[1-9]\d{6,7}$')    
    pat_account = re.compile('(\d{16}|\d{19}|\d{12})$')
    pat_head_0  = re.compile('0\d*')
    [l_dbinfo,l_username,l_num_row,l_num_distinct,l_max_row,l_weight,l_max_value]=p_parms
    l_return_stru={"scores":0,"records":[]}
    records=[]
    conn=cx_Oracle.connect(l_dbinfo[3]+'/'+l_dbinfo[4]+'@'+l_dbinfo[0]+':'+l_dbinfo[1]+'/'+l_dbinfo[2])
    cursor = conn.cursor()
    cursor.execute("""
        select
        a.table_name,
        a.column_name,
        case when nvl(b.NUM_ROWS,"""+str(l_num_row)+"""+1)>"""+str(l_num_row)+""" then 'BIG' else 'SMALL' end,
        nvl(b.NUM_ROWS,"""+str(l_num_row)+"""+1),
        a.data_type
        FROM DBA_TAB_COLS a,dba_tables b
        where a.owner=b.OWNER and a.table_name=b.TABLE_NAME and a.owner='"""+l_username+"""' 
        and (a.data_type like '%CHAR%' or a.data_type='NUMBER') and hidden_column='NO'
        and a.num_distinct>"""+str(l_num_distinct))
    results = cursor.fetchall()
    for r in results:
      if r[2]=="BIG":
        l_sample=float(l_num_row)*float(100)/float(r[3])
        cursor.execute("""
        select trim(\""""+r[1]+"""\") from (select * from (select \""""+r[1]+"""\" from """+l_username+""".\""""+r[0]+"""\"
        sample block ("""+str(l_sample)+""") ) where trim(\""""+r[1]+"""\") is not null and trim(\""""+r[1]+"""\")<>'0')
        where rownum<="""+str(l_max_row))
      elif r[2]=="SMALL":
        cursor.execute("""
        select trim(\""""+r[1]+"""\") from """+l_username+""".\""""+r[0]+"""\" where trim(\""""+r[1]+"""\") is not null
        and trim(\""""+r[1]+"""\")<>'0'
        """)
      data = cursor.fetchall()
      l_char=0
      l_date=0
      l_phone=0
      l_fax=0
      l_num=0
      l_account=0
      l_head_0=0
      if data:
        if r[4]=="NUMBER":
          for d in data:
            if not pat_date_n.match(d[0]):
              l_date=1
              break
          if l_date==0:
            records.append(r+('DATE',))
        else :
          for d in data:
            if re.search('[^0-9\-\/\+\:\s]',d[0]):
              l_char=1
              break
            if re.search('[\-\/]',d[0]):
              l_num=1
              if not pat_date_c.match(d[0]):
                l_date=1
              if not pat_fax.match(d[0]):
                l_fax=1
              if (l_date==1 and l_fax==1):          
                break
            else:
              if not pat_phone.match(d[0]):
                l_phone=1
              if not pat_date_n.match(d[0]):
                l_date=1
              if not pat_account.match(d[0]):
                l_account=1
              if not pat_fax.match(d[0]):
                l_fax=1
              if pat_head_0.match(d[0]):
                l_head_0=1
          if (l_date==0 and l_char==0):
            records.append((r[0],)+(r[1],)+(r[4],)+('DATE',))
          if (l_date==1 and l_num==0 and l_char==0 and l_phone==1 and l_account==1 and l_fax==1 and l_head_0==0):
            records.append((r[0],)+(r[1],)+(r[4],)+('NUMBER',))
    l_return_stru["records"]=records
    if (len(records)*float(l_weight))>float(l_max_value):
        l_return_stru["scores"]=float('%0.2f' %float(l_max_value))
    else:
        l_return_stru["scores"]=float('%0.2f' %(len(records)*float(l_weight)))

    cursor.close()
    conn.close()
    return l_return_stru
    
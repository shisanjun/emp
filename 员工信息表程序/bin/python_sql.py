# -*- coding:utf-8 -*-
__author__ = 'shisanjun'
#程序实现对数据表的增删改查询功能

import os
def sql_parse(sql):
    """
    查询，删除，更新，插入查询语句格式分发
    """
    parse_func={
        'insert':insert_parse,
        'delete':delete_parse,
        'update':update_parse,
        'select':select_parse,
    }
    sql_l=sql.split(' ')
    func=sql_l[0]
    res=''
    if func in parse_func:
        res=parse_func[func](sql_l)
    return res

def insert_parse(sql_1):
    """
    插入语句格式化
    """
    sql_dic={
        'func':insert,
        'into':[], #查询字段
        'values':[]  #数据.表
    }
    return handle_parse(sql_1,sql_dic)

def delete_parse(sql_1):
    """
    删除语句格式化
    """
    sql_dic={
        'func':delete,
        'from':[],
        'where':[]
    }
    return handle_parse(sql_1,sql_dic)

def update_parse(sql_1):
    """
    更新语句格式化
    """
    sql_dic={
        'func':update,
        'update':[],
        'set':[],
        'where':[]
    }
    return handle_parse(sql_1,sql_dic)
def select_parse(sql_1):
    """
    查询语句格式化
    """
    sql_dic={
        'func':select,
        'select':[], #查询字段
        'from':[],   #数据.表
        'where':[],  #filter条件
        'limit':[],  #limit条件
    }
    return handle_parse(sql_1,sql_dic)

def handle_parse(sql_l,sql_dic):
    """
    格式化语句
    """
    tag=False
    for item in sql_l:
        if tag and item in sql_dic:
            tag=False
        if not tag and item in sql_dic:
            tag=True
            key=item
            continue
        if tag:
            sql_dic[key].append(item)
    if sql_dic.get('where'):
        sql_dic['where']=where_parse(sql_dic.get('where'))

    return sql_dic

def where_parse(where_l):
    """
    格式化where语句
    """
    res=[]
    key=['and','or','not']
    char=''
    for i in where_l:
        if len(i) == 0:continue
        if i in key:
            #i为key当中存放的逻辑运算符
            if len(char) != 0:
                char=three_parse(char)
                res.append(char)
                res.append(i)
                char=''
        else:
            char+=i
    else:
        char = three_parse(char)
        res.append(char)

    return res
def three_parse(exp_str):
    """
    格式化where语句三元运算
    """
    key=['>','<','=']
    res=[]
    char=''
    opt=''
    tag=False
    for i in exp_str:
        if i in key:
            tag=True
            if len(char) != 0:
                res.append(char)
                char=''
            opt+=i
        if not tag:
            char+=i

        if tag and i not in key:
            tag=False
            res.append(opt)
            opt=''
            char+=i
    else:
        res.append(char)

    #新增解析like的功能
    if len(res) == 1:
        res=res[0].split('like')
        res.insert(1,'like')

    return res
def sql_action(sql_dic):
    """
    根据格式化语句调整用查询，删除，更新，插入功能
    """
    return sql_dic["func"](sql_dic)

def select(sql_dic):
    """
    查询功能
    """
    db,table=sql_dic["from"][0].split(".")
    f=open("%s/%s" %(db,table),"r",encoding="utf-8")
    where_res=where_action(f,sql_dic["where"])
    f.close()

    limit_res=limit_action(sql_dic["limit"],where_res)

    select_res=select_action(sql_dic["select"],limit_res)

    return select_res

def insert(sql_dic):
    """
    插入功能
    """
    try:
        #处理插入字符串有双引号和单引号
        phone=sql_dic["values"][0].strip("'").split(",")[2]
        phone=sql_dic["values"][0].strip('"').split(",")[2]
    except:
        pass

    sql="select * from db1.emp where phone = %s" %phone
    select_record=sql_action(sql_parse(sql))[1]

    if len(select_record):
        return "你的手机号%s已存在，更换其他手机号!" %phone
    else:
        db,table=sql_dic["into"][0].split(".")
        with open("%s/%s" %(db,table),"ab+") as f:
            offs=-100
            while True:
                f.seek(offs,2)
                lines=f.readlines()

                if len(lines):
                    last=lines[-1]
                    break
                offs+=2

            last=last.decode(encoding="utf-8")
            last_id=int(last.split(",")[0])+1
            try:
                record=sql_dic["values"][0].strip("'").split(",")
                record=sql_dic["values"][0].strip('"').split(",")
            except:
                pass
            record.insert(0,str(last_id))
            record_new=",".join(record)+"\n"
            f.write(record_new.encode("utf-8"))

            f.flush()
        return "%s插入成功" %record_new

def delete(sql_dic):
    """
    删除功能
    """
    db,table=sql_dic["from"][0].split(".")
    filename="%s/%s" %(db,table)
    filename_new=table+"_new"
    f3=open(filename,'r',encoding="utf-8")
    where_res=where_action(f3,sql_dic["where"])
    f3.close()

    if len(where_res)==0:
            return "要删除的记录不存在！"
    else:
        str_tmp=[]

        for line in where_res:
                str_tmp.append(line.strip())


        with open(filename,'r',encoding="utf-8") as f,\
                open(filename_new,'w',encoding="utf-8") as f1:
                for f_line in f:
                    if f_line.strip() in str_tmp:
                        continue

                    f1.write(f_line)
                f1.flush()

        os.rename(filename,filename+"bak")
        os.rename(filename_new,filename)
        os.remove(filename+"bak")
        return "共计删除%s条数据" %len(where_res)

def update(sql_dic):
    """
    更新功能
    """
    db,table=sql_dic["update"][0].split(".")
    filename="%s/%s" %(db,table)
    filename_new=table+"_new"
    f3=open(filename,'r',encoding="utf-8")
    records=where_action(f3,sql_dic["where"])
    records_new=[]
    f3.close()
    set_tmp=sql_dic["set"]
    #生成需要字段结果字典
    data_tmp={}
    for i in set_tmp[0].split(","):
        if len(i):
            s=i.split("=")
            try:
                data_tmp[s[0]]=s[1].strip("'")
                data_tmp[s[0]]=s[1].strip('"')
            except:
                pass

    title="id,name,age,phone,dept,enroll_date"
    if len(records)==0:
            return "要修改的记录不存在！"
    else:

        for line in records:

                dic=dict(zip(title.split(","),line.split(",")))

                #修改原有记录为需要修改的结果，并生成新结果列表
                for data_k in data_tmp:
                    if dic.get(data_k):
                        dic[data_k]=data_tmp[data_k]
                str_tmp=[]

                for k in dic:
                    str_tmp.append(dic[k])
                records_new.append(str_tmp)
        #生成临时结果字符串列表
        str_tmp=[]
        for line in records:
            str_tmp.append(line.strip())

        with open(filename,'r',encoding="utf-8") as f,\
                open(filename_new,'w',encoding="utf-8") as f1:
                for f_line in f:

                    if f_line.strip() in str_tmp:

                        temp=records_new[str_tmp.index(f_line.strip())]
                        temp=",".join(temp)
                        f1.write(temp)
                        continue
                    f1.write(f_line)
                    f1.flush()
        os.rename(filename,filename+"bak")
        os.rename(filename_new,filename)
        os.remove(filename+"bak")

        return "共计修改%s条数" %len(records)

def where_action(f,where_sql):
    """
    根据where条件查询结果功能
    """
    title="id,name,age,phone,dept,enroll_date"
    res=[]
    if len(where_sql):
        for line in f:
            dic=dict(zip(title.split(","),line.split(",")))

            logic_res=logic_action(dic,where_sql)
            if logic_res:
                res.append(line)
    else:
        res=f.readlines()

    return res

def logic_action(dic,where_l):
    """
    根据where条件逻辑运算
    """
    res=[]
    for exp in where_l:
        #dic与exp做bool运算
        if type(exp) is list:
            #做bool运算
            exp_k,opt,exp_v=exp
            if exp[1] == '=':
                opt="%s=" %exp[1]
            dic_v=""
            if dic.get(exp_k).isdigit():
                    dic_v=int(dic[exp_k])
                    exp_v=int(exp_v)
            else:
                    dic_v="'%s'" %dic[exp_k]
            if opt != 'like':
                if type(exp_v)==str:
                    #格式化输入的关键词，去除双引号和单引号
                    try:
                        exp_v= exp_v.strip("'")
                        exp_v= exp_v.strip('"')
                    except:
                        pass
                    exp_v="'%s'" %exp_v
                exp=str(eval("%s%s%s" %(dic_v,opt,exp_v)))
            else:
                try:
                   exp_v= exp_v.strip("'")
                   exp_v= exp_v.strip('"')
                except:
                    pass
                if exp_v in dic_v:
                    exp='True'
                else:
                    exp='False'
        res.append(exp)  #['True','or','Fasle','or','True']

    res=eval(' '.join(res))
    return res

def limit_action(limit_sql,where_res):
    """
    根据limit条件查询结果功能
    """
    res=[]
    if len(limit_sql)!=0:
        index=int(limit_sql[0])
        res=where_res[:index]
    else:
        res=where_res
    return res

def select_action(select_sql,limit_res):
    """
    根据select条件查询结果功能
    """

    res=[]
    title="id,name,age,phone,dept,enroll_date"
    select_field=select_sql
    if select_sql[0]=="*":
        res=limit_res
        select_field=title.split(",")

    else:
        for line in limit_res:
            dic=dict(zip(title.split(","),line.split(",")))

            res_1=[]
            for field in select_field[0].split(","):
                res_1.append(dic[field])
            res.append(",".join(res_1))
    return [select_field,res]

"""
select * from db1.emp
select * from db1.emp where id>5 and id<10 limit 2
select * from db1.emp where name like 李
select * from db1.emp where name like '李'
select * from db1.emp where name like "李"
select id,name,phone from db1.emp where id>5 and id<10 limit 2
insert into db1.emp values shisanjun,30,18602552454,运维,2017-8-1
insert into db1.emp values "shisanjun,30,18602552453,运维,2017-8-1"
update db1.emp set name="tianyan",age="23" where id>24
update db1.emp set name="tianyan",age="23" where id>50
update db1.emp set name=tianmao,age=23 where id>25
delete from db1.emp where id=26
"""

if __name__ == '__main__':
    print("表名为：db1.exp，查询字段为：id,name,age,phone,dept,enroll_date")
    while True:
        sql=input("sql> ").strip()
        if sql == 'exit':break
        if len(sql) == 0 :continue

        sql_dic=sql_parse(sql)

        if len(sql_dic) == 0:continue

        res=sql_action(sql_dic)
        if type(res)==list:
            for line in res[1]:
                print(line.strip())
            print("\n共计查询出%s条数据" %len(res[1]))

        else:
            print(res)

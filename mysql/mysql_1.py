#!usr/bin/python

import pymysql
#连接原始数据库
conn = pymysql.connect(host="192.168.1.176",port=3001, user="root",password="i226CtSmDMn71dlyIqdZ0pI",database="yxy_education",charset="utf8")

cursor = conn.cursor()

sql = """
select ec.id,ec.subject_id,ec.edition_id,ec.grade_id,ec.exam_mode,ec.view_type,ec.is_ceping,ec.ceping_level,ec.is_open,CONCAT_WS('-',ct.full_name,st.name,et.name) as name,SUBSTRING(full_id,3,1) as grade,ec.sync_time from t_edition_config ec INNER JOIN yxy_categories_tb ct on ec.grade_id = ct.id INNER JOIN yxy_edition_tb et on ec.edition_id = et.id INNER JOIN yxy_subject_tb st on ec.subject_id = st.id;
"""

cursor.execute(sql)
alldata = cursor.fetchall()

#连接小学库
conn1 = pymysql.connect(host="192.168.1.176",port=3008, user="root",password="i226CtSmDMn71dlyIqdZ0pI",database="tyeducation_primary",charset="utf8")
cursor1 = conn1.cursor()
#连接初中库
conn2 = pymysql.connect(host="192.168.1.176",port=3008, user="root",password="i226CtSmDMn71dlyIqdZ0pI",database="tyeducation_junior",charset="utf8")
cursor2 = conn2.cursor()
#连接高中库
conn3 = pymysql.connect(host="192.168.1.176",port=3008, user="root",password="i226CtSmDMn71dlyIqdZ0pI",database="tyeducation_senior",charset="utf8")
cursor3 = conn3.cursor()



for data in alldata:
    # print(data[10])
    id = data[0]
    name = data[9]
    subject_id = data[1]
    edition_id = data[2]
    grade_id = data[3]
    exam_mode = data[4]
    view_mode = data[5]
    is_ceping = data[6]
    ceping_level = data[7]
    is_open = data[8]
    sort_id = 0
    create_time = 0
    sync_time = data[11]
    remark = ''
    is_del = 0
    sql = "insert into tifen_textbook values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    print(name)
   
    if data[10] == '1':

        cursor1.execute(sql,(str(id),str(name),str(subject_id),str(edition_id),str(grade_id),str(exam_mode),str(view_mode),str(is_ceping),str(ceping_level),str(is_open),str(sort_id),str(create_time),str(sync_time),str(remark),str(is_del)))
        conn1.commit()
        
    if data[10] == '2':
        cursor2.execute(sql, (
        str(id), str(name), str(subject_id), str(edition_id), str(grade_id), str(exam_mode), str(view_mode),
        str(is_ceping), str(ceping_level), str(is_open), str(sort_id), str(create_time), str(sync_time), str(remark),
        str(is_del)))
        conn2.commit()
        
    if data[10] == '3':
        cursor3.execute(sql, (
            str(id), str(name), str(subject_id), str(edition_id), str(grade_id), str(exam_mode), str(view_mode),
            str(is_ceping), str(ceping_level), str(is_open), str(sort_id), str(create_time), str(sync_time),
            str(remark),
            str(is_del)))
        conn3.commit()
                   
    
cursor1.close()
conn1.close()
cursor2.close()
conn2.close()
cursor3.close()
conn3.close()
cursor.close()
conn.close()
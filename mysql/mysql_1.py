#!usr/bin/python

import pymysql
#连接原始数据库
conn = pymysql.connect(host="192.168.1.176",port=3001, user="root",password="i226CtSmDMn71dlyIqdZ0pI",database="yxy_education",charset="utf8")

cursor = conn.cursor()

sql = """
select ec.id,ec.subject_id,ec.edition_id,ec.grade_id,ec.exam_mode,ec.view_type,ec.is_ceping,ec.ceping_level,ec.is_open,CONCAT_WS('-',ct.full_name,st.name,et.name) as name,SUBSTRING(full_id,3,1) as grade from t_edition_config ec INNER JOIN yxy_categories_tb ct on ec.grade_id = ct.id INNER JOIN yxy_edition_tb et on ec.edition_id = et.id INNER JOIN yxy_subject_tb st on ec.subject_id = st.id;
"""

cursor.execute(sql)
alldata = cursor.fetchall()

#连接小学库
conn1 = pymysql.connect(host="192.168.1.176",port=3002, user="root",password="i226CtSmDMn71dlyIqdZ0pI",database="tyeducation_primary",charset="utf8")
cursor1 = conn1.cursor()
#连接初中库
conn2 = pymysql.connect(host="192.168.1.176",port=3002, user="root",password="i226CtSmDMn71dlyIqdZ0pI",database="tyeducation_junior",charset="utf8")
cursor2 = conn2.cursor()
#连接高中库
conn3 = pymysql.connect(host="192.168.1.176",port=3002, user="root",password="i226CtSmDMn71dlyIqdZ0pI",database="tyeducation_senior",charset="utf8")
cursor3 = conn3.cursor()



for data in alldata:
    # print(data[10])
    sql = "insert into tifen_textbook values(%d,'%s',%d,%d,%d,%d,%d,%d,%d,%d,0,0,0)" % (data[0],data[9],data[1],data[2],data[3],data[4],data[5],data[6],data[7],data[8])
    print(sql)
   
    if data[10] == '1':
        
        cursor1.execute(sql)
        conn1.commit()
        
    if data[10] == '2':  
      
        cursor2.execute(sql)
        
        conn2.commit()
        
    if data[10] == '3':
       
        cursor3.execute(sql)
        conn3.commit()
                   
    
cursor1.close()
conn1.close()
cursor2.close()
conn2.close()
cursor3.close()
conn3.close()
cursor.close()
conn.close()
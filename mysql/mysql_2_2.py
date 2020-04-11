#!usr/bin/python

import pymysql

primary = (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19)
junior = (20,21,22,23,24,25,26,27,28,29,30,31,32,53,54,55)
senior = (33,34,35,36,37,38,39,40,41,42,44,45,46,47,48,49,50,51,52,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79)
#连接原始数据库
conn = pymysql.connect(host="192.168.1.176",port=3001, user="root",password="i226CtSmDMn71dlyIqdZ0pI",database="yxy_education",charset="utf8")

cursor = conn.cursor()

sql = """
SELECT cn.id,( SELECT id FROM t_edition_config WHERE grade_id = cn.grade_id AND subject_id = cn.subject_id AND edition_id = cn.edition_id ) AS book_id,
cn.pid,
'' AS full_id,
chapter_name,
alias_name,
edition_id,
grade_id,
sort_id 
FROM
	t_chapter_new cn
HAVING book_id is not null    
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
    print(data)
    sql = "insert into teach_textbook_chapter values(%s,%s,%s,%s,%s,%s,%s,0,0,0,0,0)"
    # print(sql)
   
    if int(data[7]) in primary:

        cursor1.execute(sql,(str(data[0]),str(data[1]),str(data[2]),str(data[3]),str(data[4]),str(data[5]),str(data[6])))
        conn1.commit()

    if int(data[7]) in junior:

        cursor2.execute(sql,(str(data[0]),str(data[1]),str(data[2]),str(data[3]),str(data[4]),str(data[5]),str(data[6])))

        conn2.commit()

    if int(data[7]) in senior:

        cursor3.execute(sql,(str(data[0]),str(data[1]),str(data[2]),str(data[3]),str(data[4]),str(data[5]),str(data[6])))
        conn3.commit()
                   
    
cursor1.close()
conn1.close()
cursor2.close()
conn2.close()
cursor3.close()
conn3.close()
cursor.close()
conn.close()
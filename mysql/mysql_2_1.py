import pymysql

primary = (1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19)
junior = (20,21,22,23,24,25,26,27,28,29,30,31,32,53,54,55)
senior = (33,34,35,36,37,38,39,40,41,42,44,45,46,47,48,49,50,51,52,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79)
#连接原始数据库
conn = pymysql.connect(host="192.168.1.176",port=3001, user="root",password="i226CtSmDMn71dlyIqdZ0pI",database="yxy_education",charset="utf8")

cursor = conn.cursor()

sql = """
SELECT
	( SELECT id FROM t_edition_config WHERE grade_id = a.grade_id AND subject_id = a.subject_id AND edition_id = a.edition_id ) AS book_id,
	a.id,
	substring_index( substring_index( a.knowledge_id, ',', b.help_topic_id + 1 ), ',',- 1 ) knowledge_id,
	a.grade_id 
FROM
	( SELECT id, knowledge_id, grade_id, subject_id, edition_id FROM `t_chapter_new` WHERE knowledge_id <> '' ) a
	JOIN mysql.help_topic b ON b.help_topic_id < ( length( a.knowledge_id ) - length( REPLACE ( a.knowledge_id, ',', '' ))+ 1 )
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


# print(alldata)
for data in alldata:
    # print(data[10])
    try:
        sql = "insert into tifen_textbook_knowledge values(null,%d,%d,%d,%d,%d,%d,%d,%d)" % (int(data[0]),int(data[1]),int(data[2]),0,0,0,0,0)
        print(sql)
   
        if data[3] in primary:
        
            cursor1.execute(sql)
            conn1.commit()
        
        if data[3] in junior:
      
            cursor2.execute(sql)
        
            conn2.commit()
        
        if data[3] in senior:
       
            cursor3.execute(sql)
            conn3.commit()
    except:
        continue
    
cursor1.close()
conn1.close()
cursor2.close()
conn2.close()
cursor3.close()
conn3.close()
cursor.close()
conn.close()
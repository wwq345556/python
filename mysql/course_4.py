import pymysql
#连接原始数据库
conn = pymysql.connect(host="192.168.1.176",port=3001, user="root",password="i226CtSmDMn71dlyIqdZ0pI",database="yxy_education",charset="utf8")
cursor = conn.cursor()

#连接小学库
conn1 = pymysql.connect(host="192.168.1.176",port=3008, user="root",password="i226CtSmDMn71dlyIqdZ0pI",database="tyeducation_primary",charset="utf8")
cursor1 = conn1.cursor()
#连接初中库
conn2 = pymysql.connect(host="192.168.1.176",port=3008, user="root",password="i226CtSmDMn71dlyIqdZ0pI",database="tyeducation_junior",charset="utf8")
cursor2 = conn2.cursor()
#连接高中库
conn3 = pymysql.connect(host="192.168.1.176",port=3008, user="root",password="i226CtSmDMn71dlyIqdZ0pI",database="tyeducation_senior",charset="utf8")
cursor3 = conn3.cursor()


tVIdCount = "select id from yxy_school_info_tb where keywords='格灵视频'"
cursor.execute(tVIdCount)
tVId = cursor.fetchall()
id = list()
for data in tVId:
    id.append(data[0])
idString = ""
for item in id:
    idString = idString + str(item) + ','
idString = '(' + idString[:-1] + ')'
print(idString)

selectSql = 'select file_id from tifen_course where id in %s' % idString
print(selectSql)
cursor1.execute(selectSql)
selectId = cursor1.fetchall()
childid = list()
for data in selectId:
    childid.append(data[0])
childidString = ""
for item in childid:
    childidString = childidString + str(item) + ','
childidString = '(' + childidString[:-1] + ')'
print(childidString)
deleteSql = "delete from tifen_course where id in %s" % idString
deletechildSql = "delete from tifen_file_detail where id in %s" % childidString
cursor1.execute(deleteSql)
conn1.commit()
cursor1.execute(deletechildSql)
conn1.commit()

cursor2.execute(deleteSql)
conn2.commit()
cursor2.execute(deletechildSql)
conn2.commit()

cursor3.execute(deleteSql)
conn3.commit()
cursor3.execute(deletechildSql)
conn3.commit()







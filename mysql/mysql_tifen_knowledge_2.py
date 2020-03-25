import pymysql

#连接小学库
conn = pymysql.connect(host="192.168.1.176",port=3008, user="root",password="i226CtSmDMn71dlyIqdZ0pI",database="tyeducation_senior",charset="utf8")
cursor = conn.cursor()

countSql = "select * from tifen_knowledge"

count = cursor.execute(countSql)


def pid_str(id):
    pid = str(id)
    str1 = ''
    sql = "select * from tifen_knowledge where id = %d" % id
    cursor.execute(sql)
    data = cursor.fetchone()
    print(data)
    if int(data[1]) != 0:
        str1 = pid_str(data[1])
    pid = str1 + ":" + pid
    return str(pid)


for num in range(count):
    try:
        sql = "select * from tifen_knowledge limit %d,%d" % (num, 1)
        cursor.execute(sql)
        data = cursor.fetchone()

        pid_string = pid_str(data[0])
        pid_string = "0" + pid_string

        updateSql = "update tifen_knowledge set full_id = '%s' where id = %d" % (pid_string, data[0])
        print(updateSql)
        cursor.execute(updateSql)
        conn.commit()
    except:
        continue



deleteSql = "delete from tifen_knowledge where full_id = ''"
cursor.execute(deleteSql)
conn.commit()

cursor.close()
conn.close()
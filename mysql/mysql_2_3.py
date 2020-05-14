import pymysql
conn = pymysql.connect(host="192.168.1.176",port=3008, user="root",password="i226CtSmDMn71dlyIqdZ0pI",database="tyeducation_junior",charset="utf8")

cursor = conn.cursor()

countSql = "select * from tifen_textbook_chapter"

count = cursor.execute(countSql)


def pid_str(id):
    pid = str(id)
    str1 = ''
    sql = "select * from tifen_textbook_chapter where id = %d" % id    
    cursor.execute(sql)
    data = cursor.fetchone()
   
    if int(data[2]) != 0:
        str1 = pid_str(data[2])
    pid =  str1 + ":" + pid   
    return str(pid)

for num in range(count):
    sql = "select * from tifen_textbook_chapter limit %d,%d" % (num,1)
    cursor.execute(sql)
    data = cursor.fetchone()
    pid_string = pid_str(data[0])
    pid_string = "0" + pid_string
    
    updateSql = "update tifen_textbook_chapter set full_id = '%s' where id = %d" % (pid_string,data[0])
    print(updateSql)
    cursor.execute(updateSql)
    conn.commit()

cursor.close()
conn.close()

import pymysql

#连接yxy_basic
conn = pymysql.connect(host="192.168.1.176",port=3001, user="root",password="i226CtSmDMn71dlyIqdZ0pI",database="yxy_basic",charset="utf8")
cursor = conn.cursor()

#连接tifen_mall
conn1 = pymysql.connect(host="192.168.1.176",port=3002, user="root",password="i226CtSmDMn71dlyIqdZ0pI",database="tifen_mall",charset="utf8")
cursor1 = conn1.cursor()

#连接tongyideu
conn2 = pymysql.connect(host="192.168.1.176",port=3008, user="root",password="i226CtSmDMn71dlyIqdZ0pI",database="tongyiedu",charset="utf8")
cursor2 = conn2.cursor()

#连接vsdeu
conn3 = pymysql.connect(host="192.168.1.176",port=3002, user="root",password="i226CtSmDMn71dlyIqdZ0pI",database="vsedu",charset="utf8")
cursor3 = conn3.cursor()

# #查询所有学校
# schoolTotalSql = "select id,school_name,city_code,school_address,lat,lng,stage_tag,create_user,create_time from yxy_school_tb where id not in (2,57) and del = 1"
# cursor.execute(schoolTotalSql)
# alldata = cursor.fetchall()

# alldatas = {'0'}
#
# for x in alldata:
#     alldatas.add(x)
# print(alldatas)
# exit()
#循环

    #按学校查找班级  未加 作业群
classSql = "select id,categories_id,classname,UNIX_TIMESTAMP(create_time) from yxy_school_class_tb where school_id = 0 and categories_id <> 0"
cursor.execute(classSql)
classAllData = cursor.fetchall()

for classData in classAllData:
        #class_id
    class_id = classData[0]
        #老师manager_id
    teacherManagerSql = "select type from yxy_school_class_teacher where school_id = %s and class_id = %s order by id desc limit 1"
    cursor.execute(teacherManagerSql,(str(0),str(class_id)))
    teacherManager = cursor.fetchone()
    if teacherManager:
        if int(teacherManager[0]) == 3:
            manager_id = 1
        else:
            manager_id = 0
    else:
        manager_id = 0

        #plate_id
    categoriesSql = "select name,pid from yxy_categories_tb where id = %s"
    cursor.execute(categoriesSql,str(classData[1]))
    categories = cursor.fetchone()

        #name 班级无名不算脏数据
    if classData[2]:
        name = categories[0] + classData[2]
    else:
        name = categories[0]
    plate_id = categories[1]

        #create_time
    create_time = classData[3]
        #剩下的字段
    graduation_year = 0
    is_ban = 0
    is_del = 0
    last_msg = ""
    last_msg_time = 0
    join_able = 0
    print(class_id,manager_id,name,plate_id,create_time,graduation_year)

        #班级存入数据库
    classSql = "insert into tifen_school_class values(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    cursor2.execute(classSql,(str(class_id),str(manager_id),str(0),str(plate_id),str(graduation_year),str(name),str(is_ban),str(is_del),str(create_time),str(last_msg),str(last_msg_time),str(join_able)))
    conn2.commit()

        #tifen_school_classmate
        #先导入同学
